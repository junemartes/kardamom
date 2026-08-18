//! Persistent tail lanes.
//!
//! The commit tail's hash+validate work is chunked across threads. Those
//! threads used to be `std::thread::scope` spawns, one set per block, and
//! once the consensus witness got cheap the SPAWN/JOIN became 0.5ms of a
//! 1.9ms tail — 36% of the overlap scope, pure overhead. Two cheaper
//! theories were measured and rejected first (128KB stacks: no change;
//! letting the tail thread take a chunk: worse, because the caller cores
//! are shared with the harness and writer so that chunk straggles).
//!
//! So: create the threads ONCE per pool and hand them work. The lanes are
//! parked between blocks and pinned to the worker cores, which are idle
//! during the tail (the workers are parked or keep-hot spinning there).
//!
//! SAFETY MODEL. `run` publishes a pointer to the caller's closure, wakes
//! the lanes, and does not return until every lane has finished with it —
//! the same guarantee `thread::scope` gives, enforced here by a
//! completion counter the caller waits on. The closure therefore outlives
//! every use, which is what lets the lifetime be erased. Chunk indices
//! are handed out by a single `fetch_add`, so each index is executed
//! exactly once and lanes never share a chunk.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// Type-erased job: a pointer to the caller's closure plus the monomorphic
/// trampoline that calls it.
#[derive(Clone, Copy)]
struct Job {
    data: *const (),
    call: unsafe fn(*const (), usize),
}

// SAFETY: the pointer targets a closure the CALLER owns and keeps alive
// across the whole `run` call (it blocks until every lane is done), and
// the closure is `Sync` so concurrent calls are sound.
unsafe impl Send for Job {}
unsafe impl Sync for Job {}

struct Shared {
    /// Current job, generation-stamped so a lane never runs a stale one.
    job: Mutex<Option<Job>>,
    wake: Condvar,
    /// Signalled by the last lane to finish, so the caller can BLOCK
    /// instead of spinning. Spinning here is fine when the lanes have
    /// their own cores and catastrophic when they do not: with three
    /// admission lanes on two physical caller cores, a spin-only wait
    /// turned a 4-minute benchmark into 11+ minutes, because the waiter
    /// was competing with the lanes it was waiting for.
    done: Condvar,
    done_lock: Mutex<()>,
    generation: AtomicU64,
    n_chunks: AtomicUsize,
    next_chunk: AtomicUsize,
    /// Lanes still working on the current job.
    active: AtomicUsize,
    shutdown: AtomicBool,
}

pub(crate) struct LanePool {
    shared: Arc<Shared>,
    threads: Vec<std::thread::JoinHandle<()>>,
    lanes: usize,
}

impl LanePool {
    /// Spawn `lanes` persistent workers, pinned round-robin over
    /// `pin_cores` when it is non-empty.
    pub(crate) fn new(lanes: usize, pin_cores: Vec<usize>) -> Self {
        let shared = Arc::new(Shared {
            job: Mutex::new(None),
            wake: Condvar::new(),
            done: Condvar::new(),
            done_lock: Mutex::new(()),
            generation: AtomicU64::new(0),
            n_chunks: AtomicUsize::new(0),
            next_chunk: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        });
        let mut threads = Vec::with_capacity(lanes);
        for li in 0..lanes {
            let sh = shared.clone();
            let pins = pin_cores.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("stm-lane-{li}"))
                    .spawn(move || {
                        if !pins.is_empty() {
                            let _ = core_affinity::set_for_current(core_affinity::CoreId {
                                id: pins[li % pins.len()],
                            });
                        }
                        let mut seen = 0u64;
                        loop {
                            // Wait for a job newer than the last one we ran.
                            let job = {
                                let mut g = sh.job.lock().expect("lane job poisoned");
                                loop {
                                    if sh.shutdown.load(Ordering::Acquire) {
                                        return;
                                    }
                                    let g_now = sh.generation.load(Ordering::Acquire);
                                    if g_now != seen && g.is_some() {
                                        seen = g_now;
                                        break g.expect("checked");
                                    }
                                    g = sh.wake.wait(g).expect("lane job poisoned");
                                }
                            };
                            let n = sh.n_chunks.load(Ordering::Acquire);
                            loop {
                                let i = sh.next_chunk.fetch_add(1, Ordering::AcqRel);
                                if i >= n {
                                    break;
                                }
                                // SAFETY: `run` keeps the closure alive until
                                // `active` drains, and each index is handed
                                // out once.
                                unsafe { (job.call)(job.data, i) };
                            }
                            if sh.active.fetch_sub(1, Ordering::AcqRel) == 1 {
                                // Last one out wakes the caller.
                                let _g = sh.done_lock.lock().expect("lane done poisoned");
                                sh.done.notify_all();
                            }
                        }
                    })
                    .expect("lane spawn"),
            );
        }
        Self {
            shared,
            threads,
            lanes,
        }
    }

    pub(crate) fn lanes(&self) -> usize {
        self.lanes
    }

    /// Run `f(0..n_chunks)` across the lanes and return once every chunk
    /// has completed. Chunks are claimed by index, so a slow lane simply
    /// takes fewer.
    pub(crate) fn run<F>(&self, n_chunks: usize, f: &F)
    where
        F: Fn(usize) + Sync,
    {
        if n_chunks == 0 {
            return;
        }
        unsafe fn trampoline<F: Fn(usize)>(data: *const (), i: usize) {
            // SAFETY: `data` came from `&F` in `run`, which outlives the call.
            let f = unsafe { &*(data as *const F) };
            f(i);
        }
        let job = Job {
            data: f as *const F as *const (),
            call: trampoline::<F>,
        };
        self.shared.n_chunks.store(n_chunks, Ordering::Release);
        self.shared.next_chunk.store(0, Ordering::Release);
        self.shared.active.store(self.lanes, Ordering::Release);
        {
            let mut g = self.shared.job.lock().expect("lane job poisoned");
            *g = Some(job);
            self.shared.generation.fetch_add(1, Ordering::AcqRel);
        }
        self.shared.wake.notify_all();
        // Wait out the lanes: a short spin catches the common case where
        // the chunks are tens of microseconds, then BLOCK. Never spin
        // indefinitely — the lanes may be sharing this thread's core.
        let mut spins = 0u32;
        while self.shared.active.load(Ordering::Acquire) != 0 {
            spins += 1;
            if spins < 256 {
                std::hint::spin_loop();
                continue;
            }
            let mut g = self.shared.done_lock.lock().expect("lane done poisoned");
            while self.shared.active.load(Ordering::Acquire) != 0 {
                let (ng, _) = self
                    .shared
                    .done
                    .wait_timeout(g, std::time::Duration::from_micros(200))
                    .expect("lane done poisoned");
                g = ng;
            }
            break;
        }
        // Drop the job so a spurious wake cannot re-run it.
        let mut g = self.shared.job.lock().expect("lane job poisoned");
        *g = None;
    }
}

impl Drop for LanePool {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.wake.notify_all();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_chunk_runs_exactly_once() {
        let pool = LanePool::new(4, Vec::new());
        for round in 0..200 {
            let n = 1 + round % 37;
            let counts: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();
            pool.run(n, &|i: usize| {
                counts[i].fetch_add(1, Ordering::Relaxed);
            });
            for (i, c) in counts.iter().enumerate() {
                assert_eq!(c.load(Ordering::Relaxed), 1, "chunk {i} in round {round}");
            }
        }
    }

    #[test]
    fn results_are_visible_to_the_caller_after_run() {
        // The completion wait must publish the lanes' writes to the caller.
        let pool = LanePool::new(3, Vec::new());
        for round in 0..200usize {
            let n = 64;
            let out: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();
            pool.run(n, &|i: usize| {
                out[i].store(i * round + 1, Ordering::Relaxed);
            });
            for (i, v) in out.iter().enumerate() {
                assert_eq!(
                    v.load(Ordering::Relaxed),
                    i * round + 1,
                    "chunk {i} round {round}"
                );
            }
        }
    }

    #[test]
    fn back_to_back_jobs_do_not_bleed() {
        let pool = LanePool::new(4, Vec::new());
        let hits = AtomicUsize::new(0);
        for _ in 0..500 {
            pool.run(8, &|_| {
                hits.fetch_add(1, Ordering::Relaxed);
            });
        }
        assert_eq!(hits.load(Ordering::Relaxed), 500 * 8);
    }
}
