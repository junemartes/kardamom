//! The shared persistent worker pool.
//!
//! Promoted from the commit tail's private lane pool. The same machinery
//! now serves every fan-out in the workspace that wants to run a function
//! over N chunks on W persistent threads: the STM commit tail's
//! hash-and-validate lanes, the sharded-admission lanes, and the
//! validator's BAL-seeded batch execution (which used to spawn one OS
//! thread per batch per block).
//!
//! History: these threads used to be `std::thread::scope` spawns, one set
//! per block. Once the consensus witness got cheap, the spawn and join
//! cost became 0.5ms of a 1.9ms tail — 36% of the overlap scope, pure
//! overhead. Two cheaper ideas were tested and rejected first: 128KB
//! stacks made no difference, and letting the tail thread take a chunk was
//! worse, because the caller cores are shared with the harness and writer,
//! so that chunk lagged behind.
//!
//! The fix: create the threads once per pool and hand them work. Workers
//! park between jobs and can be pinned to cores.
//!
//! Safety model. `run` publishes a pointer to the caller's closure, wakes
//! the workers, and does not return until every worker has finished with
//! it. This is the same guarantee `thread::scope` gives, enforced here by
//! a completion counter the caller waits on. The closure therefore
//! outlives every use, which is what lets the lifetime be erased. Chunk
//! indices are handed out by a single `fetch_add`, so each index runs
//! exactly once and workers never share a chunk.
//!
//! Panic containment. A panic inside the closure is caught at the worker,
//! recorded, and surfaced as [`PoolPanic`] from `run`. The worker keeps
//! draining chunks and the pool survives for the next job. (The private
//! predecessor let the unwind kill the lane thread. After that, the
//! completion counter never drained and `run` deadlocked. Callers that
//! treat a panic as fatal now fail loudly instead.)

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// A chunk closure panicked. Carries the first panic observed (worker,
/// chunk, and the panic payload rendered best-effort). Every chunk still
/// ran or was drained, and the pool remains usable.
#[derive(Debug, Clone)]
pub struct PoolPanic {
    pub worker: usize,
    pub chunk: usize,
    pub message: String,
}

impl core::fmt::Display for PoolPanic {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "worker {} panicked on chunk {}: {}",
            self.worker, self.chunk, self.message
        )
    }
}

impl std::error::Error for PoolPanic {}

/// Type-erased job: a pointer to the caller's closure plus the monomorphic
/// trampoline that calls it.
#[derive(Clone, Copy)]
struct Job {
    data: *const (),
    call: unsafe fn(*const (), usize, usize),
}

// SAFETY: the pointer targets a closure the caller owns and keeps alive
// across the whole `run` call (it blocks until every worker is done). The
// closure is `Sync`, so concurrent calls are sound.
unsafe impl Send for Job {}
unsafe impl Sync for Job {}

struct Shared {
    /// Current job, generation-stamped so a worker never runs a stale one.
    job: Mutex<Option<Job>>,
    wake: Condvar,
    /// Signalled by the last worker to finish, so the caller can block
    /// instead of spinning. Spinning here is fine when the workers have
    /// their own cores, but harmful when they do not: with three
    /// admission lanes on two physical caller cores, a spin-only wait
    /// turned a 4-minute benchmark into more than 11 minutes, because the
    /// waiter competed with the lanes it was waiting for.
    done: Condvar,
    done_lock: Mutex<()>,
    generation: AtomicU64,
    n_chunks: AtomicUsize,
    next_chunk: AtomicUsize,
    /// Workers still working on the current job.
    active: AtomicUsize,
    /// First contained panic of the current job (cleared per `run`).
    panic: Mutex<Option<PoolPanic>>,
    shutdown: AtomicBool,
}

/// Persistent worker pool: W threads created once, jobs handed to them as
/// `(worker_idx, chunk_idx)` closure calls. See the module docs for the
/// safety model.
pub struct WorkerPool {
    shared: Arc<Shared>,
    threads: Vec<std::thread::JoinHandle<()>>,
    workers: usize,
}

impl WorkerPool {
    /// Spawn `workers` persistent threads, pinned round-robin over
    /// `pin_cores` when it is non-empty.
    pub fn new(workers: usize, pin_cores: Vec<usize>) -> Self {
        let shared = Arc::new(Shared {
            job: Mutex::new(None),
            wake: Condvar::new(),
            done: Condvar::new(),
            done_lock: Mutex::new(()),
            generation: AtomicU64::new(0),
            n_chunks: AtomicUsize::new(0),
            next_chunk: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            panic: Mutex::new(None),
            shutdown: AtomicBool::new(false),
        });
        let mut threads = Vec::with_capacity(workers);
        for li in 0..workers {
            let sh = shared.clone();
            let pins = pin_cores.clone();
            threads.push(
                std::thread::Builder::new()
                    // Keep this name: ops tooling greps for stm-lane-*.
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
                                let mut g = sh.job.lock().expect("worker job poisoned");
                                loop {
                                    if sh.shutdown.load(Ordering::Acquire) {
                                        return;
                                    }
                                    let g_now = sh.generation.load(Ordering::Acquire);
                                    if g_now != seen && g.is_some() {
                                        seen = g_now;
                                        break g.expect("checked");
                                    }
                                    g = sh.wake.wait(g).expect("worker job poisoned");
                                }
                            };
                            let n = sh.n_chunks.load(Ordering::Acquire);
                            loop {
                                let i = sh.next_chunk.fetch_add(1, Ordering::AcqRel);
                                if i >= n {
                                    break;
                                }
                                // SAFETY: `run` keeps the closure alive until
                                // `active` drains, and each index is handed out
                                // once.
                                let r = std::panic::catch_unwind(AssertUnwindSafe(|| unsafe {
                                    (job.call)(job.data, li, i)
                                }));
                                if let Err(p) = r {
                                    let msg = p
                                        .downcast_ref::<&str>()
                                        .map(|s| (*s).to_string())
                                        .or_else(|| p.downcast_ref::<String>().cloned())
                                        .unwrap_or_else(|| "non-string panic payload".into());
                                    let mut slot = sh.panic.lock().expect("worker panic poisoned");
                                    // First panic wins; the rest are drained.
                                    slot.get_or_insert(PoolPanic {
                                        worker: li,
                                        chunk: i,
                                        message: msg,
                                    });
                                }
                            }
                            if sh.active.fetch_sub(1, Ordering::AcqRel) == 1 {
                                // Last one out wakes the caller.
                                let _g = sh.done_lock.lock().expect("worker done poisoned");
                                sh.done.notify_all();
                            }
                        }
                    })
                    .expect("worker spawn"),
            );
        }
        Self {
            shared,
            threads,
            workers,
        }
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Run `f(worker_idx, chunk_idx)` for every chunk in `0..n_chunks` and
    /// return once every chunk has completed. Chunks are claimed by index,
    /// so a slow worker simply takes fewer. `worker_idx` stays stable for
    /// the duration of one `run` call, so a caller can key per-worker
    /// resources (snapshots, scratch buffers) on it.
    ///
    /// A contained closure panic surfaces as `Err(PoolPanic)` after every
    /// chunk has been drained. The pool stays usable.
    pub fn run<F>(&self, n_chunks: usize, f: &F) -> Result<(), PoolPanic>
    where
        F: Fn(usize, usize) + Sync,
    {
        if n_chunks == 0 {
            return Ok(());
        }
        unsafe fn trampoline<F: Fn(usize, usize)>(data: *const (), lane: usize, i: usize) {
            // SAFETY: `data` came from `&F` in `run`, which outlives this call.
            let f = unsafe { &*(data as *const F) };
            f(lane, i);
        }
        let job = Job {
            data: f as *const F as *const (),
            call: trampoline::<F>,
        };
        self.shared
            .panic
            .lock()
            .expect("worker panic poisoned")
            .take();
        self.shared.n_chunks.store(n_chunks, Ordering::Release);
        self.shared.next_chunk.store(0, Ordering::Release);
        self.shared.active.store(self.workers, Ordering::Release);
        {
            let mut g = self.shared.job.lock().expect("worker job poisoned");
            *g = Some(job);
            self.shared.generation.fetch_add(1, Ordering::AcqRel);
        }
        self.shared.wake.notify_all();
        // Wait out the workers: a short spin catches the common case where
        // the chunks take tens of microseconds, then block. Never spin
        // forever, since the workers may share this thread's core.
        let mut spins = 0u32;
        while self.shared.active.load(Ordering::Acquire) != 0 {
            spins += 1;
            if spins < 256 {
                std::hint::spin_loop();
                continue;
            }
            let mut g = self.shared.done_lock.lock().expect("worker done poisoned");
            while self.shared.active.load(Ordering::Acquire) != 0 {
                let (ng, _) = self
                    .shared
                    .done
                    .wait_timeout(g, std::time::Duration::from_micros(200))
                    .expect("worker done poisoned");
                g = ng;
            }
            break;
        }
        // Drop the job so a spurious wake cannot re-run it.
        {
            let mut g = self.shared.job.lock().expect("worker job poisoned");
            *g = None;
        }
        match self
            .shared
            .panic
            .lock()
            .expect("worker panic poisoned")
            .take()
        {
            Some(p) => Err(p),
            None => Ok(()),
        }
    }
}

impl Drop for WorkerPool {
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
        let pool = WorkerPool::new(4, Vec::new());
        for round in 0..200 {
            let n = 1 + round % 37;
            let counts: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();
            pool.run(n, &|_, i: usize| {
                counts[i].fetch_add(1, Ordering::Relaxed);
            })
            .expect("no panic");
            for (i, c) in counts.iter().enumerate() {
                assert_eq!(c.load(Ordering::Relaxed), 1, "chunk {i} in round {round}");
            }
        }
    }

    #[test]
    fn results_are_visible_to_the_caller_after_run() {
        // The completion wait must publish the workers' writes to the caller.
        let pool = WorkerPool::new(3, Vec::new());
        for round in 0..200usize {
            let n = 64;
            let out: Vec<AtomicUsize> = (0..n).map(|_| AtomicUsize::new(0)).collect();
            pool.run(n, &|_, i: usize| {
                out[i].store(i * round + 1, Ordering::Relaxed);
            })
            .expect("no panic");
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
        let pool = WorkerPool::new(4, Vec::new());
        let hits = AtomicUsize::new(0);
        for _ in 0..500 {
            pool.run(8, &|_, _| {
                hits.fetch_add(1, Ordering::Relaxed);
            })
            .expect("no panic");
        }
        assert_eq!(hits.load(Ordering::Relaxed), 500 * 8);
    }

    #[test]
    fn a_panicking_chunk_is_contained_and_the_pool_survives() {
        let pool = WorkerPool::new(4, Vec::new());
        let ran: Vec<AtomicUsize> = (0..16).map(|_| AtomicUsize::new(0)).collect();
        let err = pool
            .run(16, &|_, i: usize| {
                ran[i].fetch_add(1, Ordering::Relaxed);
                if i == 7 {
                    panic!("chunk 7 exploded");
                }
            })
            .expect_err("chunk 7 must surface");
        assert_eq!(err.chunk, 7);
        assert!(err.message.contains("chunk 7 exploded"));
        // Every other chunk still ran exactly once — the job drained.
        for (i, c) in ran.iter().enumerate() {
            assert_eq!(c.load(Ordering::Relaxed), 1, "chunk {i}");
        }
        // The pool is not wedged: the next job runs clean.
        let hits = AtomicUsize::new(0);
        pool.run(8, &|_, _| {
            hits.fetch_add(1, Ordering::Relaxed);
        })
        .expect("pool survives a contained panic");
        assert_eq!(hits.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn worker_idx_is_in_range_and_stable_per_run() {
        let pool = WorkerPool::new(3, Vec::new());
        let seen: Vec<AtomicUsize> = (0..64).map(|_| AtomicUsize::new(usize::MAX)).collect();
        pool.run(64, &|lane, i: usize| {
            assert!(lane < 3, "worker idx out of range: {lane}");
            seen[i].store(lane, Ordering::Relaxed);
        })
        .expect("no panic");
        assert!(
            seen.iter().all(|s| s.load(Ordering::Relaxed) != usize::MAX),
            "every chunk saw a worker idx"
        );
    }
}
