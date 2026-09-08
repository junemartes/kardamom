//! Background host-load sampling for a running stack.

use std::path::PathBuf;
use std::time::Duration;

/// Background `/proc/loadavg` sampler, at a 500 ms cadence. It writes
/// epoch-stamped lines to a file in the stack root. One sampler per
/// stack; it stops when dropped. Driver-stall flakes correlate with host
/// load, so a kept failure root carries this timeline next to the
/// service logs.
pub(super) struct LoadSampler {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl LoadSampler {
    pub(super) fn start(path: PathBuf) -> Self {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = stop.clone();
        let join = std::thread::Builder::new()
            .name("load-sampler".into())
            .spawn(move || {
                use std::io::Write;
                let Ok(mut f) = std::fs::File::create(&path) else {
                    return;
                };
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
                    let epoch_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis());
                    let _ = writeln!(f, "{epoch_ms} {}", load.trim());
                    std::thread::sleep(Duration::from_millis(500));
                }
            })
            .ok();
        Self { stop, join }
    }
}

impl Drop for LoadSampler {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}
