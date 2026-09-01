// Shared-memory datalayer benchmark (the `novomq::shared_memory` layer).
//
// A TX thread repeatedly **writes** a payload-sized region through
// [`SharedMemory::with_write`] (mutex-gated). An RX thread on another CPU
// repeatedly **reads** the same region through [`SharedMemory::with_read`]
// of the *same* `SharedDomain`, which is the "both sides touch the very same
// memory" use case the layer is built for. Every operation is serialised by
// the `pthread_mutex_t` embedded in the shared region itself (see
// [`DirectShmTransport`]), so this bench measures the real cross-thread
// mutex-over-head + copy cost of a `SharedMemory` region.
//
// The payload is a plain byte buffer (first 16 bytes are a header the TX
// stamps with an index + timestamp, the rest is bulk data overwritten every
// cycle), matching the `direct_shm.rs` bench and the passing `two_threads_*`
// unit tests.
//
// The `shared_memory` module is not implemented on macOS, so the whole
// implementation is cfg-gated below and only a stub `main` runs there.
#[cfg(target_os = "macos")]
fn main() {
    eprintln!("bench long_term_sm: not available on macOS (shared_memory unsupported). Exiting.");
}

#[cfg(not(target_os = "macos"))]
mod bench_impl {
    use log::info;
    use novomq::shared_memory::SharedDomain;
    use rt_tunex::{
        clock::{ClockId, get_time},
        sched::{CpuSet, Pid, set_affinity, set_fifo},
    };
    use std::{
        mem::size_of,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering as AtomicOrdering},
        },
        time::Duration,
    };
    use waitfree_sync::spsc::spsc;

    const CYCLETIME_US: u64 = 1000;
    /// Total shared-region size in bytes (matches the "big payload" unit tests
    /// and `direct_shm.rs`).
    const NO_BYTES: usize = 100_000;
    type Region = [u8; NO_BYTES];

    /// One sample == one full-region write + one full-region read.
    const BYTES_PER_SAMPLE: usize = 2 * size_of::<[u8; NO_BYTES]>();

    /// Runtime in ms; defaults to 60 seconds so the bench terminates on its own in
    /// sandboxes/CI. Set `SHARED_MEMORY_RUN_MS=0` for an infinite run.
    fn run_for_ms() -> u64 {
        std::env::var("SHARED_MEMORY_RUN_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60_000)
    }

    fn now_ns() -> u64 {
        get_time(ClockId::ClockMonotonic).unwrap().as_nanoseconds() as u64
    }

    fn throughput_gbps(bytes: usize, ns: f64) -> f64 {
        if ns <= 0.0 {
            return 0.0;
        }
        bytes as f64 / ns
    }

    fn read_index(bytes: &[u8]) -> u64 {
        u64::from_le_bytes(bytes[..8].try_into().unwrap())
    }

    fn read_tx_time(bytes: &[u8]) -> u64 {
        u64::from_le_bytes(bytes[8..16].try_into().unwrap())
    }

    fn write_index_and_time(bytes: &mut [u8], index: u64, tx_time: u64) {
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        bytes[8..16].copy_from_slice(&tx_time.to_le_bytes());
    }

    fn write_bulk(bytes: &mut [u8], index: u64) {
        for (i, b) in bytes[16..].iter_mut().enumerate() {
            *b = (index as u8).wrapping_add((i & 0xff) as u8);
        }
    }

    #[derive(Debug, Default)]
    pub struct MinMaxAvg {
        pub min: f64,
        pub max: f64,
        pub avg: f64,
    }

    pub struct MinMaxAvgCollector {
        pub min: i64,
        pub max: i64,
        sum: f64,
        count: u64,
    }
    impl MinMaxAvgCollector {
        pub fn new() -> Self {
            Self {
                min: i64::MAX,
                max: i64::MIN,
                sum: 0.0,
                count: 0,
            }
        }
        pub fn insert(&mut self, value: i64) {
            if value > self.max {
                self.max = value;
            }
            if value < self.min {
                self.min = value;
            }
            self.sum += value as f64;
            self.count += 1;
        }
        pub fn collect(&mut self) -> MinMaxAvg {
            let mma = MinMaxAvg {
                min: self.min as f64,
                max: self.max as f64,
                avg: if self.count == 0 {
                    0.0
                } else {
                    self.sum / self.count as f64
                },
            };
            *self = Self::new();
            mma
        }
    }

    pub fn run() {
        let _ = simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Info)
            .env()
            .init();
        info!("Hello");
        let stop_flag = Arc::new(AtomicBool::new(false));

        // Domain directory we can write regardless of the sandbox.
        let domain_dir = std::env::temp_dir().join("novomq-bench-shared-memory");
        std::fs::create_dir_all(&domain_dir).unwrap();

        // Both threads share ONE physical region: the TX thread's `create` keeps
        // an `Arc` to it, and the RX thread's `get` re-resolves the *same* `Arc`
        // through the domain registry — so all reads/writes are gated by the very
        // same embedded `pthread_mutex_t`.
        let domain = SharedDomain::open(&domain_dir).unwrap();
        let tx_shared = Arc::new(domain.create::<Region>("bench-region"));
        let rx_shared = domain.get::<Region>("bench-region").unwrap();
        info!(
            "shared region ready ({} bytes; same physical region across handles)",
            tx_shared.size()
        );

        // Latency (ns) flows: RX thread (producer) -> main (consumer).
        let (tx_lat, mut rx_lat) = spsc::<u64>(1024);

        // RX / reader thread: pinned to CPU 1, reads the region.
        let rx_handle = {
            let rx_shared = rx_shared;
            let stop_flag = stop_flag.clone();
            let mut tx_lat = tx_lat;
            std::thread::Builder::new()
                .name("rx".into())
                .spawn(move || {
                    let _ = set_affinity(Pid::this(), CpuSet::empty().insert(1));
                    let _ = set_fifo(Pid::this(), 95);
                    info!("RX ready");

                    let mut last_idx: Option<u64> = None;
                    let mut n_iter = 0u64;
                    let mut n_samples = 0u64;
                    while !stop_flag.load(AtomicOrdering::Relaxed) {
                        // Direct, mutex-gated **read** of the shared region.
                        let (idx, tx_time) = rx_shared.with_read(|bytes| {
                            std::hint::black_box(bytes);
                            (read_index(bytes), read_tx_time(bytes))
                        });
                        n_iter += 1;
                        if n_iter <= 5 || (n_iter % 500 == 0) {
                            // info!("RX iter={} idx={}", n_iter, idx);
                        }
                        if let Some(prev) = last_idx
                            && idx != prev
                        {
                            let rx_time = now_ns();
                            let lat = rx_time.saturating_sub(tx_time);
                            if tx_lat.try_send(lat).is_ok() {
                                n_samples += 1;
                            }
                        }
                        last_idx = Some(idx);
                        std::thread::yield_now();
                    }
                    info!("RX Done ({} polls / {} samples)", n_iter, n_samples);
                })
                .expect("spawn rx")
        };

        // TX / writer thread: pinned to CPU 0, writes the region.
        let tx_handle = {
            let tx_shared = tx_shared.clone();
            let stop_flag = stop_flag.clone();
            std::thread::Builder::new()
                .name("tx".into())
                .spawn(move || {
                    let _ = set_affinity(Pid::this(), CpuSet::empty().insert(0));
                    let _ = set_fifo(Pid::this(), 95);
                    info!("TX ready");

                    let mut i = 0u64;
                    while !stop_flag.load(AtomicOrdering::Relaxed) {
                        // Direct, mutex-gated **full-region** write into the shared region.
                        tx_shared.with_write(|bytes| {
                            write_index_and_time(bytes, i, now_ns());
                            write_bulk(bytes, i);
                            std::hint::black_box(bytes);
                        });
                        if i < 5 || (i % 500 == 0) {
                            // info!("TX iter={} idx={}", i, i);
                        }
                        i = i.wrapping_add(1);
                        std::thread::sleep(Duration::from_micros(CYCLETIME_US));
                    }
                    info!("TX Done ({} iterations)", i);
                })
                .expect("spawn tx")
        };

        // Main: collect / report latencies (bounded by the env var when set).
        let run_for = if run_for_ms() == 0 {
            None
        } else {
            Some(Duration::from_millis(run_for_ms()))
        };
        let start = std::time::Instant::now();
        let mut mma = MinMaxAvgCollector::new();
        loop {
            while let Some(lat) = rx_lat.try_recv() {
                mma.insert(lat as i64);
            }
            let mma_state = mma.collect();
            if mma_state.max >= 10.0 {
                let tp_avg = throughput_gbps(BYTES_PER_SAMPLE, mma_state.avg);
                let tp_min = throughput_gbps(BYTES_PER_SAMPLE, mma_state.max);
                let tp_max = throughput_gbps(BYTES_PER_SAMPLE, mma_state.min);
                info!(
                    "min:{:6.1}ns avg:{:6.1}ns max:{:6.1}ns | throughput min:{:6.2}GB/s avg:{:6.2}GB/s max:{:6.2}GB/s",
                    mma_state.min, mma_state.avg, mma_state.max, tp_min, tp_avg, tp_max,
                );
            }

            if let Some(d) = run_for
                && start.elapsed() >= d
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(400));
        }

        stop_flag.store(true, AtomicOrdering::Relaxed);
        let _ = tx_handle.join();
        let _ = rx_handle.join();
        info!("Bench done");
    }
}

#[cfg(not(target_os = "macos"))]
fn main() {
    bench_impl::run();
}
