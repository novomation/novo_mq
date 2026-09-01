// The `TbTransport` measured by this bench is not implemented on macOS, so
// the whole implementation is cfg-gated below and only a stub `main` runs there.
#[cfg(target_os = "macos")]
fn main() {
    eprintln!("bench long_term_tb: not available on macOS (TbTransport unsupported). Exiting.");
}

#[cfg(not(target_os = "macos"))]
mod bench_impl {
    use log::{debug, info, warn};
    use novomq::datalayer::{self, Datalayer, TbTransport, TbTransportConfig};
    const NO_BYTES: usize = 100_000;
    use rt_tunex::{
        clock::{ClockId, get_time},
        mman::{MmanFlags, mlockall},
        sched::{CpuSet, Pid, set_affinity, set_fifo},
    };
    use std::{
        mem::size_of,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use waitfree_sync::spsc::spsc;
    use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};
    const CYCLETIME_US: u64 = 1_000;

    #[repr(C, packed)]
    #[derive(Clone, Copy, Immutable, KnownLayout, FromBytes, IntoBytes, Unaligned)]
    struct Payload {
        idx: u64,
        time: u64,
        bytes: [u8; NO_BYTES],
    }

    impl datalayer::ByteInit for Payload {
        // Manual impl: `#[derive(Default)]`/the std array impl only cover
        // arrays up to length 32, but `bytes` is NO_BYTES bytes; an explicit array
        // literal has no such limit.
        fn init() -> Self {
            Payload {
                idx: 0,
                time: 0,
                bytes: [0u8; NO_BYTES],
            }
        }
    }

    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            // Zero-copy: reinterprets `self`'s own bytes in place, no copying.
            self.as_bytes()
        }
    }
    impl AsMut<[u8]> for Payload {
        fn as_mut(&mut self) -> &mut [u8] {
            // Zero-copy: reinterprets `self`'s own bytes in place, no copying.
            self.as_mut_bytes()
        }
    }

    fn now() -> u64 {
        get_time(ClockId::ClockMonotonic).unwrap().as_nanoseconds() as u64
    }

    // Over the measured round-trip latency, the payload is copied twice:
    // write_bytes() (TX local -> shared triple buffer) and read_bytes() (shared
    // triple buffer -> RX local buffer).
    const BYTES_PER_ROUNDTRIP: usize = 2 * size_of::<Payload>();

    // `ns` is a duration in nanoseconds; 1 byte/ns == 1e9 bytes/s == 1 GB/s, so
    // this conversion needs no extra scaling factor.
    fn throughput_gbps(bytes: usize, ns: f64) -> f64 {
        if ns <= 0.0 {
            return 0.0;
        }
        bytes as f64 / ns
    }

    #[derive(Debug)]
    pub struct MinMaxAvgCollector {
        pub min: i64,
        pub max: i64,
        sum: f64,
        count: u64,
    }
    impl Default for MinMaxAvgCollector {
        fn default() -> Self {
            Self {
                min: i64::MAX,
                max: Default::default(),
                sum: Default::default(),
                count: Default::default(),
            }
        }
    }
    impl MinMaxAvgCollector {
        pub fn new() -> Self {
            Self::default()
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
        pub fn avg(&self) -> f64 {
            if self.count == 0 {
                0.0
            } else {
                self.sum / self.count as f64
            }
        }
        pub fn collect(&mut self) -> MinMaxAvg {
            let mma = MinMaxAvg {
                min: self.min as f64,
                max: self.max as f64,
                avg: self.avg(),
            };
            *self = Self::default();
            mma
        }
    }

    #[derive(Debug, Default)]
    pub struct MinMaxAvg {
        pub min: f64,
        pub max: f64,
        pub avg: f64,
    }

    type TbTrans = TbTransport<Payload>;
    type TbTransR = TbTransport<Payload>;

    pub fn run() {
        let _ = mlockall(MmanFlags::MCL_CURRENT | MmanFlags::MCL_FUTURE)
            .inspect_err(|err| log::error!("mlockall failed: {err}"));

        let _ = simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Info)
            .env()
            .init();
        info!("Hello");
        let stop_flag = Arc::new(AtomicBool::new(false));

        let rx_bounded = Arc::new(AtomicBool::new(false));

        let _ = std::thread::spawn({
            let rx_bounded = rx_bounded.clone();
            let stop_flag = stop_flag.clone();
            move || {
                info!("TX Spawned");

                // let mut tx_times = Vec::with_capacity(30_000);
                let datalayer: Datalayer<TbTrans> = Datalayer::open("domain").unwrap();
                let mut p = datalayer.publisher("topic").unwrap();
                p.wait_for_subscribers(1);
                rx_bounded.store(true, Ordering::Relaxed);

                let _ = set_affinity(Pid::this(), CpuSet::empty().insert(0))
                    .inspect_err(|err| log::warn!("Could not set affinity: {err}"));
                let _ = set_fifo(Pid::this(), 95)
                    .inspect_err(|err| log::warn!("Could not set FIFO: {err}"));

                info!("TX ready");
                let mut i = 0;
                let mut msg = Payload {
                    idx: i,
                    time: now(),
                    bytes: [0u8; NO_BYTES],
                };
                while !stop_flag.load(Ordering::Relaxed) {
                    msg.idx = i;
                    msg.time = now();
                    p.try_send(msg.as_bytes()).unwrap();
                    std::thread::sleep(Duration::from_micros(CYCLETIME_US));
                    debug!("Send {i}th message");
                    i = i.wrapping_add(1);
                }
                info!("TX Done");
                // tx_times
            }
        });
        let (mut tx_lat, mut rx_lat) = spsc(1024);
        let _ = std::thread::spawn(move || {
            info!("RX Spawned");

            // let mut rx_times = Vec::with_capacity(30_000);
            let datalayer: Datalayer<TbTrans> = Datalayer::open("domain").unwrap();
            let mut p = datalayer
                .subscriber("topic", TbTransportConfig::new())
                .unwrap();
            p.wait_until_connected();

            let mut buffer = [0u8; size_of::<Payload>()];
            let _ = set_affinity(Pid::this(), CpuSet::empty().insert(1))
                .inspect_err(|err| log::warn!("Could not set affinity: {err}"));
            let _ = set_fifo(Pid::this(), 95)
                .inspect_err(|err| log::warn!("Could not set FIFO: {err}"));

            info!("RX ready");

            // `try_recv` may return the same (stale) message repeatedly when no
            // new data has been written since the last read. Track the last seen
            // message index so we only record latency for genuinely new messages.
            let mut last_idx: Option<u64> = None;
            while !stop_flag.load(Ordering::Relaxed) {
                match p.try_recv(&mut buffer) {
                    Ok((len, _)) => {
                        let t_rx = now();
                        assert_eq!(len, size_of::<Payload>());
                        // Zero-copy: interpret the received bytes directly as a
                        // `&Payload`, no copy into an owned struct.
                        let msg = Payload::ref_from_bytes(&buffer[..len]).unwrap();
                        if last_idx != Some(msg.idx) {
                            last_idx = Some(msg.idx);
                            // Keep full nanosecond precision (no lossy us round-trip).
                            let latency = (t_rx - msg.time) as f64;
                            tx_lat.try_send(latency).unwrap();
                            // log::info!("Got msg");
                        } else {
                            // log::info!("Got no msg");
                        }
                    }
                    Err(e) => log::error!("Error: {:?}", e),
                }
            }
            info!("RX Done");
        });
        let mut mma = MinMaxAvgCollector::new();
        loop {
            while let Some(lat) = rx_lat.try_recv() {
                mma.insert(lat as i64);
            }
            let mma_state = mma.collect();
            // Higher latency implies lower throughput, so min/max latency map to
            // max/min throughput respectively.
            let tp_avg = throughput_gbps(BYTES_PER_ROUNDTRIP, mma_state.avg);
            let tp_min = throughput_gbps(BYTES_PER_ROUNDTRIP, mma_state.max);
            let tp_max = throughput_gbps(BYTES_PER_ROUNDTRIP, mma_state.min);
            if mma_state.max > 1000.0 {
                warn!(
                    "min:{:6.1}ns avg:{:6.1}ns max:{:6.1}ns | throughput min:{:6.2}GB/s avg:{:6.2}GB/s max:{:6.2}GB/s",
                    mma_state.min, mma_state.avg, mma_state.max, tp_min, tp_avg, tp_max,
                );
            } else if mma_state.max >= 10.0 {
                info!(
                    "min:{:6.1}ns avg:{:6.1}ns max:{:6.1}ns | throughput min:{:6.2}GB/s avg:{:6.2}GB/s max:{:6.2}GB/s",
                    mma_state.min, mma_state.avg, mma_state.max, tp_min, tp_avg, tp_max,
                );
            }

            std::thread::sleep(Duration::from_millis(400));
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn main() {
    bench_impl::run();
}
