// Direct shared-memory transport benchmark.
//
// Two threads share one large `NO_BYTES`-sized region negotiated over the UDS +
// memfd handshake (see `DirectShmTransport::on_accept` / `on_connect`). A TX
// thread repeatedly overwrites the region; an RX thread reads it back. Both
// operate **directly** on the shared memory, gated by the mutex embedded in the
// shared region, *without* copying into an intermediate message slot.
//
// The payload is a plain byte buffer (the first 8 bytes hold a monotonically
// increasing "index"), matching the passing `two_threads_large_payload_*` unit
// tests, and avoids any packed-struct / unaligned-reference footguns.
use novomq::datalayer::{
    DirectShmTransport, DirectShmTransportConfig, socket_path, UnixDatagram,
    UnixDatagramListener, TransportListener,
};
use novomq::TransportError;
use log::info;
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
/// Total shared-region size in bytes (matches the "big payload" unit tests).
const NO_BYTES: usize = 100_000;
type T = DirectShmTransport<[u8; NO_BYTES]>;

/// The measured unit is one full-region write + one full-region read.
const BYTES_PER_ROUNDTRIP: usize = 2 * size_of::<[u8; NO_BYTES]>();

fn now_ns() -> u64 {
    get_time(ClockId::ClockMonotonic).unwrap().as_nanoseconds() as u64
}

fn throughput_gbps(bytes: usize, ns: f64) -> f64 {
    if ns <= 0.0 {
        return 0.0;
    }
    bytes as f64 / ns
}

/// The shared region layout (first 16 bytes are a header the TX stamps, the
/// rest is bulk data the TX overwrites every cycle):
/// bytes[ 0.. 8] : sequence index (u64, LE)
/// bytes[ 8..16] : tx-timestamp in ns (u64, LE)
/// bytes[16..]   : bulk data
///
/// These accessors read/write the header, avoiding packed-struct/unaligned
/// reference footguns.
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
        // Deterministic fill so the copy is observable but cheap; `black_box`
        // in the caller keeps the whole-region write from being elided.
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

/// Blocking accept (the UDS socket is non-blocking), retrying on would-block.
fn accept_block(listener: &mut UnixDatagramListener<T>) -> Result<UnixDatagram<T>, TransportError> {
    loop {
        match listener.accept() {
            Ok(s) => return Ok(s),
            Err(TransportError::WouldBlock) => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => return Err(e),
        }
    }
}

/// Runtime in ms; defaults to 60 seconds so the bench terminates on its own in
/// sandboxes/CI where the other `long_term_*` benches (defaulting to infinite)
/// would appear to "hang".  Set `DIRECT_SHM_RUN_MS=0` for an infinite run.
fn run_for_ms() -> u64 {
    std::env::var("DIRECT_SHM_RUN_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60_000)
}

fn main() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init();
    info!("Hello");
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Dedicated domain directory we can write regardless of the sandbox.
    let domain = std::env::temp_dir().join("novomq-bench-direct-shm");
    std::fs::create_dir_all(&domain).unwrap();
    let socket_addr = socket_path(&domain, "bench");

    // Latency measurements flow: RX thread (producer) -> main (consumer).
    // Latency (ns) flows: RX thread (producer) -> main (consumer).
    let (tx_lat, mut rx_lat) = spsc::<u64>(1024);

    // RX / server thread: bind + accept, then read the shared region.
    let server_handle = {
        let socket_addr = socket_addr.clone();
        let stop_flag = stop_flag.clone();
        let mut tx_lat = tx_lat;
        std::thread::spawn(move || {
            let mut listener = UnixDatagramListener::<T>::bind(&socket_addr).unwrap();
            // Best-effort RT tuning; ignored when the sandbox forbids these CPUs
            // / priorities (matches the other `long_term_*` benches' intent).
            let _ = set_affinity(Pid::this(), CpuSet::empty().insert(1));
            let _ = set_fifo(Pid::this(), 95);
            info!("RX bound; waiting for connection");

            let server = accept_block(&mut listener).unwrap();
            info!("RX ready");

            let mut last_idx: Option<u64> = None;
            let mut n_iter = 0u64;
            let mut n_samples = 0u64;
            while !stop_flag.load(AtomicOrdering::Relaxed) {
                // Direct, mutex-gated read of the shared region (full region).
                let (idx, tx_time, first16) = {
                    let g = server.extra.lock();
                    let bytes = g.as_bytes();
                    std::hint::black_box(bytes); // keep the full read observable
                    (
                        read_index(bytes),
                        read_tx_time(bytes),
                        bytes[..std::cmp::min(16, bytes.len())].iter().map(|b| format!("{b:02x}")).collect::<String>(),
                    )
                };
                n_iter += 1;
                if n_iter <= 5 || (n_iter % 500 == 0) {
                    info!("RX iter={} idx={} tx_time={} b16={}", n_iter, idx, tx_time, first16);
                }
                if let Some(prev) = last_idx {
                    if idx != prev {
                        let rx_time = now_ns();
                        let lat = rx_time.saturating_sub(tx_time);
                        if tx_lat.try_send(lat).is_ok() {
                            n_samples += 1;
                        }
                    }
                }
                last_idx = Some(idx);
                std::thread::yield_now();
            }
            info!("RX Done ({} polls / {} samples)", n_iter, n_samples);
        })
    };

    // TX / client thread: connect, then write the shared region.
    let tx_handle = {
        let socket_addr = socket_addr.clone();
        let stop_flag = stop_flag.clone();
        std::thread::spawn(move || {
            // Best-effort RT tuning.
            let _ = set_affinity(Pid::this(), CpuSet::empty().insert(0));
            let _ = set_fifo(Pid::this(), 95);
            info!("TX ready");

            let client = loop {
                match UnixDatagram::<T>::connect(&socket_addr, DirectShmTransportConfig::new()) {
                    Ok(c) => break c,
                    Err(TransportError::PeerDoesNotExist) => {
                        std::thread::sleep(Duration::from_millis(20))
                    }
                    Err(e) => {
                        log::warn!("connect failed: {e}");
                        std::thread::sleep(Duration::from_millis(20))
                    }
                }
            };

            let mut i = 0u64;
            while !stop_flag.load(AtomicOrdering::Relaxed) {
                // Direct, mutex-gated **full-region** write into the shared region.
                {
                    let mut g = client.extra.lock();
                    let bytes = g.as_bytes_mut();
                    write_index_and_time(bytes, i, now_ns());
                    write_bulk(bytes, i);
                    std::hint::black_box(&*bytes);
                }
                if i < 5 || (i % 500 == 0) {
                    info!("TX iter={} idx={}", i, i);
                }
                i = i.wrapping_add(1);
                std::thread::sleep(Duration::from_micros(CYCLETIME_US));
            }
            info!("TX Done ({} iterations)", i);
        })
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
            let tp_avg = throughput_gbps(BYTES_PER_ROUNDTRIP, mma_state.avg);
            let tp_min = throughput_gbps(BYTES_PER_ROUNDTRIP, mma_state.max);
            let tp_max = throughput_gbps(BYTES_PER_ROUNDTRIP, mma_state.min);
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
    let _ = server_handle.join();
    info!("Bench done");
}
