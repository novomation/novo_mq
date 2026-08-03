// Raw memcpy speed test, without any of the TbTransport/triple-buffer/
// datalayer machinery. This isolates just the "2 copies of a Payload-sized
// buffer" cost (write_bytes + read_bytes equivalent) so it can be compared
// directly against the round-trip latency measured in `long_term_tb`.
use log::{info, warn};
const NO_BYTES: usize = 1024 * 2024;
// Same total size as `Payload` in long_term_tb.rs (16-byte header + bytes).
const PAYLOAD_BYTES: usize = NO_BYTES + 16;

use rt_tunex::{
    clock::{ClockId, get_time},
    mman::{MmanFlags, mlockall},
    sched::{CpuSet, Pid, set_affinity, set_fifo},
};
use std::time::Duration;

const CYCLETIME_US: u64 = 1000;

fn now() -> u64 {
    get_time(ClockId::ClockMonotonic).unwrap().as_nanoseconds() as u64
}

// Each timed iteration performs 2 copies of PAYLOAD_BYTES: src -> shared,
// then shared -> dst.
const BYTES_PER_ITER: usize = 2 * PAYLOAD_BYTES;

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

fn main() {
    let _ = mlockall(MmanFlags::MCL_CURRENT | MmanFlags::MCL_FUTURE)
        .inspect_err(|err| log::error!("mlockall failed: {err}"));

    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init();
    info!("Hello");

    set_affinity(Pid::this(), CpuSet::empty().insert(0)).unwrap();
    set_fifo(Pid::this(), 95).unwrap();

    // `src` stands in for the publisher's local `Payload`, `shared` for the
    // triple-buffer slot, `dst` for the subscriber's local receive buffer.
    let mut src = vec![0u8; PAYLOAD_BYTES].into_boxed_slice();
    let mut shared = vec![0u8; PAYLOAD_BYTES].into_boxed_slice();
    let mut dst = vec![0u8; PAYLOAD_BYTES].into_boxed_slice();
    src[0] = 1; // avoid an all-zero page that could be special-cased

    let mut mma = MinMaxAvgCollector::new();
    let mut last_report = now();
    let mut i: u64 = 0;
    loop {
        let t0 = now();
        // write_bytes equivalent: local -> shared memory
        shared.copy_from_slice(std::hint::black_box(&src));
        // read_bytes equivalent: shared memory -> local
        dst.copy_from_slice(std::hint::black_box(&shared));
        let t1 = now();
        std::hint::black_box(&dst);

        mma.insert((t1 - t0) as i64);
        i = i.wrapping_add(1);
        src[0] = i as u8;

        std::thread::sleep(Duration::from_micros(CYCLETIME_US));

        if now() - last_report > 400_000_000 {
            let mma_state = mma.collect();
            // Higher latency implies lower throughput, so min/max latency map
            // to max/min throughput respectively.
            let tp_avg = throughput_gbps(BYTES_PER_ITER, mma_state.avg);
            let tp_min = throughput_gbps(BYTES_PER_ITER, mma_state.max);
            let tp_max = throughput_gbps(BYTES_PER_ITER, mma_state.min);
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
            last_report = now();
        }
    }
}
