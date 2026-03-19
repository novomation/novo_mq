use linux_rt::{
    CpuSet,
    clock::{ClockId, get_time},
    mman::{MmanFlags, mlockall},
    sched::{Pid, set_affinity, set_fifo},
};
use log::{debug, info, warn};
use plotly::{Layout, Plot, Scatter, common::Mode, layout::Axis};
use rt_datalayer::{datalayer::Datalayer, subscriber::Subscriber};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use waitfree_sync::spsc::spsc;
const SAMPLE: usize = 20_000;
const CYCLETIME_US: u64 = 1_000;

#[repr(C, packed)]
struct Payload {
    idx: u64,
    time: u64,
}
impl Payload {
    fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.idx.to_le_bytes());
        bytes[8..].copy_from_slice(&self.time.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let idx = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let time = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Payload { idx, time }
    }
}

fn now() -> u64 {
    get_time(ClockId::ClockMonotonicRaw)
        .unwrap()
        .as_nanoseconds() as u64
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
    let stop_flag = Arc::new(AtomicBool::new(false));

    let rx_bounded = Arc::new(AtomicBool::new(false));

    let tx_handle = std::thread::spawn({
        let rx_bounded = rx_bounded.clone();
        let stop_flag = stop_flag.clone();
        move || {
            info!("TX Spawned");

            // let mut tx_times = Vec::with_capacity(30_000);
            let datalayer = Datalayer::open("domain").unwrap();
            let mut p = datalayer.publisher("topic").unwrap();
            p.wait_for_subscribers(1);
            rx_bounded.store(true, Ordering::Relaxed);

            set_affinity(Pid::this(), CpuSet::empty().set(2)).unwrap();
            set_fifo(Pid::this(), 95).unwrap();

            info!("TX ready");
            let mut i = 0;
            while !stop_flag.load(Ordering::Relaxed) {
                let msg = Payload {
                    idx: i,
                    time: now(),
                };
                p.try_send(&msg.to_bytes()).unwrap();
                std::thread::sleep(Duration::from_micros(CYCLETIME_US));
                debug!("Send {i}th message");
                i = i.wrapping_add(1);
            }
            info!("TX Done");
            // tx_times
        }
    });
    let (mut tx_lat, mut rx_lat) = spsc(1024);
    let rx_handle = std::thread::spawn(move || {
        info!("RX Spawned");

        // let mut rx_times = Vec::with_capacity(30_000);
        let datalayer = Datalayer::open("domain").unwrap();
        let mut p: Subscriber = datalayer.subscriber("topic").unwrap();
        p.wait_until_connected();

        let mut buffer = [0u8; 1000];
        set_affinity(Pid::this(), CpuSet::empty().set(3)).unwrap();
        set_fifo(Pid::this(), 95).unwrap();

        info!("RX ready");
        let mut count = 0;

        while !stop_flag.load(Ordering::Relaxed) {
            if let Ok((len, _)) = p.try_recv(&mut buffer) {
                let t_rx = now();
                assert_eq!(len, 16);
                let msg = Payload::from_bytes(&buffer);
                let latency = (t_rx - msg.time) as f64 / 1000.0;
                tx_lat.try_send(latency);
                count += 1;
                // info!("Lat {latency} us");
            }
        }
        info!("RX Done");
        // rx_times
    });
    let mut mma = MinMaxAvgCollector::new();
    loop {
        while let Some(lat) = rx_lat.try_recv() {
            mma.insert(lat as i64);
        }
        let mma_state = mma.collect();
        if mma_state.max > 10.0 {
            warn!(
                "min:{:.2}us avg:{:.2}us max:{:.2}us",
                mma_state.min, mma_state.avg, mma_state.max
            );
        } else if mma_state.max >= 7.0 {
            info!(
                "min:{:.2}us avg:{:.2}us max:{:.2}us",
                mma_state.min, mma_state.avg, mma_state.max
            );
        }

        std::thread::sleep(Duration::from_millis(400));
    }
    // while !rx_bounded.load(Ordering::Relaxed) {
    //     std::thread::sleep(Duration::from_millis(2));
    // }
    // run_nrt_load_in_background(20, 500, CYCLETIME_US / 2, stop_flag.clone());
    // let tx_times = tx_handle.join().unwrap();
    // let rx_times = rx_handle.join().unwrap();
    // stop_flag.store(true, Ordering::Relaxed);
    // info!("Benchmark done -> Make plot");
    // // Evaluate measurement
    // let mut latency = vec![];
    // let mut id = vec![];

    // for i in 0..SAMPLE {
    //     if let Some(tx) = tx_times.iter().find(|el| el.0 == i)
    //         && let Some(rx) = rx_times.iter().find(|el| el.0 == i)
    //     {
    //         latency.push((rx.1 - tx.1).as_nanoseconds() as f64 / 1000.0);
    //         id.push(i);
    //     }
    // // }

    // let mut plot = Plot::new();
    // let trace = Scatter::new(id, latency).mode(Mode::Lines).name("Latency");
    // plot.add_trace(trace);
    // plot.set_layout(
    //     Layout::new()
    //         .title(format!(
    //             "rt-datalyer: Latency benchmark with cycle_us={CYCLETIME_US}",
    //         ))
    //         .x_axis(Axis::new().title("Packet ID"))
    //         .y_axis(Axis::new().title("Latency in μs")),
    // );
    // plot.write_html("out.html");
}

pub fn run_nrt_load_in_background(
    num_nrt_threads: usize,
    offset_ms: u64,
    cycletime_us: u64,
    stop_flag: Arc<AtomicBool>,
) {
    let handels = (0..num_nrt_threads)
        .map(|i| {
            std::thread::spawn({
                let stop_flag = stop_flag.clone();
                move || {
                    let start = std::time::Instant::now();
                    while !stop_flag.load(Ordering::Relaxed)
                        && start.elapsed().as_millis() < offset_ms as u128 * i as u128
                    {
                        std::thread::sleep(Duration::from_micros(cycletime_us));
                    }
                    let datalayer = Datalayer::open("domain").unwrap();
                    let mut sub: Subscriber = datalayer.subscriber("topic").unwrap();
                    let mut buf = [0u8; 4];
                    let mut count = 0;
                    while !stop_flag.load(Ordering::Relaxed) {
                        if let Ok((len, all)) = sub.try_recv(&mut buf) {
                            count += 1;
                            // info!(
                            //     "sub[{i}] Got data {:?} dropped {} bytes",
                            //     &buf[0..len],
                            //     all - len
                            // );
                        }
                        if count != 0 && count != SAMPLE && count % 500 == 0 {
                            info!("sub {i}: Received {count}");
                        }
                        std::thread::sleep(Duration::from_micros(cycletime_us));
                    }
                    info!("Exit NRT {i}")
                }
            })
        })
        .collect::<Vec<_>>();
}
