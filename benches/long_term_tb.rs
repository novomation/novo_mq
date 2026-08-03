use log::{debug, info, warn};
use novomq::{
    datalayer::{Datalayer, TbTransport, TbTransportConfig},
    subscriber::Subscriber,
};
use rt_tunex::{
    clock::{ClockId, get_time},
    mman::{MmanFlags, mlockall},
    sched::{CpuSet, Pid, set_affinity, set_fifo},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use waitfree_sync::spsc::spsc;
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
    get_time(ClockId::ClockMonotonic).unwrap().as_nanoseconds() as u64
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

type TbTrans = TbTransport<[u8; 16]>;

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

            set_affinity(Pid::this(), CpuSet::empty().insert(2)).unwrap();
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
    let _ = std::thread::spawn(move || {
        info!("RX Spawned");

        // let mut rx_times = Vec::with_capacity(30_000);
        let datalayer: Datalayer<TbTrans> = Datalayer::open("domain").unwrap();
        let mut p = datalayer
            .subscriber("topic", TbTransportConfig::new())
            .unwrap();
        p.wait_until_connected();

        let mut buffer = [0u8; 1000];
        set_affinity(Pid::this(), CpuSet::empty().insert(3)).unwrap();
        set_fifo(Pid::this(), 95).unwrap();

        info!("RX ready");

        // `try_recv` may return the same (stale) message repeatedly when no
        // new data has been written since the last read. Track the last seen
        // message index so we only record latency for genuinely new messages.
        let mut last_idx: Option<u64> = None;
        while !stop_flag.load(Ordering::Relaxed) {
            if let Ok((len, _)) = p.try_recv(&mut buffer) {
                let t_rx = now();
                assert_eq!(len, 16);
                let msg = Payload::from_bytes(&buffer);
                if last_idx != Some(msg.idx) {
                    last_idx = Some(msg.idx);
                    let latency = (t_rx - msg.time) as f64 / 1000.0;
                    tx_lat.try_send(latency).unwrap();
                    // log::info!("Got msg");
                } else {
                    // log::info!("Got no msg");
                }
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
}
