use log::{debug, info};
use novomq::{datalayer::Datalayer, subscriber::Subscriber};
use plotly::{Layout, Plot, Scatter, common::Mode, layout::Axis};
use rt_tunex::{
    clock::{ClockId, get_time},
    sched::{CpuSet, Pid, set_affinity, set_fifo},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
const SAMPLE: usize = 20_000;
const CYCLETIME_US: u64 = 1_000;

fn main() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init();
    info!("Hello");
    let stop_flag = Arc::new(AtomicBool::new(false));

    let rx_bounded = Arc::new(AtomicBool::new(false));

    let tx_handle = std::thread::spawn({
        let rx_bounded = rx_bounded.clone();
        move || {
            info!("TX Spawned");

            let mut tx_times = Vec::with_capacity(30_000);
            let datalayer = Datalayer::open("domain").unwrap();
            let mut p = datalayer.publisher("topic").unwrap();
            p.wait_for_subscribers(1);
            rx_bounded.store(true, Ordering::Relaxed);

            set_affinity(Pid::this(), CpuSet::empty().insert(2)).unwrap();
            set_fifo(Pid::this(), 95).unwrap();

            info!("TX ready");
            for i in 0..SAMPLE {
                tx_times.push((i, get_time(ClockId::ClockMonotonic).unwrap()));
                p.try_send(&i.to_le_bytes()).unwrap();
                std::thread::sleep(Duration::from_micros(CYCLETIME_US));
                debug!("Send {i}th message");
            }
            info!("TX Done");
            tx_times
        }
    });
    let rx_handle = std::thread::spawn(move || {
        info!("RX Spawned");

        let mut rx_times = Vec::with_capacity(30_000);
        let datalayer = Datalayer::open("domain").unwrap();
        let mut p: Subscriber = datalayer.subscriber("topic").unwrap();
        p.wait_until_connected();

        let mut buffer = [0u8; 1000];
        set_affinity(Pid::this(), CpuSet::empty().insert(3)).unwrap();
        set_fifo(Pid::this(), 95).unwrap();

        info!("RX ready");
        let mut count = 0;

        while count < SAMPLE {
            if let Ok((len, _)) = p.try_recv(&mut buffer) {
                let i = usize::from_le_bytes(buffer[0..len].try_into().unwrap());
                rx_times.push((i, get_time(ClockId::ClockMonotonic).unwrap()));
                count += 1;
                // info!("Received new message. {i}/{count}.");
            }
        }
        info!("RX Done");
        rx_times
    });
    while !rx_bounded.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(2));
    }
    run_nrt_load_in_background(20, 500, CYCLETIME_US / 2, stop_flag.clone());
    let tx_times = tx_handle.join().unwrap();
    let rx_times = rx_handle.join().unwrap();
    stop_flag.store(true, Ordering::Relaxed);
    info!("Benchmark done -> Make plot");
    // Evaluate measurement
    let mut latency = vec![];
    let mut id = vec![];

    for i in 0..SAMPLE {
        if let Some(tx) = tx_times.iter().find(|el| el.0 == i)
            && let Some(rx) = rx_times.iter().find(|el| el.0 == i)
        {
            latency.push((rx.1 - tx.1).as_nanoseconds() as f64 / 1000.0);
            id.push(i);
        }
    }

    let mut plot = Plot::new();
    let trace = Scatter::new(id, latency).mode(Mode::Lines).name("Latency");
    plot.add_trace(trace);
    plot.set_layout(
        Layout::new()
            .title(format!(
                "rt-datalyer: Latency benchmark with cycle_us={CYCLETIME_US}",
            ))
            .x_axis(Axis::new().title("Packet ID"))
            .y_axis(Axis::new().title("Latency in μs")),
    );
    plot.write_html("out.html");
}

pub fn run_nrt_load_in_background(
    num_nrt_threads: usize,
    offset_ms: u64,
    cycletime_us: u64,
    stop_flag: Arc<AtomicBool>,
) {
    let _handels = (0..num_nrt_threads)
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
                        if let Ok((_len, _all)) = sub.try_recv(&mut buf) {
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
