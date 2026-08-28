use log::error;
use rt_tunex::sched::{CpuSet, Pid, set_affinity, set_other};

pub fn set_calling_thread_to_nrt() {
    if let Some(allowed_cpus) = allowed_cpus() {
        let cpuset = CpuSet::from_bitmask(allowed_cpus);
        let _ = set_affinity(Pid::this(), cpuset.clone()).inspect_err(|err| {
            error!("Could not set affinity {:?} to management: {}", cpuset, err)
        });
    } else {
        error!("Could not set nrt affinity");
    }

    let _ = set_other(Pid::this(), 0)
        .inspect_err(|err| error!("Could not set sched normal to management: {}", err));
}
#[cfg(target_os = "linux")]
fn allowed_cpus() -> Option<u64> {
    let tid = unsafe { nix::libc::getpid() };
    let status = std::fs::read_to_string(format!("/proc/{tid}/status")).unwrap();
    status.lines().find_map(|line| {
        let mut splitted = line.split(":");
        let key = splitted.next()?;
        if key == "Cpus_allowed" {
            splitted
                .next()
                .map(|allowed| u64::from_str_radix(allowed.trim(), 16).unwrap_or_default())
        } else {
            None
        }
    })
}

#[cfg(target_os = "macos")]
fn allowed_cpus() -> Option<u64> {
    // macOS does not expose /proc/<pid>/status (the Linux procfs). Instead we
    // ask the OS for the number of usable processor parallelism and build a
    // best-effort contiguous low-bits mask from it.
    let cpus = std::thread::available_parallelism().ok()?.get() as u64;
    if cpus == 0 {
        return None;
    }
    if cpus >= 64 {
        Some(u64::MAX)
    } else {
        Some((1u64 << cpus) - 1)
    }
}

#[cfg(target_os = "windows")]
fn allowed_cpus() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allowed_cpus() {
        set_calling_thread_to_nrt();
    }
}
