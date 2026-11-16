use log::error;
use rt_tunex::sched::{Pid, set_other};

pub fn set_calling_thread_to_nrt() {
    #[cfg(target_os = "linux")]
    {
        use rt_tunex::sched::{CpuSet, set_affinity};

        if let Some(allowed_cpus) = allowed_cpus() {
            let cpuset = CpuSet::from_bitmask(allowed_cpus);
            let _ = set_affinity(Pid::this(), cpuset.clone()).inspect_err(|err| {
                error!("Could not set affinity {:?} to management: {}", cpuset, err)
            });
        } else {
            error!("Could not set nrt affinity");
        }
    }

    let _ = set_other(Pid::this(), 0)
        .inspect_err(|err| error!("Could not set sched normal to management: {}", err));
}
#[cfg(target_os = "linux")]
fn allowed_cpus() -> Option<u64> {
    let tid = nix::unistd::gettid();
    let status = std::fs::read_to_string(format!("/proc/{tid}/status"))
        .inspect_err(|err| error!("Could not read /proc/{tid}/status: {err}"))
        .ok()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allowed_cpus() {
        set_calling_thread_to_nrt();
    }
}
