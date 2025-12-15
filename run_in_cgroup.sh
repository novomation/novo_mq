#!/bin/sh
echo "Creating RT cgroup"
set -euo pipefail
UUID=rt_test
sudo cgcreate -t rbg:rbg -a rbg:rbg -g cpuset:$UUID
sudo cgset -r cpuset.cpus=0-5 $UUID

trap 'echo "Interrupted - moving on ...";' INT
# sudo chown -R rbg:rbg /sys/fs/cgroup/rt_test/cgroup.procs

# echo $$ > /sys/fs/cgroup/rt_test/cgroup.procs
# bash -c "$*"
sudo --preserve-env=HOME,USER,PATH,CARGO_HOME cgexec -g cpuset:$UUID unshare bash -c "$*" || true
echo "Deleteting RT cgroup"
sudo cgdelete cpuset:$UUID