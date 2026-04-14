# Develop hints

## Benchmark
```bash
cargo rbench
```


## Generate Load

```bash
stress-ng --cpu 8 --matrix 4 --mq 3 --vm 2 --vm-bytes 2G -t 20s
```

## RTSan
```bash
RTSAN_OPTIONS=suppressions=suppressions.supp RTSAN_ENABLE=1 cargo test  test_nonblocking --features rtsan
```