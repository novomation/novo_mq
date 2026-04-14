# Develop hints

## Benchmark
```bash
cargo rbench
```


## Generat Load

```bash
stress-ng --cpu 8 --matrix 4 --mq 3 --vm 2 --vm-bytes 2G -t 20s
```

