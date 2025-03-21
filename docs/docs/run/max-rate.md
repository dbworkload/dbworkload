# Max Rate

You can pass a `--max-rate` flag and let `dbworkload` calculate and maintain the desired TPS.

## Example

Run **bank** workload and sustain a TPS of 2500.

```bash
dbworkload run -w bank.py \
    --uri 'postgres://cockroach:cockroach@localhost:26257/bank?sslmode=require' \
    --max-rate 2500 
```
