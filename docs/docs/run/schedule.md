# Schedule

You can now pass a `--schedule` flag to `dbworkload` for better connection and runtime management.

Example `my_schedule.txt`, with inline comments

```text
connections,max_rate,ramp,duration

# create 5 connections, run for 3 minutes ramping up over 2 minutes
5,,2,3

# now scale to 7 connections, run for 5 minutes ramping over 3 minutes
7,,3,5

# scale down to 2 connections only, immediately, and run for 1 minute
2,,0,1

# figure out how to sustain a TPS of 125, run for 1 minute
,125,0,1

# pause for a minute, gotta catch my breath!
0,0,0,1

# 10 threads or 200 max_rate? max_rate wins
10,200,2,4
```

## Example

```bash
dbworkload run -w bank.py \
    --uri 'postgres://cockroach:cockroach@localhost:26257/bank?sslmode=require' \
    --schedule my_schedule.txt 
```
