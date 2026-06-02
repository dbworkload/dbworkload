# GIL-free Runtime

!!! warning "Experimental feature"

    The GIL-free runtime is experimental. The default `multiprocessing` runtime
    remains the stable runtime for normal dbworkload runs.

Python free-threaded builds can run Python bytecode without the Global Interpreter
Lock (GIL). In dbworkload, this makes it possible to experiment with a simpler
thread-based runtime:

```text
one Python process
many worker threads
shared in-process stats
```

This avoids the process supervisor and multiprocessing queue machinery used by
the default runtime.

## Usage

Use the experimental runtime with:

```bash
dbworkload run \
    --runtime gil-free \
    -w bank.py \
    --uri 'postgres://user:password@localhost:26257/bank?sslmode=disable' \
    --concurrency 8 \
    --duration 60
```

The runtime is useful when testing Python free-threaded builds such as
`python3.14t`.

## HTTP Control Endpoint

The GIL-free runtime starts a small HTTP control endpoint by default on port
`26160`. It listens on IPv4 `0.0.0.0` and IPv6 `[::]`.

Use `adjust_count` to add or remove workers while the run is active:

```bash
curl "http://localhost:26160/?adjust_count=5"
curl "http://localhost:26160/?adjust_count=-2"
```

The endpoint adjusts the target worker count. Connections are added or removed
through the same internal worker machinery used by schedule and max-rate control,
so changes are cooperative rather than forcibly interrupting in-flight work.

Use `--control-port` to choose a different port:

```bash
dbworkload run --runtime gil-free --control-port 8282 ...
```

## Current Limitations

The GIL-free runtime supports fixed-concurrency runs, `--max-rate`, and schedule
rows that set connection count, max rate, ramp time, and duration. It also
supports live connection changes through the HTTP control endpoint.

This feature is not implemented yet:

- live connection changes through `dbworkload.pipe`

If you need those features, use the default runtime:

```bash
dbworkload run --runtime multiprocessing ...
```

## Schedule Support

The GIL-free runtime supports schedule rows like:

```text
connections,max_rate,ramp,duration
2,,0,1
8,,2,5
1,,0,1
```

It also supports rows that use `max_rate`:

connections,max_rate,ramp,duration
,3000,0,5
4,3000,1,5
```

When `max_rate` is set and `connections` is empty or zero, dbworkload starts with
one worker, measures `__cycle__` throughput, and extrapolates how many workers
are needed. If the workload overshoots the target, each worker adds a small
per-cycle pause to float around the requested rate.

## Checking the GIL

When testing this runtime, confirm that the Python interpreter is actually a
free-threaded build:

```bash
python -c "import sysconfig; print(sysconfig.get_config_var('Py_GIL_DISABLED'))"
```

Expected output:

```text
1
```

Some native extensions may cause Python to re-enable the GIL at runtime. To make
that visible during testing, run with:

```bash
PYTHON_GIL=0 dbworkload run --runtime gil-free ...
```

If the GIL is re-enabled, dbworkload should refuse to continue once the runtime
guard detects it.
