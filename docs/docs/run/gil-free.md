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

## Current Limitations

The GIL-free runtime supports fixed-concurrency runs and schedule rows that set
connection count, ramp time, and duration.

These features are not implemented yet:

- `--max-rate`
- schedule rows that use `max_rate`
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

The `max_rate` column must be empty or zero. Rows that request a max rate are
rejected because max-rate control is not implemented yet for this runtime.

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
