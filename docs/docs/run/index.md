# Workload Run functions

Here is the list of functions and options you can use for running your workload.

| function                    | description                                                  |
| ----------------------------| -------------------------------------------------------------|
| [--schedule](schedule.md)     | Setup a workload run schedule                                |
| [pipe](pipe.md)             | Use a pipe to add/remove connections at runtime              |
| [--max-rate](max-rate.md)             | Configure workload to run based on a max rate, rather than connection count            |
| [--runtime gil-free](gil-free.md) | Experimental runtime for Python free-threaded builds |

!!! warning "Experimental"

    The `--runtime gil-free` implementation is experimental. It is intended for
    Python free-threaded builds and currently does not support `--max-rate` or
    live connection changes through `dbworkload.pipe`.
