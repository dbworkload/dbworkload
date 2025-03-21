# How it works

It’s helpful to understand first what `dbworkload` does:

- At runtime, `dbworkload` first imports the class you pass, `bank.py`.

- It spawns _n_ threads for concurrent execution (see next section on Concurrency).
- By default, it sets the connection to `autocommit` mode.
- Each thread creates a database connection - no need for a connection pool.
- In a loop, each `dbworkload` thread will:
  - execute function `loop()` which returns a list of functions.
  - execute each function in the list sequentially. Each function, typically, executes a SQL statement/transaction.
- Execution stats are funneled back to the _MainThread_, which aggregates and, optionally, prints them to _stdout_ and saves them to a CSV file.
- If the connection drops, it will recreate it. You can also program how long you want the connection to last.
- `dbworkload` stops once a limit has been reached (iteration/duration), or you Ctrl+C.

## Concurrency - processes and threads

`dbworkload` uses both the `multiprocessing` and `threading` library to achieve high concurrency, that is, opening multiple connections to the DBMS.

There are 2 parameters that can be used to configure how many processes you want to create, and for each process, how many threads:

- `--procs/-x`, to configure the count of processes (defaults to the CPU count)
- `--concurrency/-c`, to configure the total number of connections (also referred to as _executing threads_)

`dbworkload` will spread the load across the processes, so that each process has an even amount of threads.

Example: if we set `--procs 4` and `--concurrency 10`, dbworkload will create as follows:

- Process-1: MainThread + 1 extra thread. Total = 2
- Process-2: MainThread + 1 extra thread. Total = 2
- Process-3: MainThread + 2 extra threads. Total = 3
- Process-4: MainThread + 2 extra threads. Total = 3

Total executing threads/connections = 10

This allows you to fine tune the count of Python processes and threads to fit your system.

Furthermore, each _executing thread_ receives a unique ID (an integer).
The ID is passed to the workload class with function `setup()`, along with the total count of threads, i.e. the value passed to `-c/--concurrency`.
You can leverage the ID and the thread count in various ways, for example, to have each thread process a subset of a dataset.
