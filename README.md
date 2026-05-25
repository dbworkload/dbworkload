# dbworkload

`dbworkload` is a Python utility for creating and running bespoke database
workload scripts.

It is designed for cases where you want full control over the workload logic,
but do not want to rebuild the surrounding execution machinery every time. You
write the workload as a Python class; `dbworkload` handles the operational
pieces around it, such as concurrency, process/thread execution, run duration,
iteration limits, scheduling, metrics collection, and result output.

## Why use it?

Database workloads are often highly specific: the order of transactions, the
statements inside each transaction, the shape of generated data, and the way
that data changes over time all matter. `dbworkload` keeps that logic in your
hands while providing a reusable runner around it.

With `dbworkload`, you can:

- model realistic application flows as Python code;
- control which transactions run, in which order, and with which data;
- scale execution across configurable processes, threads, and connections;
- run workloads for a fixed duration or number of iterations;
- collect and export execution statistics;
- target different databases by installing the driver extras you need.

## How it works

A workload is a Python class that defines how connections are set up and what
each execution loop should do. At runtime, `dbworkload` imports that class,
starts the requested level of concurrency, opens database connections, executes
the workload loop, aggregates stats, and stops when the configured limit is
reached or the run is interrupted.

This split keeps the workload script focused on database behavior while the
runner handles the repeatable mechanics of executing it.

## Documentation

The README is only a short introduction. For installation, examples, CLI
reference, supported drivers, and workload authoring details, visit the
official documentation:

<https://dbworkload.github.io/dbworkload/>

## MCP Server

`dbworkload` also ships an optional MCP server for AI coding agents. It exposes
workload authoring guidance and a one-iteration dry-run tool so agents can
generate, validate, and repair workload files locally.

See [dbworkload/mcp/README.md](dbworkload/mcp/README.md) for installation and
client configuration examples.

## License

`dbworkload` is released under the Apache License Version 2.0 license.
