#!/usr/bin/python

import typer

EPILOG = "Docs: <https://dbworkload.github.io/dbworkload/>"


class Param:
    LogLevel = typer.Option(
        "info", "--log-level", "-l", show_choices=True, help="Set the logging level."
    )

    Procs = typer.Option(
        None,
        "--procs",
        "-x",
        help="Number of processes to spawn. Defaults to <system-cpu-count>.",
        show_default=False,
    )

    CSVMaxRows = typer.Option(100000, help="Max count of rows per resulting CSV file.")
