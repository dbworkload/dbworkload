#!/usr/bin/python

"""Experimental GIL-free threaded runtime scaffold.

This module is intentionally separate from the current multiprocessing runtime.
It is the future home for a Python free-threaded implementation that can use
`threading.Thread`, `threading.Lock`, and in-process stats aggregation instead
of process supervisors and multiprocessing queues.
"""

import logging
import sys
from pathlib import Path

from dbworkload.connection import ConnInfo

logger = logging.getLogger("dbworkload")


def run(
    concurrency: int,
    workload_path: Path,
    prom_port: int,
    iterations: int,
    procs: int,
    ramp: int,
    conn_info: ConnInfo,
    duration: int,
    conn_duration: int,
    max_rate: int,
    args: dict,
    driver: str,
    quiet: bool,
    save: bool,
    schedule: list,
    histogram_bins: list,
    delay_stats: int,
    log_level: str,
):
    """Run a workload with the experimental GIL-free threaded runtime."""
    logger.error(
        "The GIL-free threaded runtime is scaffolded but not implemented yet. "
        "Use '--runtime multiprocessing' for the current stable runtime."
    )
    sys.exit(1)
