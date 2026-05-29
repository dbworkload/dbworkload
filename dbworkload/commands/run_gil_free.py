#!/usr/bin/python

"""Experimental GIL-free threaded runtime.

This is a first-pass implementation for Python free-threaded builds. It keeps
the behavior intentionally narrow: fixed concurrency, optional ramp, optional
duration, optional iterations, periodic stats, and graceful Ctrl+C shutdown.
"""

import logging
import random
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock, Thread

import numpy as np
import tabulate
from psutil import cpu_percent, virtual_memory

from dbworkload.commands.run import (
    FINAL_HEADERS,
    FREQUENCY,
    HEADERS_CSV,
    MAX_RETRIES,
    get_connection,
    log_and_sleep,
    print_stats,
    run_transaction,
)
from dbworkload.connection import ConnInfo
from dbworkload.utils.common import Prom, Stats, WorkerStats, import_class_at_runtime

logger = logging.getLogger("dbworkload")


@dataclass
class RunState:
    stats: Stats
    lock: Lock
    stop_event: Event
    active_connections: int = 0
    peak_connections: int = 0
    task_done_threads: int = 0
    stats_received: int = 0
    worker_error: BaseException | None = None

    def add_stats(self, worker_stats: WorkerStats) -> None:
        tds = worker_stats.get_tdigest_ndarray()
        if not tds:
            return
        with self.lock:
            self.stats.add_tds(tds)
            self.stats_received += 1

    def mark_started(self) -> None:
        with self.lock:
            self.active_connections += 1
            self.peak_connections = max(self.peak_connections, self.active_connections)

    def mark_stopped(self, task_done: bool = False) -> None:
        with self.lock:
            self.active_connections = max(0, self.active_connections - 1)
            if task_done:
                self.task_done_threads += 1

    def set_error(self, error: BaseException) -> None:
        with self.lock:
            self.worker_error = error
        self.stop_event.set()


def require_gil_disabled() -> None:
    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)

    if is_gil_enabled is None:
        logger.error("The GIL-free runtime requires Python 3.13+ free-threaded builds.")
        sys.exit(1)

    if is_gil_enabled():
        logger.error("The GIL is enabled. Refusing to run --runtime gil-free.")
        sys.exit(1)

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
    
    require_gil_disabled()

    # TODO: implement max-rate concurrency adjustments for the GIL-free runtime.
    if max_rate:
        logger.error("--runtime gil-free does not support --max-rate yet.")
        sys.exit(1)

    # TODO: implement multi-step schedules for the GIL-free runtime.
    if schedule:
        logger.error("--runtime gil-free does not support --schedule yet.")
        sys.exit(1)

    # TODO: implement dbworkload.pipe dynamic resizing for the GIL-free runtime.

    logger.setLevel(log_level)

    start_time = int(time.time())
    workload = import_class_at_runtime(workload_path)
    run_name = (
        workload.__name__
        + "."
        + time.strftime("%Y%m%d_%H%M%S", time.gmtime(start_time))
    )

    logger.info(f"Starting workload {run_name} with GIL-free threaded runtime")

    if save:
        with open(run_name + ".csv", "w") as f:
            f.write(",".join(HEADERS_CSV) + "\n")

    state = RunState(
        stats=Stats(start_time),
        lock=Lock(),
        stop_event=Event(),
    )
    prom = Prom(prom_port, state.stats, histogram_bins)
    workers: list[Thread] = []
    hard_stop = False

    def signal_handler(sig, frame):
        nonlocal hard_stop
        logger.info("KeyboardInterrupt signal detected. Stopping threads...")
        if hard_stop:
            logger.warning("Forcibly quitting.")
            sys.exit(1)
        hard_stop = True
        state.stop_event.set()

    def write_csv(report: list, centroids, endtime: int) -> None:
        if not save:
            return
        with open(run_name + ".csv", "a") as f:
            for row in report:
                f.write(str(endtime) + ",")
                for col in row:
                    f.write(str(col) + ",")
                np.savetxt(f, next(centroids), newline=";")
                f.write("\n")

    def publish_window(endtime: int) -> None:
        with state.lock:
            active_connections = state.active_connections
            stats_received = state.stats_received

            cpu_util = cpu_percent()
            vmem = virtual_memory().percent
            if (
                stats_received != active_connections
                or cpu_util > 70
                or vmem > 70
            ):
                logger.warning(
                    f"{stats_received=}, expected={active_connections}. "
                    f"CPU Util={cpu_util}%, Memory={vmem}%"
                )

            report = state.stats.calculate_stats(active_connections, endtime)
            centroids = state.stats.get_centroids()
            state.stats.new_window(endtime)
            state.stats_received = 0

        write_csv(report, centroids, endtime)

        if not quiet:
            print_stats(report)

        prom.publish(report)

    def graceful_shutdown() -> None:
        logger.debug("Gracefully shutting down GIL-free runtime...")
        state.stop_event.set()

        for worker_thread in workers:
            worker_thread.join()

        end_time = int(time.time())
        final_connections = max(state.peak_connections, concurrency)

        with state.lock:
            report = state.stats.calculate_stats(
                final_connections,
                end_time - delay_stats,
            )
            centroids = state.stats.get_centroids()

        write_csv(report, centroids, state.stats.endtime)

        if not quiet:
            logger.info("Printing final stats")
            print_stats(report)

        prom.publish(report)

        logger.info("Printing summary for the full test run")

        final_stats_report = tabulate.tabulate(
            state.stats.calculate_final_stats(final_connections, state.stats.endtime),
            FINAL_HEADERS,
            tablefmt="simple_outline",
            intfmt=",",
            floatfmt=",.2f",
        )

        runtime_params = tabulate.tabulate(
            [
                ["runtime", "gil-free"],
                ["workload_path", workload_path],
                ["conn_params", conn_info.params],
                ["conn_extras", conn_info.extras],
                ["concurrency", concurrency],
                ["duration", duration],
                ["iterations", iterations],
                ["ramp", ramp],
                ["args", args],
                ["delay_stats", delay_stats],
            ],
            headers=["Parameter", "Value"],
        )

        runtime_details = tabulate.tabulate(
            [
                ["run_name", run_name],
                [
                    "start_time",
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_time)),
                ],
                ["end_time", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(end_time))],
                ["test_duration", int(end_time - start_time)],
            ],
        )

        if save:
            with open(run_name + ".txt", "w") as f:
                f.writelines(
                    [
                        runtime_details,
                        "\n\n",
                        final_stats_report,
                        "\n\n",
                        runtime_params,
                        "\n",
                    ]
                )

        print(
            "\n",
            runtime_details,
            "\n\n",
            final_stats_report,
            "\n\n",
            runtime_params,
            "\n",
            sep="",
        )

    signal.signal(signal.SIGINT, signal_handler)

    iterations_per_thread = None
    if iterations:
        concurrency = min(iterations, concurrency)
        iterations_per_thread = iterations // concurrency

        if iterations % concurrency > 0:
            logger.warning(
                f"You have requested {iterations} iterations on {concurrency} "
                f"threads. {iterations} modulo {concurrency} = "
                f"{iterations % concurrency} iterations will not be executed."
            )

    ramp_interval = ramp / concurrency if ramp and concurrency else 0
    offset = start_time % FREQUENCY

    for worker_id in range(concurrency):
        worker_thread = Thread(
            target=worker,
            daemon=True,
            args=(
                state,
                log_level,
                conn_info,
                driver,
                workload,
                args,
                conn_duration,
                offset,
                worker_id,
                iterations_per_thread,
                concurrency,
            ),
        )
        worker_thread.start()
        workers.append(worker_thread)
        if ramp_interval:
            time.sleep(ramp_interval)

    report_time = start_time + FREQUENCY + delay_stats
    end_time = time.time() + duration if duration else float("inf")

    try:
        while time.time() < end_time and not state.stop_event.is_set():
            with state.lock:
                task_done_threads = state.task_done_threads
                active_connections = state.active_connections
                worker_error = state.worker_error

            if worker_error:
                logger.error(
                    f"error_type={worker_error.__class__.__name__}, {worker_error=}"
                )
                break

            if task_done_threads > 0 and task_done_threads >= len(workers):
                logger.info("Requested iteration limit reached")
                break

            if time.time() >= report_time:
                publish_window(int(time.time() - delay_stats))
                report_time += FREQUENCY

            time.sleep(0.001)
    finally:
        graceful_shutdown()

    if state.worker_error:
        sys.exit(1)


def worker(
    state: RunState,
    log_level: str,
    conn_info: ConnInfo,
    driver: str,
    workload: object,
    args: dict,
    conn_duration: int,
    offset: int,
    id: int = 0,
    iterations: int = 0,
    concurrency: int = 0,
):
    logger.setLevel(log_level)
    logger.debug(f"Thread ID {id} started")

    try:
        w = workload(args)
    except Exception:
        state.set_error(Exception(traceback.format_exc()))
        return

    c = 0
    conn_endtime = 0
    ws = WorkerStats()
    run_init = True
    stopped = False

    state.mark_started()

    try:
        while not state.stop_event.is_set():
            if conn_duration:
                conn_endtime = time.time() + int(
                    conn_duration * random.uniform(0.8, 1.2)
                )

            try:
                logger.debug(f"driver: {driver}, params: {conn_info.params}")
                with get_connection(driver, conn_info) as conn:
                    logger.debug("Connection started")

                    if run_init:
                        run_init = False
                        if hasattr(w, "setup") and callable(w.setup):
                            run_transaction(
                                conn,
                                lambda conn: w.setup(conn, id, concurrency),
                                driver,
                                max_retries=MAX_RETRIES,
                            )

                    ts = int(time.time())
                    stat_time = ts + FREQUENCY - ts % FREQUENCY + offset

                    while not state.stop_event.is_set():
                        if iterations and c >= iterations:
                            logger.debug("Task completed!")
                            state.add_stats(ws)
                            state.mark_stopped(task_done=True)
                            stopped = True
                            return

                        if conn_duration and time.time() >= conn_endtime:
                            logger.debug(
                                "conn_duration reached, will reset the connection."
                            )
                            break

                        cycle_start = time.time()
                        for txn in w.loop():
                            if state.stop_event.is_set():
                                break

                            start = time.time()
                            retries = run_transaction(
                                conn,
                                lambda conn: txn(conn),
                                driver,
                                max_retries=MAX_RETRIES,
                            )

                            for _ in range(retries):
                                ws.add_latency_measurement("__retries__", 0)

                            if retries < MAX_RETRIES:
                                ws.add_latency_measurement(
                                    txn.__name__, time.time() - start
                                )

                        c += 1
                        ws.add_latency_measurement(
                            "__cycle__", time.time() - cycle_start
                        )

                        if time.time() >= stat_time:
                            state.add_stats(ws)
                            ws.new_window()
                            stat_time += FREQUENCY

            except Exception as e:
                if is_retryable_driver_error(driver, e):
                    if not state.stop_event.is_set():
                        log_and_sleep(e)
                    continue

                state.set_error(e)
                return
    finally:
        if not stopped:
            state.add_stats(ws)
            state.mark_stopped()


def is_retryable_driver_error(driver: str, error: Exception) -> bool:
    if driver == "postgres":
        import psycopg

        if isinstance(error, psycopg.errors.UndefinedTable):
            return False
        return True

    if driver == "mysql":
        import mysql.connector.errorcode

        return error.errno != mysql.connector.errorcode.ER_NO_SUCH_TABLE

    if driver == "maria":
        return not str(error).endswith(" doesn't exist")

    if driver == "oracle":
        return not str(error).startswith("ORA-00942: table or view does not exist")

    if driver == "pinecone":
        from pinecone.exceptions import PineconeException

        if isinstance(error, PineconeException):
            status = getattr(error, "status", None)
            return status not in (400, 401, 403, 404)

    return False
