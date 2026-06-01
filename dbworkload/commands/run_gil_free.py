#!/usr/bin/python

"""Experimental GIL-free threaded runtime.

This is a first-pass implementation for Python free-threaded builds. It keeps
the behavior intentionally narrow: fixed concurrency, optional ramp, optional
duration, optional iterations, periodic stats, and graceful Ctrl+C shutdown.
"""

import json
import logging
import math
import random
import signal
import socket
import sys
import time
import traceback
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Lock, Thread
from urllib.parse import parse_qs, urlparse

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

MAX_RATE_LOWER_BAND = 0.90
MAX_RATE_UPPER_BAND = 1.10
MAX_RATE_MASSIVE_OVERSHOOT = 2.0
MAX_RATE_ADJUSTMENT_COOLDOWN = 60
MAX_RATE_MAX_CYCLE_PAUSE = 1.0
MAX_RATE_EWMA_ALPHA = 0.40
CONTROL_BIND_IPV4 = "0.0.0.0"
CONTROL_BIND_IPV6 = "::"


class IPv6ThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer variant that binds an IPv6 socket only.

    dbworkload starts one IPv4 listener and one IPv6 listener on the same port.
    Setting IPV6_V6ONLY avoids the common dual-stack ambiguity where binding
    [::] may also claim 0.0.0.0 on some platforms.
    """

    address_family = socket.AF_INET6

    def server_bind(self) -> None:
        try:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        except OSError:
            logger.debug("Could not set IPV6_V6ONLY on control server socket.")
        super().server_bind()


@dataclass
class RunState:
    """Shared state for all worker threads in this single Python process."""

    stats: Stats
    lock: Lock
    stop_event: Event
    active_connections: int = 0
    peak_connections: int = 0
    task_done_threads: int = 0
    stats_received: int = 0
    cycle_pause: float = 0
    worker_error: BaseException | None = None

    def add_stats(self, worker_stats: WorkerStats) -> None:
        tds = worker_stats.get_tdigest_ndarray()
        if not tds:
            return

        # Stats is shared by every worker thread. On free-threaded Python the GIL
        # does not serialize bytecode execution, so updates to the shared Stats
        # object and its counters must be protected explicitly.
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

        # One worker failure stops the whole run. Event is used because it is a
        # thread-safe latch: one thread calls set(), all other threads can poll
        # is_set() cheaply without sharing a raw bool.
        self.stop_event.set()

    def set_cycle_pause(self, pause: float) -> None:
        # max-rate control writes this value from the reporting thread. Workers
        # read it once per cycle and sleep outside the lock.
        with self.lock:
            self.cycle_pause = max(0, pause)

    def get_cycle_pause(self) -> float:
        with self.lock:
            return self.cycle_pause


@dataclass
class WorkerHandle:
    """Bookkeeping for one OS thread running a workload worker."""

    id: int
    thread: Thread

    # This is a per-worker shutdown signal. The global RunState.stop_event stops
    # the entire run; this event lets schedule scaling stop only selected
    # workers when a later schedule row lowers the target concurrency.
    stop_event: Event
    stopping: bool = False


@dataclass
class MaxRateControllerState:
    """State for the GIL-free max-rate controller.

    max-rate is intentionally split into two controls:

    1. Worker count is coarse control. It is useful when the workload is far
       below the requested rate and more concurrency is needed.
    2. Per-cycle pause is fine control. It is preferred when the workload is
       above the requested rate, because reducing workers can undershoot and then
       force the controller into an add/remove oscillation.

    The cooldown applies only to worker-count changes. The pause can be adjusted
    every reporting window because it is cheap, reversible, and does not distort
    the next stats window as much as adding/removing workers. When a worker-count
    change uses ramp_time, the cooldown is extended by that ramp time because the
    stats gathered during ramp-up/ramp-down are not steady-state measurements.
    """

    smoothed_rate: float = 0
    next_worker_adjustment_time: float = 0

    def reset(self) -> None:
        # A schedule row is a new control problem: the target rate, target
        # connections, and ramp may all change. Carrying a previous smoothed
        # rate into the new row would make the first decisions depend on stale
        # measurements from a different target.
        self.smoothed_rate = 0
        self.next_worker_adjustment_time = 0


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
    control_port: int = 26160,
):
    """Run a workload with the experimental GIL-free threaded runtime."""

    require_gil_disabled()

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

    # workers holds Thread objects plus their per-worker stop Events. The list
    # itself is shared by the main scheduler loop and the adjustment thread, so
    # list mutation is protected by workers_lock.
    workers: list[WorkerHandle] = []
    workers_lock = Lock()
    control_lock = Lock()
    max_rate_state = MaxRateControllerState()
    hard_stop = False
    control_servers: list[ThreadingHTTPServer] = []

    def signal_handler(sig, frame):
        nonlocal hard_stop
        logger.info("KeyboardInterrupt signal detected. Stopping threads...")
        if hard_stop:
            logger.warning("Forcibly quitting.")
            sys.exit(1)
        hard_stop = True

        # Ctrl+C is a global stop. Workers will notice this Event at their next
        # polling point and flush their local WorkerStats before returning.
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

    def publish_window(endtime: int) -> list:
        # Reporting takes a consistent snapshot of the shared Stats object, then
        # resets the current window while workers are briefly blocked from
        # adding new measurements.
        with state.lock:
            active_connections = state.active_connections
            stats_received = state.stats_received

            cpu_util = cpu_percent()
            vmem = virtual_memory().percent
            if stats_received != active_connections or cpu_util > 70 or vmem > 70:
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
        return report

    def graceful_shutdown() -> None:
        logger.debug("Gracefully shutting down GIL-free runtime...")
        state.stop_event.set()

        for server in control_servers:
            server.shutdown()
            server.server_close()

        # If a schedule ramp is still adding/removing workers, wait for it before
        # joining the actual worker threads.
        if adjustment_thread and adjustment_thread.is_alive():
            adjustment_thread.join()

        with workers_lock:
            worker_handles = list(workers)

        for worker_handle in worker_handles:
            worker_handle.stop_event.set()

        # Thread.join() replaces the old multiprocessing queue drain. Every
        # worker's finally block flushes its WorkerStats before the join returns.
        for worker_handle in worker_handles:
            worker_handle.thread.join()

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
                ["control_port", control_port],
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

    offset = start_time % FREQUENCY
    next_worker_id = 0

    def start_worker(schedule_concurrency: int) -> None:
        nonlocal next_worker_id
        worker_id = next_worker_id
        next_worker_id += 1

        # Each worker gets its own Event so schedule changes can stop a subset
        # of workers. A plain boolean would need a lock or busy coordination;
        # Event gives us a small thread-safe signalling primitive.
        worker_stop_event = Event()
        worker_thread = Thread(
            target=worker,
            daemon=True,
            args=(
                state,
                worker_stop_event,
                log_level,
                conn_info,
                driver,
                workload,
                args,
                conn_duration,
                offset,
                worker_id,
                iterations_per_thread,
                schedule_concurrency,
            ),
            name=f"dbworkload-gil-free-{worker_id}",
        )
        worker_thread.start()

        with workers_lock:
            workers.append(
                WorkerHandle(
                    id=worker_id,
                    thread=worker_thread,
                    stop_event=worker_stop_event,
                )
            )

    def reap_workers() -> None:
        # Remove finished threads from the bookkeeping list. This keeps later
        # schedule adjustments from trying to stop already-completed workers.
        with workers_lock:
            workers[:] = [x for x in workers if x.thread.is_alive()]

    def active_worker_handles() -> list[WorkerHandle]:
        # Only workers that are alive and not already selected for shutdown are
        # eligible for the next scale-down decision.
        with workers_lock:
            return [x for x in workers if x.thread.is_alive() and not x.stopping]

    def active_worker_count() -> int:
        return len(active_worker_handles())

    def adjust_workers(cc_change: int, ramp_time: int, schedule_concurrency: int):
        # Schedule rows express target concurrency. This helper applies the
        # difference from the previous target, optionally spacing changes across
        # ramp_time seconds.
        if cc_change == 0:
            return

        ramp_interval = ramp_time / abs(cc_change) if ramp_time else 0

        if cc_change > 0:
            for _ in range(cc_change):
                if state.stop_event.is_set():
                    return
                start_worker(schedule_concurrency)
                if ramp_interval:
                    time.sleep(ramp_interval)

        if cc_change < 0:
            # Negative slicing selects the tail of the active worker list. There
            # is no correctness requirement for which workers stop; we only need
            # to stop the requested count.
            for worker_handle in active_worker_handles()[cc_change:]:
                if state.stop_event.is_set():
                    return
                worker_handle.stopping = True
                worker_handle.stop_event.set()
                if ramp_interval:
                    time.sleep(ramp_interval)

    def request_worker_target_unlocked(target_cc: int, ramp_time: int) -> bool:
        nonlocal adjustment_thread, current_cc
        target_cc = max(0, target_cc)
        cc_change = target_cc - current_cc
        if cc_change == 0:
            return False

        if adjustment_thread and adjustment_thread.is_alive():
            logger.debug("Skipping worker adjustment; previous adjustment is active.")
            return False

        adjustment_thread = Thread(
            target=adjust_workers,
            daemon=True,
            args=(cc_change, ramp_time, target_cc),
        )
        adjustment_thread.start()
        current_cc = target_cc
        return True

    def request_worker_target(target_cc: int, ramp_time: int) -> bool:
        # Schedule, max-rate, and the HTTP control server can all request
        # worker-count changes. Serialize those decisions so current_cc remains
        # the single source of truth for the requested target concurrency.
        with control_lock:
            return request_worker_target_unlocked(target_cc, ramp_time)

    def request_worker_adjustment(
        adjust_count: int, ramp_time: int
    ) -> tuple[bool, int, int]:
        # HTTP control expresses changes as deltas rather than absolute targets.
        # Keep the read-modify-write under the same lock so concurrent requests
        # cannot accidentally calculate from a stale target count.
        with control_lock:
            previous_target = current_cc
            target_cc = max(0, previous_target + adjust_count)
            changed = request_worker_target_unlocked(target_cc, ramp_time)
            return changed, previous_target, target_cc

    def requested_worker_count() -> int:
        with control_lock:
            return current_cc

    def make_control_handler():
        class ControlHandler(BaseHTTPRequestHandler):
            """HTTP control endpoint for live GIL-free concurrency adjustment."""

            server_version = "dbworkload-control"
            sys_version = ""

            def log_message(self, format: str, *args) -> None:
                logger.debug("control server: " + format, *args)

            def send_json(self, status: int, payload: dict) -> None:
                body = json.dumps(payload, sort_keys=True).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                values = parse_qs(parsed.query, keep_blank_values=True)

                if parsed.path != "/" or "adjust_count" not in values:
                    self.send_json(
                        400,
                        {
                            "error": "expected GET /?adjust_count=<integer>",
                            "target_connections": requested_worker_count(),
                            "active_connections": active_worker_count(),
                        },
                    )
                    return

                try:
                    adjust_count = int(values["adjust_count"][0])
                except (TypeError, ValueError):
                    self.send_json(
                        400,
                        {
                            "error": "adjust_count must be a positive or negative integer",
                            "target_connections": requested_worker_count(),
                            "active_connections": active_worker_count(),
                        },
                    )
                    return

                changed, previous_target, target_cc = request_worker_adjustment(
                    adjust_count, 0
                )
                if target_cc == previous_target:
                    self.send_json(
                        200,
                        {
                            "adjust_count": adjust_count,
                            "active_connections": active_worker_count(),
                            "changed": False,
                            "target_connections": previous_target,
                        },
                    )
                    return

                if not changed:
                    self.send_json(
                        409,
                        {
                            "error": "worker adjustment already in progress",
                            "active_connections": active_worker_count(),
                            "target_connections": requested_worker_count(),
                        },
                    )
                    return

                logger.info(
                    "HTTP control adjusted target connections by %s: %s -> %s",
                    adjust_count,
                    previous_target,
                    target_cc,
                )
                self.send_json(
                    200,
                    {
                        "adjust_count": adjust_count,
                        "active_connections": active_worker_count(),
                        "changed": True,
                        "target_connections": target_cc,
                    },
                )

        return ControlHandler

    def start_control_server() -> None:
        if not control_port:
            logger.info("GIL-free HTTP control server disabled.")
            return

        handler = make_control_handler()
        listeners = [
            (ThreadingHTTPServer, CONTROL_BIND_IPV4),
            (IPv6ThreadingHTTPServer, CONTROL_BIND_IPV6),
        ]

        for server_cls, host in listeners:
            try:
                server = server_cls((host, control_port), handler)
            except OSError as e:
                logger.warning(
                    "Could not start GIL-free HTTP control server on %s:%s: %s",
                    host,
                    control_port,
                    e,
                )
                continue

            server_thread = Thread(
                target=server.serve_forever,
                daemon=True,
                name=f"dbworkload-gil-free-control-{host}",
            )
            server_thread.start()
            control_servers.append(server)
            logger.info(
                "GIL-free HTTP control server listening on %s:%s",
                host,
                control_port,
            )

    def cooldown_expired() -> bool:
        return time.time() >= max_rate_state.next_worker_adjustment_time

    def record_worker_adjustment(ramp_time: int) -> None:
        # Worker changes distort the next stats windows in two ways:
        #
        # 1. A fixed settling period gives the database/client time to absorb
        #    the new connection count.
        # 2. ramp_time explicitly spreads the change over time, so measurements
        #    taken during that ramp are a blend of old and new concurrency.
        #
        # The controller therefore waits for both periods before changing the
        # worker count again. Per-cycle pause is still allowed during cooldown.
        max_rate_state.next_worker_adjustment_time = (
            time.time() + MAX_RATE_ADJUSTMENT_COOLDOWN + ramp_time
        )

    def get_cycle_rate(report: list) -> int:
        for row in report:
            if row[1] == "__cycle__":
                return row[6]
        return 0

    def apply_max_rate_control(target_rate: int, report: list, ramp_time: int) -> None:
        """Adjust workers and/or per-cycle pause to approach target_rate.

        The controller avoids rapid add/remove oscillation with three rules:

        * A dead band accepts rates close to the target. Between
          MAX_RATE_LOWER_BAND and MAX_RATE_UPPER_BAND we keep the worker count
          unchanged and only slowly reduce any existing pause.
        * Overshoot is handled with pause first. Worker removal is reserved for
          massive overshoot or cases where the required pause would be too large.
        * Worker-count changes are rate-limited by a cooldown. The effective
          cooldown is MAX_RATE_ADJUSTMENT_COOLDOWN + ramp_time, because ramped
          changes skew the stats window until the ramp has completed.
        """
        nonlocal current_cc

        current_rate = get_cycle_rate(report)
        active_cc = max(1, active_worker_count())

        if current_rate > 0:
            if max_rate_state.smoothed_rate:
                max_rate_state.smoothed_rate = (
                    MAX_RATE_EWMA_ALPHA * current_rate
                    + (1 - MAX_RATE_EWMA_ALPHA) * max_rate_state.smoothed_rate
                )
            else:
                max_rate_state.smoothed_rate = current_rate

        control_rate = max_rate_state.smoothed_rate or current_rate

        if current_rate <= 0:
            state.set_cycle_pause(0)
            if cooldown_expired():
                if request_worker_target(max(1, current_cc + 1), ramp_time):
                    record_worker_adjustment(ramp_time)
            return

        lower_bound = target_rate * MAX_RATE_LOWER_BAND
        upper_bound = target_rate * MAX_RATE_UPPER_BAND

        # Inside the dead band, avoid worker-count changes. If we were applying
        # a pause, decay it gently so the controller can recover when the target
        # is raised or the workload naturally slows down.
        if lower_bound <= control_rate <= upper_bound:
            state.set_cycle_pause(state.get_cycle_pause() * 0.5)
            return

        per_worker_rate = control_rate / active_cc

        if control_rate < lower_bound:
            state.set_cycle_pause(0)
            if not cooldown_expired():
                return

            target_cc = max(current_cc + 1, math.ceil(target_rate / per_worker_rate))
            previous_cc = current_cc
            if not request_worker_target(target_cc, ramp_time):
                return

            record_worker_adjustment(ramp_time)
            logger.warning(
                "Increasing workers for max_rate: desired max_rate: %s, "
                "current_rate: %s, smoothed_rate: %.2f, current_cc: %s, "
                "target_cc: %s, cooldown_until: %.2f",
                target_rate,
                current_rate,
                control_rate,
                previous_cc,
                target_cc,
                max_rate_state.next_worker_adjustment_time,
            )
            return

        # Overshoot path. First calculate the sleep needed to bring the current
        # worker count down to the target. This is the fine-control path and is
        # preferred over removing workers.
        current_interval = active_cc / control_rate
        target_interval = active_cc / target_rate
        pause = max(0, target_interval - current_interval)

        massive_overshoot = control_rate >= target_rate * MAX_RATE_MASSIVE_OVERSHOOT
        pause_too_large = pause > MAX_RATE_MAX_CYCLE_PAUSE

        if not massive_overshoot and not pause_too_large:
            state.set_cycle_pause(pause)
            return

        state.set_cycle_pause(min(pause, MAX_RATE_MAX_CYCLE_PAUSE))

        if not cooldown_expired() or current_cc <= 1:
            return

        target_cc = max(1, math.floor(target_rate / per_worker_rate))
        if target_cc >= current_cc:
            return

        previous_cc = current_cc
        if not request_worker_target(target_cc, ramp_time):
            return

        record_worker_adjustment(ramp_time)
        logger.warning(
            "Reducing workers for max_rate: desired max_rate: %s, "
            "current_rate: %s, smoothed_rate: %.2f, current_cc: %s, "
            "target_cc: %s, pause: %.6f, cooldown_until: %.2f",
            target_rate,
            current_rate,
            control_rate,
            previous_cc,
            target_cc,
            pause,
            max_rate_state.next_worker_adjustment_time,
        )

    report_time = start_time + FREQUENCY + delay_stats

    # if no schedule was passed, create a schedule with just 1 line
    if schedule is None:
        schedule = [(concurrency, max_rate, ramp, duration)]

    current_cc = 0
    adjustment_thread: Thread | None = None
    start_control_server()

    try:
        for i, s in enumerate(schedule):
            cc, row_max_rate, ramp_time, dur = s
            cc = int(cc or 0)
            row_max_rate = int(row_max_rate or 0)
            ramp_time = int(ramp_time or 0)

            if dur and ramp_time > dur:
                ramp_time = dur

            logger.info(
                f"Starting schedule {i + 1}/{len(schedule)}: "
                f"{cc=}, max_rate={row_max_rate}, {ramp_time=}, {dur=}"
            )

            if adjustment_thread and adjustment_thread.is_alive():
                adjustment_thread.join()

            state.set_cycle_pause(0)
            max_rate_state.reset()

            # A max-rate row starts with either the requested connection count
            # or one worker. The reporting loop will extrapolate the target
            # connection count once it has a measured __cycle__ rate.
            if row_max_rate and cc == 0:
                worker_adjusted = request_worker_target(1, ramp_time)
            else:
                worker_adjusted = request_worker_target(cc, ramp_time)

            # When a schedule row provides both an explicit connection count and
            # max_rate, the initial ramp to that connection count can skew the
            # first stats windows. Treat that ramp as a worker adjustment and
            # delay the next worker-count change by cooldown + ramp_time.
            #
            # A max-rate row with connections empty/zero is different: the one
            # initial worker is only a probe used to measure per-worker
            # throughput. Do not start the 60s cooldown there, otherwise a
            # target-only row such as ",3000,0,60" would spend most of its run
            # waiting before it can extrapolate and add the workers it needs.
            if row_max_rate and cc > 0 and worker_adjusted:
                record_worker_adjustment(ramp_time)

            end_schedule_time = time.time() + dur if dur else float("inf")

            while time.time() < end_schedule_time and not state.stop_event.is_set():
                reap_workers()

                # The main loop only inspects shared counters under the lock.
                # The actual worker execution remains outside this lock.
                with state.lock:
                    task_done_threads = state.task_done_threads
                    worker_error = state.worker_error

                if worker_error:
                    logger.error(
                        f"error_type={worker_error.__class__.__name__}, "
                        f"{worker_error=}"
                    )
                    break

                if task_done_threads > 0 and task_done_threads >= next_worker_id:
                    logger.info("Requested iteration limit reached")
                    break

                if time.time() >= report_time:
                    report = publish_window(int(time.time() - delay_stats))
                    if row_max_rate and report:
                        apply_max_rate_control(row_max_rate, report, ramp_time)
                    report_time += FREQUENCY

                time.sleep(0.001)

            if state.worker_error or state.stop_event.is_set():
                break
    finally:
        graceful_shutdown()

    if state.worker_error:
        sys.exit(1)


def worker(
    state: RunState,
    worker_stop_event: Event,
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
        # Workers exit when either the whole run is stopping or this specific
        # worker was selected for scale-down by a schedule row.
        while not state.stop_event.is_set() and not worker_stop_event.is_set():
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

                    # The inner loop is the hot workload path. It checks stop
                    # Events at natural boundaries so shutdown is cooperative:
                    # the thread finishes the current transaction/cycle, flushes
                    # local stats, and returns.
                    while (
                        not state.stop_event.is_set()
                        and not worker_stop_event.is_set()
                    ):
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
                            if state.stop_event.is_set() or worker_stop_event.is_set():
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
                            # WorkerStats is thread-local, so measurement writes
                            # do not need a lock. Only the handoff into shared
                            # RunState.stats is locked inside add_stats().
                            state.add_stats(ws)
                            ws.new_window()
                            stat_time += FREQUENCY

                        cycle_pause = state.get_cycle_pause()
                        if cycle_pause:
                            sleep_until = time.time() + cycle_pause
                            while (
                                time.time() < sleep_until
                                and not state.stop_event.is_set()
                                and not worker_stop_event.is_set()
                            ):
                                time.sleep(min(0.05, sleep_until - time.time()))

            except Exception as e:
                if is_retryable_driver_error(driver, e):
                    if (
                        not state.stop_event.is_set()
                        and not worker_stop_event.is_set()
                    ):
                        log_and_sleep(e)
                    continue

                state.set_error(e)
                return
    finally:
        if not stopped:
            # Always flush the current thread-local stats window. This is the
            # equivalent of the old queue-drain step, but without multiprocessing
            # queues.
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
