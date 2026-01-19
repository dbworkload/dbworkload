#!/usr/bin/python

import errno
import logging
import multiprocessing as mp
import os
import queue
import random
import signal
import sys
import time
import traceback
from contextlib import contextmanager
from threading import Thread

import numpy as np
import tabulate
from psutil import cpu_percent, virtual_memory

import dbworkload.utils.common
from dbworkload.cli.dep import ConnInfo

# from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, Session
# from cassandra.policies import (
#     WhiteListRoundRobinPolicy,
#     DowngradingConsistencyRetryPolicy,
# )
# from cassandra.query import tuple_factory
# from cassandra.policies import ConsistencyLevel


DEFAULT_SLEEP = 3
MAX_RETRIES = 3
FREQUENCY = 10
STATS_BUFFER = 8

DBWORKLOAD_PIPE = "dbworkload.pipe"

logger = logging.getLogger("dbworkload")

sigterm_received = False

HEADERS: list = [
    "elapsed",
    "id",
    "threads",
    "tot_ops",
    "tot_ops/s",
    "period_ops",
    "period_ops/s",
    "mean(ms)",
    "p50(ms)",
    "p90(ms)",
    "p95(ms)",
    "p99(ms)",
    "max(ms)",
]

HEADERS_CSV: list = [
    "ts",
    "elapsed",
    "id",
    "threads",
    "tot_ops",
    "tot_ops_s",
    "period_ops",
    "period_ops_s",
    "mean_ms",
    "p50_ms",
    "p90_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "centroids",
]

FINAL_HEADERS: list = [
    "elapsed",
    "id",
    "threads",
    "tot_ops",
    "tot_ops/s",
    "mean(ms)",
    "p50(ms)",
    "p90(ms)",
    "p95(ms)",
    "p99(ms)",
    "max(ms)",
]


def signal_handler(sig, frame):
    """Handles Ctrl+C events gracefully,
    ensuring all running processes are closed rather than killed.

    Args:
        sig (_type_):
        frame (_type_):
    """
    logger.info("KeyboardInterrupt signal detected. Stopping processes...")
    global sigterm_received

    # if a keyboardinterrupt event was already receive, just exit.
    if sigterm_received:
        logger.warning("Forcibly quitting. You're rude!")
        sys.exit(1)

    sigterm_received = True


def cycle(iterable, backwards=False):
    global current_proc

    if not backwards:
        current_proc += 1
        return current_proc % iterable
    else:
        v = current_proc % iterable
        current_proc -= 1
        return v


# Launch or kill worker threads based on cc_change value.
# workers are added or removed evenly across all supervisors.
# If a ramp time is specified, threads creation or destruction
# will be paced accordingly.
def launch_or_kill_workers(
    queues: list,
    ramp_time: int,
    cc_change: int,
    proc_len: list,
    iterations_per_thread,
    concurrency,
):
    if cc_change == 0:
        return

    ramp_interval = ramp_time / abs(cc_change)
    global thread_id

    if cc_change > 0:
        for _ in range(cc_change):
            queues[cycle(proc_len)].put(
                (
                    thread_id,
                    iterations_per_thread,
                    concurrency,
                )
            )
            thread_id += 1
            time.sleep(ramp_interval)

    if cc_change < 0:
        for _ in range(abs(cc_change)):
            queues[cycle(proc_len, backwards=True)].put("kill_one")
            time.sleep(ramp_interval)


def run(
    concurrency: int,
    workload_path: str,
    prom_port: int,
    iterations: int,
    procs: int,
    ramp: int,
    conn_info: dict,
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
    def gracefully_shutdown():
        logger.debug("Gracefully shutting down...")

        end_time = int(time.time())
        _stats_received = stats_received

        # notify all Supervisors to quit
        for q in queues.values():
            q.put("poison_pill")

        # wait for supervisors to quit and drain
        # the to_main_q at the same time to avoid locking
        for x in supervisors.values():
            while x.is_alive():
                try:
                    msg = to_main_q.get(block=True, timeout=0.5)
                    if isinstance(msg, list):
                        _stats_received += 1
                        stats.add_tds(msg)
                except queue.Empty:
                    pass

            x.join()

        # Catch all for loose stats, if any?
        while True:
            try:
                msg = to_main_q.get(block=False)
                if isinstance(msg, list):
                    _stats_received += 1
                    stats.add_tds(msg)
            except queue.Empty:
                break

        cpu_util = cpu_percent()
        vmem = virtual_memory().percent
        if _stats_received != active_connections or cpu_util > 70 or vmem > 70:
            logger.warning(
                f"{_stats_received=}, expected={active_connections}. CPU Util={cpu_util}%, Memory={vmem}%"
            )

        # now that we have all stat reports, calculate the stats one last time.
        report = stats.calculate_stats(active_connections, end_time - delay_stats)
        centroids = stats.get_centroids()

        if save:
            with open(run_name + ".csv", "a") as f:
                for row in report:
                    f.write(str(stats.endtime) + ",")
                    for col in row:
                        f.write(str(col) + ",")
                    np.savetxt(f, next(centroids), newline=";")
                    f.write("\n")

        if not quiet:
            logger.info("Printing final stats")
            print_stats(report)

        prom.publish(report)

        logger.info("Printing summary for the full test run")

        # the final stat report summarizes the entire test run
        final_stats_report = tabulate.tabulate(
            stats.calculate_final_stats(active_connections, stats.endtime),
            FINAL_HEADERS,
            tablefmt="simple_outline",
            intfmt=",",
            floatfmt=",.2f",
        )

        # Print test run details
        runtime_params = tabulate.tabulate(
            [
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
                        "\n",
                        "\n",
                        final_stats_report,
                        "\n",
                        "\n",
                        runtime_params,
                        "\n",
                    ]
                )

        print(
            "\n",
            runtime_details,
            "\n",
            "\n",
            final_stats_report,
            "\n",
            "\n",
            runtime_params,
            "\n",
            sep="",
        )

        if os.path.exists(DBWORKLOAD_PIPE):
            os.remove(DBWORKLOAD_PIPE)

        sys.exit(0)

    logger.setLevel(log_level)

    start_time = int(time.time())
    workload = dbworkload.utils.common.import_class_at_runtime(workload_path)

    run_name = (
        workload.__name__
        + "."
        + time.strftime("%Y%m%d_%H%M%S", time.gmtime(start_time))
    )

    logger.info(f"Starting workload {run_name}")

    # the offset registers at what second we want all threads
    # to send the stat report, so they all send it at the same time
    offset = start_time % FREQUENCY

    # open a new csv file and just write the header columns
    if save:
        with open(run_name + ".csv", "w") as f:
            f.write(",".join(HEADERS_CSV) + "\n")

    # register Ctrl+C handler
    signal.signal(signal.SIGINT, signal_handler)

    stats = dbworkload.utils.common.Stats(start_time)

    prom = dbworkload.utils.common.Prom(prom_port, stats, histogram_bins)

    to_main_q = mp.Queue()

    global queues
    global supervisors
    supervisors = {}
    queues = {}

    # start a separate thread for messages coming in via the pipe
    # echo 5 > dbworkload.pipe # create 5 more connections
    Thread(
        target=listen_to_pipe,
        daemon=True,
        args=(
            queues,
            0,
            procs,
            None,
            concurrency,
        ),
    ).start()

    # launch supervisors in a dedicated OS process
    for x in range(procs):
        queues[x] = mp.Queue()
        supervisors[x] = mp.Process(
            target=supervisor,
            args=(
                to_main_q,
                queues[x],
                log_level,
                conn_info,
                driver,
                workload,
                args,
                conn_duration,
                offset,
                x,
            ),
            daemon=True,
        )
        supervisors[x].start()

    # report time happens STATS_BUFFER seconds after the stats are received.
    # we add this buffer to make sure we get all the stats reports
    # from each thread before we aggregate and display
    report_time = start_time + FREQUENCY + STATS_BUFFER + delay_stats

    returned_procs = 0
    active_connections = 0
    stats_received = 0

    global current_proc
    global thread_id

    current_proc = -1
    current_cc = 0
    thread_id = 0
    pause_for_ramp_time = 0

    iterations_per_thread = None
    if iterations:
        # ensure we don't create more threads than the total number of iterations requested.
        # eg. we don't need 8 threads if iterations is 4: we only need 4 threads
        concurrency = min(iterations, concurrency)
        iterations_per_thread = iterations // concurrency

        if iterations % concurrency > 0:
            logger.warning(
                f"You have requested {iterations} iterations on {concurrency} threads. {iterations} modulo {concurrency} = {iterations%concurrency} iterations will not be executed."
            )

    # if no schedule was passed, create a schedule with just 1 line
    if schedule is None:
        schedule = [(concurrency, max_rate, ramp, duration)]

    # loop through all lines in the schedule
    for i, s in enumerate(schedule):
        cc, max_rate, ramp_time, dur = s

        # sanitize
        if dur and ramp_time > dur:
            ramp_time = dur

        logger.info(
            f"Starting schedule {i+1}/{len(schedule)}: {cc=}, {max_rate=}, {ramp_time=}, {dur=}"
        )

        # always make sure that a duration is specified, even if none was passed
        # in which case it defaults to infinite
        end_schedule_time = time.time() + dur if dur else float("inf")

        # if max_rate was set instead of concurrency
        # and current_cc = 0,
        # start the workload with 1 thread so that dbworkload
        # has stats to measure on for adding/removing threads
        # as part of the calculations for maintaining
        # the desired max_rate
        if current_cc == 0 and max_rate:
            Thread(
                target=launch_or_kill_workers,
                daemon=True,
                args=(
                    queues,
                    ramp_time,
                    1,
                    procs,
                    iterations_per_thread,
                    concurrency,
                ),
            ).start()

            current_cc = 1

        if not max_rate:
            Thread(
                target=launch_or_kill_workers,
                daemon=True,
                args=(
                    queues,
                    ramp_time,
                    cc - current_cc,
                    procs,
                    iterations_per_thread,
                    concurrency,
                ),
            ).start()

            current_cc = cc

        task_done_threads = 0

        # loop for the entire duration of the schedule's current line
        while time.time() < end_schedule_time:
            try:
                # read from the queue for stats or completion messages
                msg = to_main_q.get(block=False)
                # a stats report is a list obj
                if isinstance(msg, list):
                    stats_received += 1
                    stats.add_tds(msg)
                elif msg == "init":
                    active_connections += 1
                elif msg == "got_killed":
                    active_connections -= 1
                elif msg == "task_done":
                    task_done_threads += 1
                elif isinstance(msg, Exception):
                    logger.error(f"error_type={msg.__class__.__name__}, {msg=}")
                    gracefully_shutdown()
                else:
                    logger.error(f"unrecognized message: {msg}")
                    gracefully_shutdown()

            except queue.Empty:
                pass

            if sigterm_received:
                gracefully_shutdown()

            if task_done_threads > 0 and task_done_threads >= active_connections:
                logger.info("Requested iteration/duration limit reached")
                gracefully_shutdown()

            if time.time() >= report_time:
                cpu_util = cpu_percent()
                vmem = virtual_memory().percent
                if stats_received != active_connections or cpu_util > 70 or vmem > 70:
                    logger.warning(
                        f"{stats_received=}, expected={active_connections}. CPU Util={cpu_util}%, Memory={vmem}%"
                    )

                # remove the STATS_BUFFER seconds added
                endtime = int(time.time() - delay_stats) - STATS_BUFFER

                report = stats.calculate_stats(active_connections, endtime)

                # if max_rate is specified, try to stick to it.
                # to calculate how to get to the max rate, we need a non-empty report
                if max_rate and report:
                    current_rate = report[0][6]  # __cycle__ period_ops/s

                    # approximate how many threads are needed to get
                    # to the desired max_rate given the current QPS rate
                    # and current threads count
                    extrapolated_cc = int(max_rate / (current_rate / current_cc))

                    # adjust the thread count if there is a difference
                    # between the current thread count and the calculated
                    # thread count, but not if there is one such operation already
                    # running, that is, not if there's an operation that is slow due
                    # to a long ramp_time.
                    if (
                        extrapolated_cc - current_cc
                        and time.time() >= pause_for_ramp_time
                    ):
                        Thread(
                            target=launch_or_kill_workers,
                            daemon=True,
                            args=(
                                queues,
                                ramp_time,
                                extrapolated_cc - current_cc,
                                procs,
                                iterations_per_thread,
                                concurrency,
                            ),
                        ).start()

                        # make sure we will not add/remove threads while the newly
                        # created thread is still working
                        pause_for_ramp_time = time.time() + ramp_time + 2 * FREQUENCY

                        logger.warning(
                            f"Calculating max_rate: desired max_rate: {max_rate}, "
                            f"current_rate: {report[0][6]}, current_cc = {current_cc}, "
                            f"extrapolated_cc = {extrapolated_cc}, "
                            f"difference: {extrapolated_cc-current_cc}"
                        )
                        current_cc = extrapolated_cc

                        # ramp_time is only considered for reaching the desired max_rate.
                        # For adjustments over time, we want the changes to happen immediately
                        # and not smoothed out over the initial ramp_time value
                        ramp_time = 0

                centroids = stats.get_centroids()

                stats.new_window(endtime)
                stats_received = 0

                if save:
                    with open(run_name + ".csv", "a") as f:
                        for row in report:
                            f.write(str(stats.endtime) + ",")
                            for col in row:
                                f.write(str(col) + ",")
                            np.savetxt(f, next(centroids), newline=";")
                            f.write("\n")

                if not quiet:
                    print_stats(report)

                prom.publish(report)

                report_time += FREQUENCY

            # pause briefly to prevent the loop from overheating the CPU
            time.sleep(0.001)

    gracefully_shutdown()


# a supervisor runs in a separate process.
# The idea is to create as many supervisors as vCPUs.
# The sole role of the supervisor is to listen for instructions
# from the MainProcess.
# Instructions are:
#   - Create a new worker.
#   - Destroy a worker.
#   - Destroy all workers and return.
def supervisor(
    to_main_q: mp.Queue,
    from_main_q: mp.Queue,
    log_level: str,
    conn_info: ConnInfo,
    driver: str,
    workload: object,
    args: dict,
    conn_duration: int,
    offset: int,
    id: int,
):
    logger.setLevel(log_level)
    logger.debug(f"Supervisor-{id} started")

    threads: list[Thread] = []
    from_proc_q = mp.Queue()

    # capture KeyboardInterrupt and do nothing
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    while True:
        msg = from_main_q.get(block=True)

        if msg == "poison_pill":
            logger.debug(f"Supervisor-{id} terminating...")

            # wait for Threads to return before
            # letting the Supervisor MainThread return
            for x in threads:
                if x.is_alive():
                    from_proc_q.put("poison_pill")

            for x in threads:
                if x.is_alive():
                    x.join()

            logger.debug(f"Supervisor-{id} terminated")
            return

        elif msg == "kill_one":
            from_proc_q.put("poison_pill")

        elif isinstance(msg, tuple):
            t = Thread(
                target=worker,
                daemon=True,
                args=(
                    to_main_q,
                    from_proc_q,
                    log_level,
                    conn_info,
                    driver,
                    workload,
                    args,
                    conn_duration,
                    offset,
                    *msg,
                ),
            )
            t.start()
            threads.append(t)


def worker(
    to_main_q: mp.Queue,
    from_proc_q: mp.Queue,
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
    def gracefully_return(msg):
        # send final stats
        to_main_q.put(ws.get_tdigest_ndarray(), block=False)

        # send notification to MainThread
        to_main_q.put(msg)

        logger.debug(f"Thread ID {id} returned")

    logger.setLevel(log_level)

    logger.debug(f"Thread ID {id} started")

    # catch exception while instantiating the workload class
    try:
        w = workload(args)
    except Exception as e:
        stack_lines = traceback.format_exc()
        to_main_q.put(Exception(stack_lines))
        return

    c = 0

    conn_endtime = 0

    ws = dbworkload.utils.common.WorkerStats()

    run_init = True

    # send notification that a new thread has started
    to_main_q.put("init")

    while True:
        # listen for termination messages (poison pill)
        try:
            from_proc_q.get(block=False)
            logger.debug("Poison pill received, terminating...")
            gracefully_return("got_killed")
            return
        except queue.Empty:
            pass

        if conn_duration:
            # reconnect every conn_duration +/- 20%
            conn_endtime = time.time() + int(conn_duration * random.uniform(0.8, 1.2))

        try:
            logger.debug(f"driver: {driver}, params: {conn_info.params}")
            # with Cluster().connect('bank') as conn:
            with get_connection(driver, conn_info) as conn:
                logger.debug("Connection started")

                # execute setup() only once per thread
                if run_init:
                    run_init = False

                    if hasattr(w, "setup") and callable(w.setup):
                        run_transaction(
                            conn,
                            lambda conn: w.setup(
                                conn,
                                id,
                                concurrency,
                            ),
                            driver,
                            max_retries=MAX_RETRIES,
                        )

                # send stats
                ts = int(time.time())
                stat_time = ts + FREQUENCY - ts % FREQUENCY + offset

                while True:
                    # listen for termination messages (poison pill)
                    try:
                        from_proc_q.get(block=False)
                        logger.debug("Poison pill received, terminating...")
                        gracefully_return("got_killed")
                        return
                    except queue.Empty:
                        pass

                    # return if the iteration count has been reached
                    if iterations and c >= iterations:
                        logger.debug("Task completed!")
                        gracefully_return("task_done")
                        return

                    # break from the inner loop if limit for connection duration has been reached
                    # this will cause for the outer loop to reset the timer and restart with a new conn
                    if conn_duration and time.time() >= conn_endtime:
                        logger.debug(
                            "conn_duration reached, will reset the connection."
                        )
                        break

                    cycle_start = time.time()
                    for txn in w.loop():
                        start = time.time()
                        retries = run_transaction(
                            conn,
                            lambda conn: txn(conn),
                            driver,
                            max_retries=MAX_RETRIES,
                        )

                        # record how many retries there were, if any
                        for _ in range(retries):
                            ws.add_latency_measurement("__retries__", 0)

                        # if retries matches max_retries, then it's a total failure and we don't record the txn time
                        if retries < MAX_RETRIES:
                            ws.add_latency_measurement(
                                txn.__name__, time.time() - start
                            )

                    c += 1

                    ws.add_latency_measurement("__cycle__", time.time() - cycle_start)

                    if to_main_q.full():
                        logger.error("=========== Q FULL!!!! ======================")
                    if time.time() >= stat_time:
                        to_main_q.put(ws.get_tdigest_ndarray(), block=False)
                        ws.new_window()
                        stat_time += FREQUENCY

        except Exception as e:
            if driver == "postgres":
                import psycopg

                if isinstance(e, psycopg.errors.UndefinedTable):
                    to_main_q.put(e)
                    return
                log_and_sleep(e)

            elif driver == "mysql":
                import mysql.connector.errorcode

                if e.errno == mysql.connector.errorcode.ER_NO_SUCH_TABLE:
                    to_main_q.put(e)
                    return
                log_and_sleep(e)

            elif driver == "maria":
                if str(e).endswith(" doesn't exist"):
                    to_main_q.put(e)
                    return
                log_and_sleep(e)

            elif driver == "oracle":
                if str(e).startswith("ORA-00942: table or view does not exist"):
                    to_main_q.put(e)
                    return
                log_and_sleep(e)

            elif driver == "pinecone":
                from pinecone.exceptions import PineconeException
                
                if isinstance(e, PineconeException):
                    status = getattr(e, "status", None)

                    if status in (400, 401, 403, 404):
                        # fatal: bad config, bad auth, missing index
                        to_main_q.put(e)
                        return

                    # retryable (service errors, transient failures)
                    log_and_sleep(e)


            else:
                # for all other Exceptions, report and return
                logger.error(type(e), stack_info=True)
                to_main_q.put(e)
                return


def listen_to_pipe(queues, ramp_time, procs, iterations_per_thread, concurrency):
    # https://stackoverflow.com/questions/39089776/python-read-named-pipe

    try:
        os.mkfifo(DBWORKLOAD_PIPE)
    except OSError as oe:
        if oe.errno != errno.EEXIST:
            raise

    while True:
        with open(DBWORKLOAD_PIPE) as fifo:
            for line in fifo:
                try:
                    t = int(line)
                except:
                    continue

                logger.info(f"{'Adding' if t > 0 else 'Removing' } {abs(t)} threads.")
                Thread(
                    target=launch_or_kill_workers,
                    daemon=True,
                    args=(
                        queues,
                        ramp_time,
                        t,
                        procs,
                        iterations_per_thread,
                        concurrency,
                    ),
                ).start()


def log_and_sleep(e: Exception):
    logger.error(f"error_type={e.__class__.__name__}, msg={e}")
    logger.info("Sleeping for %s seconds" % (DEFAULT_SLEEP))
    time.sleep(DEFAULT_SLEEP)


def print_stats(report: list):
    print(
        tabulate.tabulate(
            report,
            HEADERS,
            intfmt=",",
            floatfmt=",.2f",
        ),
        "\n",
    )


def run_transaction(conn, op, driver: str, max_retries=3):
    """
    Execute the operation *op(conn)* retrying serialization failure.

    If the database returns an error asking to retry the transaction, retry it
    *max_retries* times before giving up (and propagate it).
    """
    for retry in range(1, max_retries + 1):
        try:
            op(conn)
            # If we reach this point, we were able to commit, so we break
            # from the retry loop.
            return retry - 1
        except Exception as e:
            if driver == "postgres":
                import psycopg.errors

                if isinstance(e, psycopg.errors.SerializationFailure):
                    # This is a retry error, so we roll back the current
                    # transaction and sleep for a bit before retrying. The
                    # sleep time increases for each failed transaction.
                    logger.debug(f"SerializationFailure:: {e}")
                    conn.rollback()
                    time.sleep((2**retry) * 0.1 * (random.random() + 0.5))
                else:
                    raise e
            else:
                raise e
    logger.debug(f"Transaction did not succeed after {max_retries} retries")
    return retry


@contextmanager
def get_connection_with_context(driver: str, conn_info: ConnInfo):
    if driver == "spanner":
        from google.cloud import spanner

        try:
            yield spanner.Client().instance(conn_info.params["instance"]).database(
                conn_info.params["database"]
            )
        except Exception as e:
            logger.error(e)
        finally:
            pass


def get_connection(driver: str, conn_info: ConnInfo):
    if driver == "postgres":
        import psycopg

        return psycopg.connect(**conn_info.params, connect_timeout=5)
    elif driver == "mysql":
        import mysql.connector

        return mysql.connector.connect(**conn_info.params)
    elif driver == "maria":
        import mariadb

        return mariadb.connect(**conn_info.params)
    elif driver == "oracle":
        import oracledb

        conn = oracledb.connect(**conn_info.params)
        conn.autocommit = conn_info.extras.get("autocommit", False)
        return conn
    # elif driver == "sqlserver":
    #     return
    elif driver == "mongo":
        import pymongo

        return pymongo.MongoClient(**conn_info)

    elif driver == "pinecone":
        from pinecone import Pinecone
        
        pc = Pinecone(api_key=conn_info.params["api_key"])
        return pc.Index(conn_info.params["index_name"])

    else:
        return get_connection_with_context(driver, conn_info)

    # elif driver == "cassandra":
    #     profile = ExecutionProfile(
    #         load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
    #         retry_policy=DowngradingConsistencyRetryPolicy(),
    #         consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    #         serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    #         request_timeout=15,
    #         row_factory=tuple_factory,
    #     )
    #     cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    #     # session = cluster.connect()
    #     return cluster.connect()
