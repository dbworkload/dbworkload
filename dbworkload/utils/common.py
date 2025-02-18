#!/usr/bin/python

import importlib
import logging
import numpy as np
import os
import random
import sys
import time
import urllib.parse
import yaml
import prometheus_client as prom
from pytdigest import TDigest

RESERVED_WORDS = [
    "unique",
    "inverted",
    "index",
    "constraint",
    "family",
    "like",
    "primary",
    "foreign",
    "key",
    "create",
    "table",
    "if",
    "not",
    "exists",
    "null",
    "global",
    "local",
    "temporary",
    "temp",
    "unlogged",
    "visible",
    "using",
    "hash" "with",
    "bucket_count",
]

DEFAULT_ARRAY_COUNT = 3
NOT_NULL_MIN = 20
NOT_NULL_MAX = 40

logger = logging.getLogger("dbworkload")


class Prom:
    def __init__(self, prom_port: int = 26260):
        self.prom_latency: dict[str, list[prom.Gauge]] = {}

        # don't stop just because prom server can't start
        try:
            prom.start_http_server(prom_port)
        except OSError as e:
            logger.warning(f"Cannot start prometheus server: {e}")

        self.threads = prom.Gauge(
            "threads", "count of connection threads to the database."
        )

    def publish(self, report: list):
        for row in report:
            id = row[1]

            if id not in self.prom_latency:
                self.prom_latency[id] = []
                self.prom_latency[id].append(
                    prom.Gauge(f"{id}__tot_ops", "total count of ops")
                )
                self.prom_latency[id].append(
                    prom.Gauge(
                        f"{id}__tot_ops_s", "derived value from tot_ops / elapsed"
                    )
                )
                self.prom_latency[id].append(
                    prom.Gauge(f"{id}__period_ops", "ops count for the recent window")
                )
                self.prom_latency[id].append(
                    prom.Gauge(
                        f"{id}__period_ops_s",
                        "derived value from period_ops / window duration",
                    )
                )
                self.prom_latency[id].append(prom.Gauge(f"{id}__mean_ms", "mean_ms"))
                self.prom_latency[id].append(prom.Gauge(f"{id}__p50_ms", "p50_ms"))
                self.prom_latency[id].append(prom.Gauge(f"{id}__p90_ms", "p90_ms"))
                self.prom_latency[id].append(prom.Gauge(f"{id}__p95_ms", "p95_ms"))
                self.prom_latency[id].append(prom.Gauge(f"{id}__p99_ms", "p99_ms"))
                self.prom_latency[id].append(prom.Gauge(f"{id}__max_ms", "max_ms"))

            for idx, v in enumerate(row[3:]):
                self.prom_latency[id][idx].set(v)

        # threads value is the same for all rows
        if report:
            self.threads.set(report[0][2])


class Stats:
    """Print workload stats
    and export the stats as Prometheus endpoints
    """

    def __init__(self, ts: int):
        self.cumulative_counts: dict[str, TDigest] = {}
        self.instantiation_time = ts

        self.quantiles = [0.50, 0.90, 0.95, 0.99, 1.0]

        self.new_window(ts)

    # reset stats while keeping cumulative counts
    def new_window(self, start_time) -> None:
        self.window_start_time: int = start_time
        self.window_stats: dict[str, list] = {}

        # hold the ndarray of the combined tdigest
        self.window_stats_centroids: dict[str, np.ndarray] = {}

    def add_tds(self, l: list):
        for x in l:
            self.cumulative_counts.setdefault(x[0], TDigest())
            self.window_stats.setdefault(x[0], [])
            self.window_stats[x[0]].append(
                TDigest(compression=1000).of_centroids(x[1], compression=1000)
            )

    # calculate the current stats this instance has collected.
    def calculate_stats(self, active_connections: int, endtime: int) -> list:
        self.endtime = endtime

        # cover the case where elapsed is zero and runs into ZeroDivisionError
        elapsed = (
            endtime - self.instantiation_time
            if endtime - self.instantiation_time
            else 1
        )

        window_elapsed = (
            endtime - self.window_start_time if endtime - self.window_start_time else 1
        )

        def get_stats_row(id: str):
            td = TDigest(compression=1000).combine(self.window_stats[id])

            self.window_stats_centroids[id] = td.get_centroids()

            self.cumulative_counts[id] = TDigest(compression=1000).combine(
                self.cumulative_counts[id], td
            )
            return [
                elapsed,
                id,
                active_connections,
                int(self.cumulative_counts[id].weight),
                int(self.cumulative_counts[id].weight // elapsed),
                int(td.weight),
                int(td.weight // window_elapsed),
                round(td.mean * 1000, 2),
            ] + [round(x * 1000, 2) for x in td.inverse_cdf(self.quantiles)]

        return [get_stats_row(id) for id in sorted(list(self.window_stats.keys()))]

    def calculate_final_stats(self, active_connections: int, endtime: int) -> list:
        def get_stats_row(id: str):
            # cover the case where elapsed is zero and runs into ZeroDivisionError
            elapsed = (
                endtime - self.instantiation_time
                if endtime - self.instantiation_time
                else 1
            )
            return [
                elapsed,
                id,
                active_connections,
                int(self.cumulative_counts[id].weight),
                int(self.cumulative_counts[id].weight // elapsed),
                round(self.cumulative_counts[id].mean * 1000, 2),
            ] + [
                round(x * 1000, 2)
                for x in self.cumulative_counts[id].inverse_cdf(self.quantiles)
            ]

        return [get_stats_row(id) for id in sorted(list(self.window_stats.keys()))]

    def get_centroids(self):
        return iter(
            [
                self.window_stats_centroids[x]
                for x in sorted(list(self.window_stats_centroids.keys()))
            ]
        )


class WorkerStats:
    def __init__(self):
        self.quantiles = [0.50, 0.90, 0.95, 0.99, 1.0]

        self.new_window()

    # reset stats while keeping cumulative counts
    def new_window(self) -> None:
        self.window_start_time: float = time.time()
        self.window_stats: dict[str, list] = {}

    # add one latency measurement in seconds
    def add_latency_measurement(self, id: str, measurement: float) -> None:
        self.window_stats.setdefault(id, []).append(measurement)

    def get_tdigest_ndarray(self):
        return [
            (id, TDigest.compute(np.array(l), compression=1000).get_centroids())
            for id, l in self.window_stats.items()
        ]


def get_driver_from_scheme(scheme: str):
    return {
        "postgres": "postgres",
        "postgresql": "postgres",
        "mongo": "mongo",
        "mongodb": "mongo",
        "maria": "maria",
        "mariadb": "maria",
        "mysql": "mysql",
        "mysqldb": "mysql",
        "oracle": "oracle",
        "cassandra": "cassandra",
        "sqlserver": "sqlserver",
        "spanner": "spanner",
    }.get(scheme, None)


def set_query_parameter(url: str, param_name: str, param_value: str):
    """convenience function to add a query parameter string such as '&application_name=myapp' to a url

    Args:
        url (str]): The URL string
        param_name (str): the parameter to add
        param_value (str): the value of the parameter

    Returns:
        str: the new URL with the added parameter
    """
    scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(query_string)
    query_params[param_name] = [param_value]
    new_query_string = urllib.parse.urlencode(query_params, doseq=True)
    return urllib.parse.urlunsplit((scheme, netloc, path, new_query_string, fragment))


def import_class_at_runtime(path: str):
    """Imports a class with the same name of the module capitalized.
    Example: 'workloads/bank.py' returns class 'Bank' in module 'bank'

    Args:
        path (string): the path of the module to import

    Returns:
        class: the imported class
    """

    # load the module at runtime
    sys.path.append(os.path.dirname(path))
    module_name = os.path.splitext(os.path.basename(path))[0]

    try:
        module = importlib.import_module(module_name)
        return getattr(module, module_name.capitalize())
    except AttributeError as e:
        logger.error(e)
        sys.exit(1)
    except ImportError as e:
        logger.error(e)
        sys.exit(1)


def get_based_name_dir(filepath: str):
    """Return the directory name based on the filename

    Args:
        filepath (str): the filepath, eg: /path/to/myfile.txt

    Returns:
        str: the name of the directory, eg: /path/to/file
    """
    return os.path.join(
        os.path.dirname(filepath),
        os.path.splitext(os.path.basename(filepath))[0].lower(),
    )


def get_workload_load(workload_path: str):
    """Get the data generation YAML string, as a Python dict object

    Args:
        workload_path (str): the workload class filepath

    Returns:
        (dict): the data gen definition
    """
    # find if the .yaml file exists
    yaml_file = os.path.abspath(get_based_name_dir(workload_path) + ".yaml")

    if os.path.exists(yaml_file):
        logger.debug("Found data generation definition YAML file %s" % yaml_file)
        with open(yaml_file, "r") as f:
            return yaml.safe_load(f)
    else:
        logger.debug(
            f"YAML file {yaml_file} not found. Loading data generation definition from the 'load' variable"
        )
        try:
            workload = import_class_at_runtime(workload_path)
            return yaml.safe_load(workload({}).load)
        except AttributeError as e:
            logger.warning(f"{e}. Make sure self.load is a valid variable in __init__")
            return {}


def get_new_dburl(dburl: str, db_name: str):
    """Return the dburl with the database name replaced.

    Args:
        dburl (str): the database connection string
        db_name (str): the new database name

    Returns:
        str: the new connection string
    """
    # craft the new dburl
    scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(dburl)
    path = "/" + db_name
    return urllib.parse.urlunsplit((scheme, netloc, path, query_string, fragment))


def ddl_to_yaml(ddl: str):
    """Transform a SQL DDL string of (multiple) CREATE TABLE statements
    into a data generation definition YAML string

    Args:
        ddl (str): CREATE TABLE statements

    Returns:
        (str): the YAML data gen definition string
    """

    def get_type_and_args(col_type_and_args: list):
        is_not_null = False
        if "not" in col_type_and_args and "null" in col_type_and_args:
            is_not_null = True
        # check if it is an array
        # string array
        # string []
        # string[]
        is_array = False
        col_type_and_args = [x.lower() for x in col_type_and_args]
        if (
            "[]" in col_type_and_args[0]
            or "array" in col_type_and_args
            or "[]" in col_type_and_args
        ):
            is_array = True

        datatype: str = col_type_and_args[0].replace("[]", "")
        arg = None
        if len(col_type_and_args) > 1:
            arg = col_type_and_args[1:]

        if datatype.lower() in ["bool", "boolean"]:
            return {
                "type": "bool",
                "args": {
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in [
            "int2",
            "smallint",
            "int4",
            "int8",
            "int64",
            "bigint",
            "int",
            "integer",
        ]:
            if datatype.lower() in ["int2", "smallint"]:
                int_min = -(2**15) + 1
                int_max = 2**15 - 1
            elif datatype.lower() == "int4":
                int_min = -(2**31) + 1
                int_max = 2**31 - 1
            else:
                int_min = -(2**63) + 1
                int_max = 2**63 - 1

            return {
                "type": "integer",
                "args": {
                    "min": int_min,
                    "max": int_max,
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in [
            "string",
            "char",
            "character",
            "varchar",
            "text",
            "clob",
        ]:
            _min = 10
            _max = 30
            if arg and arg[0].isdigit():
                _min = int(arg[0]) // 3 + 1
                _max = int(arg[0])

            return {
                "type": "string",
                "args": {
                    "min": _min,
                    "max": _max,
                    "prefix": "",
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in [
            "decimal",
            "float",
            "float4",
            "float8",
            "dec",
            "numeric",
            "real",
            "double",
        ]:
            _min = 0
            _max = 10000000
            _round = 2
            if arg:
                if ":" in arg[0]:
                    prec, scale = arg[0].split(":")
                    if prec:
                        _max = 10 ** (int(prec) - int(scale))
                    if scale:
                        _round = int(scale)
                elif arg[0].isdigit():
                    _max = 10 ** int(arg[0])
                    _round = 0

            return {
                "type": "float",
                "args": {
                    "min": _min,
                    "max": _max,
                    "round": _round,
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["time", "timetz"]:
            return {
                "type": "time",
                "args": {
                    "start": "07:30:00",
                    "end": "15:30:00",
                    "micros": False,
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["json", "jsonb"]:
            return {
                "type": "json",
                "args": {
                    "min": 10,
                    "max": 50,
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                },
            }

        elif datatype.lower() == "date":
            return {
                "type": "date",
                "args": {
                    "start": "2000-01-01",
                    "end": "2024-12-31",
                    "format": "%Y-%m-%d",
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["timestamp", "timestamptz"]:
            return {
                "type": "timestamp",
                "args": {
                    "start": "2000-01-01",
                    "end": "2024-12-31",
                    "format": "%Y-%m-%d %H:%M:%S.%f",
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() == "uuid":
            return {
                "type": "uuid",
                "args": {
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["bit", "varbit"]:
            _size = 1
            if arg and arg[0].isdigit():
                _size = int(arg[0])

            return {
                "type": "bit",
                "args": {
                    "size": _size,
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        elif datatype.lower() in ["bytes", "blob", "bytea"]:
            return {
                "type": "bytes",
                "args": {
                    "size": 20,
                    "seed": random.randint(0, 100),
                    "null_pct": (
                        0.0
                        if is_not_null
                        else round(random.randint(NOT_NULL_MIN, NOT_NULL_MAX) / 100, 2)
                    ),
                    "array": DEFAULT_ARRAY_COUNT if is_array else 0,
                },
            }

        else:
            logger.error(
                f"Data type not implemented: '{datatype}'. Consider changing to another datatype or raise a GitHub issue."
            )
            sys.exit(1)

    def get_table_name_and_table_list(
        create_table_stmt: str, sort_by: list, count: int = 1000000
    ):
        # find CREATE TABLE opening parenthesis
        p1 = create_table_stmt.find("(")

        # find CREATE TABLE closing parenthesis
        within_brackets = 0
        for i, c in enumerate(create_table_stmt[p1:]):
            if c == "(":
                within_brackets += 1
            elif c == ")":
                within_brackets -= 1
            if within_brackets == 0:
                break

        # extract column definitions (within parentheses part)
        # eg: id uuid primary key, s string(30)
        col_def_str = create_table_stmt[p1 + 1 : p1 + i].strip()

        # find table name (before parenthesis part)
        for i in create_table_stmt[:p1].split():
            if i.lower() not in RESERVED_WORDS:
                table_name = i.replace(".", "__")
                break

        # process the content within parenthesis in the
        # CREATE TABLE stmt char by char to distinguish
        # the comma for separating columns vs the comma to separate
        # decimal precision
        within_brackets = 0
        col_def = ""
        for i in col_def_str:
            if i == "(":
                within_brackets += 1
                col_def += " "
                continue
            if i == ")":
                within_brackets -= 1
                col_def += " "
                continue
            if within_brackets == 0 or i.isdigit():
                col_def += i
            elif within_brackets > 0 and i == ",":
                col_def += ":"

        # process the content within parenthesis in the
        # CREATE TABLE stmt char by char to distinguish
        # the comma for separating columns vs the comma
        # included in single quote strings such as those in DEFAULT
        # eg: mycol STRING NULL DEFAULT 'corporate, inc'
        within_quote = False
        col_def_str = col_def
        col_def = ""
        for i in col_def_str:
            if i == "'":
                within_quote = not within_quote
                continue
            if within_quote:
                continue
            else:
                col_def += i

        col_def = [x.strip().lower() for x in col_def.split(",")]

        ll = []
        for x in col_def:
            # break it down to tokens
            col_name_and_type = x.strip().split(" ")
            col_name_and_type = [x for x in col_name_and_type if x]
            # remove those lines that are not column definition,
            # like CONSTRAINT, INDEX, FAMILY, etc..
            if col_name_and_type[0].lower() not in RESERVED_WORDS:
                ll.append(col_name_and_type)

        table_list = []
        table_list.append({"count": count})
        table_list[0]["sort-by"] = sort_by
        table_list[0]["columns"] = {}

        for x in ll:
            table_list[0]["columns"][x[0]] = get_type_and_args(x[1:])

        return table_name, table_list

    def get_create_table_stmts(ddl: str):
        """Parses a DDL SQL file and returns only the CREATE TABLE stmts

        Args:
            ddl (str): the raw DDL string

        Returns:
            list: the list of CREATE TABLE stmts
        """
        # remove all the multiline comments
        # delimited by /* and */
        while True:
            i = ddl.find("/*")
            j = ddl.find("*/")

            if i < 0:
                break
            ddl = ddl[:i] + ddl[j + 2 :]

        # given the whole ddl string,
        # line by line, remove empty lines and
        # all the comment lines
        # and return a list of lines, stripped of any whitespace
        stmts = []

        for s in ddl.split("\n"):
            s = s.strip()

            i = s.find("--")
            if i >= 0:
                s = s[:i]

            if s:
                stmts.append(s)

        # rejoin the lines into a new, clean ddl string
        clean_ddl = " ".join(stmts)

        # split the clean string by semicolon to get a list
        # of SQL statements
        stmts = [s.strip() for s in clean_ddl.split(";")]

        # keep only strings that start with 'create' and
        # have word 'table' between beginning and the first open parenthesis
        return [
            s
            for s in stmts
            if s.lower().startswith("create") and "table" in s[: s.find("(")].lower()
        ]

    stmts = get_create_table_stmts(ddl)

    d = {}
    for stmt in stmts:
        table_name, table_list = get_table_name_and_table_list(
            stmt, count=100, sort_by=[]
        )
        d[table_name] = table_list

    return yaml.dump(d, default_flow_style=False, sort_keys=False)


def get_threads_per_proc(procs: int, threads: int):
    """Returns a list of threads count per procs

    Args:
        procs (int): procs count
        threads (int): threads count

    Returns:
        list: list of threads per procs
    """

    c = int(threads / procs)
    m = threads % procs

    l = [c for _ in range(min(procs, threads))]

    for x in range(m):
        l[x] += 1

    l.sort()

    return l


def get_import_stmts(
    csv_files: list,
    table_name: str,
    http_server_hostname: str = "myhost",
    http_server_port: str = "3000",
    delimiter: str = "",
    nullif: str = "",
):
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    chunk_gen = chunks(csv_files, 20)
    stmts = []

    if delimiter == "\t":
        delimiter_option = "e'\\t', "
    else:
        delimiter_option = f"'{delimiter}', "

    prefix = f"IMPORT INTO {table_name} CSV DATA ("
    mid = ") WITH delimiter = "
    suffix = f"nullif = '{nullif}';"

    for chunk in chunk_gen:
        csv_data = ""

        for x in chunk:
            csv_data += f"'http://{http_server_hostname}:{http_server_port}/{x}', "

        stmts.append(prefix + csv_data[:-2] + mid + delimiter_option + suffix)

    return stmts
