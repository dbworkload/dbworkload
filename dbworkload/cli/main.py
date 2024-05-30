#!/usr/bin/python

from dbworkload.cli.dep import Param, EPILOG
from .. import __version__
from enum import Enum
from pathlib import Path
from typing import Optional
import json
import logging
import os
import dbworkload.cli.util
import dbworkload.models.run
import dbworkload.models.util
import dbworkload.utils.common
import platform
import re
import sys
import typer
import yaml


logger = logging.getLogger("dbworkload")


class Driver(str, Enum):
    postgres = "postgres"
    mysql = "mysql"
    maria = "maria"
    oracle = "oracle"
    sqlserver = "sqlserver"
    mongo = "mongo"
    cassandra = "cassandra"


app = typer.Typer(
    epilog=EPILOG,
    no_args_is_help=True,
    help=f"dbworkload v{__version__}: DBMS workload utility.",
)


app.add_typer(dbworkload.cli.util.app, name="util")

version: bool = typer.Option(True)


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"


@app.command(help="Run the workload.", epilog=EPILOG, no_args_is_help=True)
def run(
    workload_path: Optional[Path] = Param.WorkloadPath,
    builtin_workload: str = typer.Option(None, help="Built-in workload."),
    # driver: str = typer.Option(None, help="Driver name"),
    driver: Driver = typer.Option(None, help="DBMS driver."),
    uri: str = Param.db_uri,
    conn_args_file: Optional[Path] = typer.Option(
        None,
        "--conn-args-file",
        "-i",
        help="Filepath to the connection arguments file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    procs: int = Param.Procs,
    args: str = Param.Args,
    concurrency: int = typer.Option(
        1, "-c", "--concurrency", help="Number of concurrent workers."
    ),
    ramp: int = typer.Option(0, "-r", "--ramp", help="Ramp up time in seconds."),
    iterations: int = typer.Option(
        None,
        "-i",
        "--iterations",
        help="Total number of iterations. Defaults to <ad infinitum>.",
        show_default=False,
    ),
    duration: int = typer.Option(
        None,
        "-d",
        "--duration",
        help="Duration in seconds. Defaults to <ad infinitum>.",
        show_default=False,
    ),
    conn_duration: int = typer.Option(
        None,
        "-k",
        "--conn-duration",
        show_default=False,
        help="The number of seconds to keep database connection alive before restarting. Defaults to <ad infinitum>.",
    ),
    app_name: Optional[str] = typer.Option(
        None,
        "--app-name",
        "-a",
        help="The application name specified by the client. Defaults to <db-name>.",
        show_default=False,
    ),
    autocommit: bool = typer.Option(
        True,
        "--no-autocommit",
        show_default=False,
        help="Unset autocommit in the connections.",
    ),
    frequency: int = typer.Option(
        10,
        "-s",
        "--stats-frequency",
        help="How often to display the stats in seconds. Set 0 to disable",
    ),
    prom_port: int = typer.Option(
        26260, "-p", "--port", help="The port of the Prometheus server."
    ),
    log_level: LogLevel = Param.LogLevel,
):
    logger.setLevel(log_level.upper())

    logger.debug("Executing run()")

    if not procs:
        procs = os.cpu_count()

    # check workload is a valid module and class
    if workload_path:
        workload = dbworkload.utils.common.import_class_at_runtime(workload_path)
    else:
        workload = dbworkload.utils.common.import_class_at_runtime(builtin_workload)

    conn_info = {}

    # check if the uri parameter is actually a URI
    if re.search(r".*://.*/(.*)\?", uri):
        driver = dbworkload.utils.common.get_driver_from_uri(uri)
        
        uri = dbworkload.utils.common.set_query_parameter(
            url=uri,
            param_name="application_name",
            param_value=app_name if app_name else workload.__name__,
        )
        if driver == "postgres":
            conn_info["conninfo"] = uri

        elif driver == "mongo":
            conn_info["host"] = uri

    else:
        driver = driver.value
        # if not, split the key-value pairs
        for pair in uri.replace(" ", "").split(","):
            k, v = pair.split("=")
            conn_info[k] = v

    
        if driver == "mysql":
            conn_info["autocommit"] = autocommit

    args = load_args(args)

    dbworkload.models.run.run(
        concurrency,
        workload_path,
        builtin_workload,
        frequency,
        prom_port,
        iterations,
        procs,
        ramp,
        conn_info,
        duration,
        conn_duration,
        args,
        driver,
        log_level.upper(),
    )


def load_args(args: str):
    # load args dict from file or string
    if args:
        if os.path.exists(args):
            with open(args, "r") as f:
                args = f.read()
                # parse into JSON if it's a JSON string
                try:
                    return json.load(args)
                except Exception as e:
                    pass
        else:
            args = yaml.safe_load(args)
            if isinstance(args, str):
                logger.error(
                    f"The value passed to '--args' is not a valid path to a JSON/YAML file, nor has no key:value pairs: '{args}'"
                )
                sys.exit(1)
            else:
                return args
    return {}


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"dbworkload : {__version__}")
        typer.echo(f"Python     : {platform.python_version()}")
        raise typer.Exit()


@app.callback()
def version_option(
    _: bool = typer.Option(
        False,
        "--version",
        "-v",
        callback=_version_callback,
        help="Print the version and exit",
    ),
) -> None:
    pass
