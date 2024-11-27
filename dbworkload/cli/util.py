#!/usr/bin/python

from pathlib import Path
from enum import Enum
from typing import Optional
import dbworkload.models.run
import dbworkload.models.util
import dbworkload.utils.common
from dbworkload.cli.dep import Param, EPILOG
import typer


class Compression(str, Enum):
    bz2 = "bz2"
    gzip = "gzip"
    xz = "xz"
    zip = "zip"


app = typer.Typer(
    epilog=EPILOG,
    no_args_is_help=True,
    help="Various utils.",
)


@app.command(
    "csv",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Generate CSV files from a YAML data generation file.",
)
def util_csv(
    input: Optional[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="Filepath to the YAML data generation file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        show_default=False,
        help="Output directory for the CSV files. Defaults to <input-basename>.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    procs: int = Param.Procs,
    csv_max_rows: int = Param.CSVMaxRows,
    http_server_hostname: str = typer.Option(
        "localhost",
        "-n",
        "--hostname",
        show_default=False,
        help="The hostname of the http server that serves the CSV files.",
    ),
    http_server_port: int = typer.Option(
        3000,
        "-p",
        "--port",
        help="The port of the http server that servers the CSV files.",
    ),
    compression: Compression = typer.Option(
        None,
        "-c",
        "--compression",
        help="The compression format.",
    ),
    delimiter: str = typer.Option(
        "\t",
        "-d",
        "--delimiter",
        help='The delimeter char to use for the CSV files. Defaults to "tab".',
        show_default=False,
    ),
):
    dbworkload.models.util.util_csv(
        input=input,
        output=output,
        compression=compression,
        procs=procs,
        csv_max_rows=csv_max_rows,
        delimiter=delimiter,
        http_server_hostname=http_server_hostname,
        http_server_port=http_server_port,
    )


@app.command(
    "yaml",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Generate YAML data generation file from a DDL SQL file.",
)
def util_yaml(
    input: Optional[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="Filepath to the DDL SQL file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        show_default=False,
        help="Output filepath. Defaults to <input-basename>.yaml.",
        exists=False,
        file_okay=True,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
):
    dbworkload.models.util.util_yaml(input=input, output=output)


@app.command(
    "merge_sort",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Merge-Sort multiple sorted CSV files into 1+ files.",
)
def util_sort_merge(
    input: Optional[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="Directory of files to be merged",
        exists=True,
        file_okay=False,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        show_default=False,
        help="Output filepath. Defaults to <input>.merged.",
        exists=False,
        file_okay=True,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    csv_max_rows: int = Param.CSVMaxRows,
    compress: bool = typer.Option(
        True,
        "--no-compress",
        show_default=False,
        help="Do not gzip output files.",
    ),
):
    dbworkload.models.util.util_merge_sort(input, output, csv_max_rows, compress)


@app.command(
    "plot",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Plot charts from the dbworkload statistics CSV file.",
)
def util_plot(
    input: Optional[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="Input CSV file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
):
    dbworkload.models.util.util_plot(input)


@app.command(
    "html",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Save charts to HTML from the dbworkload statistics CSV file.",
)
def util_html(
    input: Optional[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="Input CSV file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
):
    dbworkload.models.util.util_html(input)


@app.command(
    "merge_csvs",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Merge multiple dbworkload statistic CSV files.",
)
def util_merge_csvs(
    input_dir: Optional[Path] = typer.Option(
        ...,
        "--input_dir",
        "-i",
        help="Input CSV directory",
        exists=True,
        file_okay=False,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
):
    dbworkload.models.util.util_merge_csvs(input_dir)


@app.command(
    "gen_stub",
    epilog=EPILOG,
    no_args_is_help=True,
    help="Generate a dbworkload class stub.",
)
def util_gen_stub(
    input_file: Optional[Path] = typer.Option(
        ...,
        "--input",
        "-i",
        help="Input SQL file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
):
    dbworkload.models.util.util_gen_stub(input_file)
