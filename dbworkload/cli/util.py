#!/usr/bin/python

import sys
from enum import Enum
from pathlib import Path
from typing import Optional

import typer

import dbworkload.models.util
from dbworkload.cli.dep import EPILOG, Param

try:
    from ..models.convert import ConvertTool
except:
    pass


class Compression(str, Enum):
    bz2 = "bz2"
    gzip = "gzip"
    xz = "xz"
    zip = "zip"


util_app = typer.Typer(
    epilog=EPILOG,
    no_args_is_help=True,
    help="Various utils.",
)


@util_app.command(
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


@util_app.command(
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


@util_app.command(
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


@util_app.command(
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


@util_app.command(
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


@util_app.command(
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


@util_app.command(
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


@util_app.command(
    name="convert",
    help="Convert from PL to PL/pgSQL",
    no_args_is_help=True,
)
def cli_convert(
    base_dir: Optional[Path] = typer.Option(
        ".",
        "--dir",
        "-d",
        help="Directory path",
        exists=True,
        file_okay=False,
        dir_okay=True,
        writable=False,
        readable=True,
        resolve_path=True,
    ),
    uri: str = typer.Option(
        None,
        "--uri",
        help="The connection URI to the database.",
    ),
    root_file: Optional[str] = typer.Option(
        None,
        "--root-file",
        "-r",
        help="The root_file. Leave empty for processing all *.ddl files.",
    ),
    generator_llm: Optional[str] = typer.Option(
        "Ollama:llama3.2:3b",
        "--generator-llm",
        "-g",
        help="The generator provider:model_name",
    ),
    refiner_llm: Optional[str] = typer.Option(
        "OpenAI:gpt-5",
        "--refiner-llm",
        "-n",
        help="The refiner provider:model_name.",
    ),
):

    try:
        ConvertTool(
            base_dir,
            uri,
            root_file,
            generator_llm,
            refiner_llm,
            # seed,
            # seed_each_time,
        ).run()
    except Exception as e:
        print(e, file=sys.stderr)
        typer.Exit(1)
