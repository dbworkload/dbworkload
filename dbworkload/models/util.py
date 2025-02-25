#!/usr/bin/python
import csv
import string
from io import TextIOWrapper
from dbworkload.models.ddl_generator import Generate_ddls
from jinja2 import Environment, PackageLoader
from pathlib import PosixPath
from plotly.subplots import make_subplots
from pytdigest import TDigest
import datetime as dt
import dbworkload
import dbworkload.utils.common
import dbworkload.utils.simplefaker
import gzip
import itertools
import logging
import numpy as np
import os
import pandas as pd
import plotext as plt
import plotly.graph_objects as go
import plotly.io as pio
import shutil
import sqlparse
import sys
import yaml
import re

logger = logging.getLogger("dbworkload")
logger.setLevel(logging.INFO)


def util_csv(
        input: PosixPath,
        output: PosixPath,
        compression: str,
        procs: int,
        csv_max_rows: int,
        delimiter: str,
        http_server_hostname: str,
        http_server_port: str,
):
    """Wrapper around SimpleFaker to create CSV datasets
    given an input YAML data gen definition file
    """

    with open(input, "r") as f:
        load: dict = yaml.safe_load(f.read())

    if not output:
        output_dir = dbworkload.utils.common.get_based_name_dir(input)
    else:
        output_dir = output

    # backup the current directory as to not override
    if os.path.isdir(output_dir):
        os.rename(
            output_dir,
            str(output_dir) + "." + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S"),
        )

    # create new directory
    os.mkdir(output_dir)

    if not compression:
        compression = None

    if not procs:
        procs = os.cpu_count()

    dbworkload.utils.simplefaker.SimpleFaker(csv_max_rows=csv_max_rows).generate(
        load, int(procs), output_dir, delimiter, compression
    )

    csv_files = os.listdir(output_dir)

    for table_name in load.keys():
        print(f"=== IMPORT STATEMENTS FOR TABLE {table_name} ===\n")

        for s in dbworkload.utils.common.get_import_stmts(
                [x for x in csv_files if x.startswith(table_name)],
                table_name,
                http_server_hostname,
                http_server_port,
                delimiter,
                "",
        ):
            print(s, "\n")

        print()


def util_yaml(input: PosixPath, output: PosixPath):
    """Wrapper around util function ddl_to_yaml() for
    crafting a data gen definition YAML string from
    CREATE TABLE statements.
    """

    with open(input, "r") as f:
        ddl = f.read()

    if not output:
        output = dbworkload.utils.common.get_based_name_dir(input) + ".yaml"

    # backup the current file as to not override
    if os.path.exists(output):
        os.rename(output, str(output) + "." + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S"))

    # create new file
    with open(output, "w") as f:
        f.write(dbworkload.utils.common.ddl_to_yaml(ddl))


def util_merge_sort(input_dir: str, output_dir: str, csv_max_rows: int, compress: bool):
    from operator import itemgetter

    class MergeSort:
        def __init__(
                self, input_dir: str, output_dir: str, csv_max_rows: int, compress: bool
        ):
            # input CSV files - it assumes files are already sorted
            files = os.listdir(input_dir)
            # Filtering only the files.
            self.CSVs = [
                os.path.join(input_dir, f)
                for f in files
                if os.path.isfile(os.path.join(input_dir, f))
            ]

            self.compress = ".gz" if compress else ""
            self.file_extension = self.CSVs[0][-3:]

            self.CSV_MAX_ROWS = csv_max_rows
            self.COUNTER = 0
            self.C = 0

            # source holds the list of lines in each CSV file, marked by the idx number
            # file_handlers holds a the open file handler for each CSV file, marked by the idx number
            self.source: dict[int, list] = {}
            self.file_handlers: dict[int, TextIOWrapper] = {}

            self.output: TextIOWrapper
            if not output_dir:
                self.output_dir = str(input_dir) + ".merged"
            else:
                self.output_dir = output_dir

            # backup the current file as to not override
            if os.path.exists(self.output_dir):
                os.rename(
                    self.output_dir,
                    str(self.output_dir)
                    + "."
                    + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S"),
                )

            # create new directory
            os.mkdir(self.output_dir)

        def initial_fill(self, csv: str, idx: int):
            """
            opens the CSV file, saves the file handler,
            read few lines into source list for the index.
            """
            f = open(csv, "r")
            self.file_handlers[idx] = f
            while len(self.source[idx]) < 5:
                line = f.readline()
                if line != "":
                    self.source[idx].append(line)
                else:
                    # reached end of file
                    logger.debug(
                        f"initial_fill: CSV file '{csv}' at source index {idx} reached EOF."
                    )
                    f.close()
                    break

        def replenish_source_list(self, idx: int):
            """
            Refills the source list with a new value from the source file
            """
            try:
                f = self.file_handlers.get(idx, None)
                if not f:
                    return
                line = f.readline()
                if line != "":
                    self.source[idx].append(line)
                else:
                    # reached end of file
                    logger.debug(f"index {idx} reached EOF.")
                    f.close()
                    del self.file_handlers[idx]
            except Exception as e:
                logger.error("Excepton in replenish_queue: ", e)

        def close_output(self):
            self.output.close()

            if self.compress:
                with open(self.output.name, "rb") as f_in:
                    with gzip.open(f"{self.output.name}{self.compress}", "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(self.output.name)

            logger.info(f"Saved {self.output_filename}{self.compress}")

        def open_new_output(self):
            self.output_filename = (
                f"out_{str.zfill(str(self.COUNTER), 6)}.{self.file_extension}"
            )
            self.output = open(
                os.path.join(self.output_dir, self.output_filename),
                "+w",
            )

        def write_to_csv(self, v: str):
            # create a new output file if the limit is reached
            if self.C >= self.CSV_MAX_ROWS:
                self.close_output()

                self.COUNTER += 1
                self.C = 0

                self.open_new_output()

            self.output.write(v)
            self.C += 1

        def run(self):
            # init the source dict by opening each CSV file
            # and only reading few lines.
            for idx, csv in enumerate(self.CSVs):
                self.source[idx] = []

                self.initial_fill(csv, idx)

            # the source dict now has a key for every file and a list of the first values read
            # the file_handler dict has a key for every file and a pointer to the open file handler

            staging = []
            # pop the first value in each source list to list `staging`
            # `staging` will have the first values of all source CSV files
            for k, v in self.source.items():
                try:
                    staging.append((v.pop(0), k))
                except IndexError as e:
                    pass
            from pprint import pprint

            first_k = None
            first_v = None
            self.open_new_output()

            # sort list `staging`
            # pop the first value (the smallest) in `first_v`
            # make a note of the source of that value in `first_k`
            # replenish the corrisponding source

            while True:
                if first_k is not None:
                    try:
                        self.replenish_source_list(first_k)
                        staging.append((self.source[first_k].pop(0), first_k))

                    except IndexError as e:
                        # the source list is empty
                        logger.debug(f"source list {first_k} is now empty")
                        first_k = None

                if staging:
                    staging.sort(key=itemgetter(0))
                    try:
                        first_v, first_k = staging.pop(0)
                        self.write_to_csv(first_v)
                    except IndexError as e:
                        logger.warning("Exception in main: ", e)
                        self.output.close()
                else:
                    break

            self.close_output()

            logger.info("Completed")

    MergeSort(input_dir, output_dir, csv_max_rows, compress).run()


def util_plot(input: PosixPath):
    df = pd.read_csv(
        input,
        header=0,
        names=[
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
        ],
    )

    # define index column
    df.set_index("elapsed", inplace=True)

    plt.clf()
    plt.theme("pro")
    plt.subplots(3, 1)
    plt.subplot(1, 1).title(f"Test Run: {input.stem}")

    for id in df["id"].unique():
        df1 = df[df["id"] == id]

        # p99
        plt.subplot(1, 1).plotsize(None, plt.th() // 1.7)
        plt.plot(
            df1["p99_ms"].index, df1["p99_ms"], label=f"{id}_p99", marker="braille"
        )

        # ops/s
        plt.subplot(2, 1)
        plt.plot(
            df1["period_ops_s"].index,
            df1["period_ops_s"],
            label=f"{id}_ops/s",
            marker="braille",
        )

    plt.subplot(3, 1)
    plt.xlabel("elapsed")
    plt.bar(df1["threads"].index, df1["threads"], label="threads", marker="braille")

    plt.show()


def util_html(input: PosixPath):
    TEMPLATE_NAME = "plotly_dark"
    COLORS = itertools.cycle(pio.templates[TEMPLATE_NAME].layout.colorway)

    def get_color():
        return next(COLORS)

    out = os.path.join(input.parent, input.stem + ".html")

    df = pd.read_csv(
        input,
        header=0,
        names=[
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
        ],
    )

    # Create subplots and mention plot grid size
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=("Response Time (ms)", "ops/s", "concurrency"),
        row_width=[0.15, 0.3, 0.7],
    )

    fig.update_layout(
        template=TEMPLATE_NAME,
        title=f"Test Run: {input.stem}",
        hovermode="x unified",
        hoversubplots="axis",  # not working yet
        xaxis_rangeslider_visible=False,
        xaxis3_title_text="elapsed",
    )

    for id in sorted(df["id"].unique()):
        df1 = df[df["id"] == id]

        line_color = get_color()

        fig.add_trace(
            go.Scatter(
                name=f"{id}_p99",
                x=df1["elapsed"],
                y=df1["p99_ms"],
                line=dict(color=line_color, width=1.7),
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                name=f"{id}_mean",
                x=df1["elapsed"],
                y=df1["mean_ms"],
                line=dict(color=line_color, width=0.5, dash="dot"),
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                name=f"{id}_ops/s",
                x=df1["elapsed"],
                y=df1["period_ops_s"],
                line=dict(color=line_color, width=1),
            ),
            row=2,
            col=1,
        )

        # only __cycle__ is guaranteed to be present throughout the entire test run
        if id == "__cycle__":
            thread_bar = go.Bar(
                name="threads",
                x=df1["elapsed"],
                y=df1["threads"],
            )

    fig.add_trace(
        thread_bar,
        row=3,
        col=1,
    )

    fig.write_html(out)
    logger.info(f"Saved merged CSV file to '{out}'")


def util_merge_csvs(input_dir: str):
    logger.warning(
        "This feature is experimental. Validate results and file any bug/issue."
    )

    # collect only regular, CSV files.
    files = os.listdir(input_dir)
    CSVs = [
        os.path.join(input_dir, f)
        for f in files
        if os.path.isfile(os.path.join(input_dir, f)) and f.endswith(".csv")
    ]

    if not CSVs:
        logger.error(f"No valid CSVs in directory '{input_dir}'")
        sys.exit(1)

    # use one of the CSVs filename to create the output filename
    out = os.path.basename(CSVs[0])[:-4] + ".merged.csv"

    # read all CSVs into a single df, sorted by `ts``
    df = pd.concat((pd.read_csv(f) for f in CSVs), ignore_index=True).sort_values("ts")

    min_ts = df["ts"].min()

    # convert the current `centroids` string to a 2-dims np.array
    df["centroids"] = df["centroids"].apply(str.split, args=(";",)).apply(np.genfromtxt)

    def get_elapsed_bucket(x):
        """
        for a given timestamp x, return the
        relative elapsed time in steps of 10s.
        Eg: min_ts=1000 and

        x=1023:
        1023-1000=23 --> 40

        x=1010:

        1010-1000=10 --> 20
        """
        x -= min_ts
        return (x if x % 10 == 0 else x + 10 - x % 10) + 10

    # rebase all ts values into ranges (buckets) of 10s
    df["elapsed"] = df["ts"].apply(get_elapsed_bucket)

    def combine_centroids(x):
        """
        combine centroids of multiple TDigests together,
        and return the new aggregated centroids.
        Note: compression=1000
        """
        return (
            TDigest(compression=1000)
            .combine([TDigest.of_centroids(y, compression=1000) for y in x])
            .get_centroids()
        )

    # for each elapsed range bucket, merge the data for all `id` together
    # by aggregating the count of `threads` and by aggregating the `centroids`
    df = df.groupby(["elapsed", "id"]).agg(
        {"ts": min, "threads": sum, "centroids": combine_centroids}
    )

    # the weight of the TDigest represents the count of ops
    df["period_ops"] = df["centroids"].map(
        lambda x: TDigest(compression=1000).of_centroids(x, compression=1000).weight
    )

    df["period_ops_s"] = df["period_ops"].apply(lambda x: x // 10)

    df["tot_ops"] = df["period_ops"].groupby(["id"]).cumsum()

    # convert `elabpsed` and `id` to regular df columns
    df = df.reset_index()

    df["tot_ops_s"] = df["tot_ops"] // df["elapsed"]

    # calculate mean and quantiles and convert from seconds to millis
    df["mean_ms"] = df["centroids"].map(
        lambda x: TDigest(compression=1000).of_centroids(x, compression=1000).mean
                  * 1000
    )
    df[["p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms"]] = [
        x * 1000
        for x in df["centroids"].map(
            lambda x: TDigest(compression=1000)
            .of_centroids(x, compression=1000)
            .inverse_cdf([0.50, 0.90, 0.95, 0.99, 1.00])
        )
    ]

    # round all values to 2 decimals
    df[["mean_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms"]] = df[
        ["mean_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms"]
    ].apply(round, args=(2,))

    # rearrange cols and eliminate centroids while keeping the column
    df = df[
        [
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
        ]
    ]

    df["centroids"] = None

    # finally, save the df to file
    df.to_csv(out, index=False)
    logger.info(f"Saved merged CSV file to '{out}'")


def util_gen_stub(input_file: PosixPath):
    env = Environment(loader=PackageLoader("dbworkload"))
    template = env.get_template("stub.j2")

    out = os.path.join(input_file.parent, input_file.stem + ".py")

    with open(input_file, "r") as f:
        content = f.read()

    # Remove all multiline comments (/* ... */)
    while True:
        i = content.find("/*")
        j = content.find("*/")
        if i < 0:
            break
        content = content[:i] + content[j + 2:]

    # Remove line comments (--) and empty lines
    cleaned_lines = []
    for line in content.split("\n"):
        line = line.strip()
        comment_index = line.find("--")
        if comment_index >= 0:
            line = line[:comment_index]
        if line:
            cleaned_lines.append(line)
    clean_ddl = " ".join(cleaned_lines)

    # Determine if the file uses explicit BEGIN/COMMIT boundaries
    if "begin" in clean_ddl.lower() and "commit" in clean_ddl.lower():
        transactions = []
        current_txn = []
        inside_txn = False
        # First split on semicolon; note that semicolons inside a transaction will still separate the statements.
        raw_stmts = [stmt.strip() for stmt in clean_ddl.split(";") if stmt.strip()]
        for stmt in raw_stmts:
            lower_stmt = stmt.lower()
            if not inside_txn:
                if lower_stmt.startswith("begin"):
                    # Start a new transaction block
                    inside_txn = True
                    current_txn = [stmt]
                else:
                    # Statement outside any transaction block, treat as an independent transaction
                    transactions.append(stmt)
            else:
                current_txn.append(stmt)
                if lower_stmt.startswith("commit"):
                    # End the current transaction block
                    inside_txn = False
                    # Join all parts of the transaction into one string.
                    transactions.append(";".join(current_txn) + ";")
                    current_txn = []
        # If we ended with an unclosed transaction, add what we have.
        if inside_txn and current_txn:
            transactions.append(" ; ".join(current_txn))
    else:
        # No BEGIN/COMMIT markers; treat each semicolon-separated statement as a transaction.
        transactions = [stmt.strip() for stmt in clean_ddl.split(";") if stmt.strip()]

    model = {}
    model["txn_count"] = len(transactions)
    model["name"] = input_file.name.split(".")[0].capitalize()
    model["txns"] = [
        sqlparse.format(txn, reindent=True, keyword_case="upper") for txn in transactions
    ]

    phs = []
    txn_type = []
    for txn in transactions:
        phs.append(txn.count("%s"))
        txn_type.append(
            txn.lower().startswith("select") or txn.lower().find("returning") > 0
        )
    model["bind_params"] = phs
    model["txn_type"] = txn_type

    with open(out, "w") as f:
        f.write(template.render(model=model))

    logger.info(f"Saved stub '{out}'")

def init(input_file: PosixPath):
    return

def generate_workload(zip_content_location, all_schemas, db_name, output_file_location):
    """
    Reads a TSV file named 'crdb_internal.node_statement_statistics.txt' from each numeric node directory
    under zip_content_location/nodes (starting at 1), stopping when a directory doesn't exist.

    Filters rows by db_name and excludes rows where "job id=" appears in the application_name.
    For each txn_fingerprint_id, it only aggregates key values from the first node that contains it;
    if the txn_id is encountered in a later node, its key values are ignored.

    Finally, it writes the grouped key values to <db_name>.workload.sql in the output_file_location.
    """
    statement_statistics_file_name = "crdb_internal.node_statement_statistics.txt"
    nodes_base_dir = os.path.join(zip_content_location, "nodes")

    # We'll store each txn_id along with the node it was first found in and its key values.
    # Structure: { txn_id: { "node": node_number, "keys": [list of key values] } }
    grouped_keys = {}
    node = 1

    while True:
        node_dir = os.path.join(nodes_base_dir, str(node))
        if not os.path.exists(node_dir):
            # Stop when a node directory doesn't exist.
            break

        file_path = os.path.join(node_dir, statement_statistics_file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Could not find TSV file: {file_path}")

        with open(file_path, mode="r", newline="", encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter='\t', quotechar='"')
            header = next(reader, None)
            if not header:
                raise ValueError(f"TSV file is empty or missing a header row: {file_path}")

            # Map column names to indices.
            column_index = {col: i for i, col in enumerate(header)}

            # Validate required columns.
            required_columns = [
                "database_name",
                "application_name",
                "key",
                "txn_fingerprint_id",
            ]
            for col in required_columns:
                if col not in column_index:
                    raise ValueError(f"Missing expected column '{col}' in TSV header of file: {file_path}")

            # Process each row.
            for row in reader:
                if row[column_index["database_name"]] == db_name and \
                        "job id=" not in row[column_index["application_name"]]:
                    txn_id = row[column_index["txn_fingerprint_id"]]
                    key_val = replace_placeholders(row[column_index["key"]], all_schemas)

                    if txn_id in grouped_keys:
                        # Only add key values from the same node where this txn_id was first encountered.
                        if grouped_keys[txn_id]["node"] == node:
                            grouped_keys[txn_id]["keys"].append(key_val)
                        # If txn_id was first found in an earlier node, skip adding new key values.
                    else:
                        grouped_keys[txn_id] = {"node": node, "keys": [key_val]}

        node += 1  # Move on to the next node directory.

    output_path = os.path.join(output_file_location, f"{db_name}.workload.sql")
    with open(output_path, mode="w", encoding="utf-8") as out_file:
        # Write each txn_fingerprint_id block.
        for txn_id, data in grouped_keys.items():
            keys = data["keys"]
            out_file.write("-------Begin Transaction------\nBEGIN;\n")
            for key in keys:
                out_file.write(key + ";\n")
            out_file.write("COMMIT;\n-------End Transaction-------\n\n\n")

    print(f"Successfully wrote {len(grouped_keys)} workload statements to {output_path}")
    return grouped_keys

def replace_placeholders(sql, all_schemas):
    """
    Replace placeholders in an SQL query:
    - `_` -> `%s`
    - `__more__` -> Expands based on the number of fields in VALUES (...) or IN (...)
    
    Returns the modified query.
    """

    # Match the VALUES clause in an INSERT statement
    values_match = re.search(r'\((.*)\).*VALUES\s.*?', sql, re.IGNORECASE)
    table_name = extract_table_names(sql)
    schema = all_schemas[table_name]
    if values_match:
        values_content = values_match.group(1)  # Extract content inside VALUES(...)
        fields = [f.strip() for f in values_content.split(',')]  # Split by comma and trim spaces
        
        expanded_values = []
        for field in fields:
            expanded_values.append(schema.columns[field].col_type)

        # Generate the correct VALUES clause
        placeholders = ', '.join(expanded_values)
        
        # Replace the entire VALUES(...) with corrected placeholders
        sql = re.sub(r'VALUES\s*\(.*?\)', f'VALUES ({placeholders})', sql, flags=re.IGNORECASE)

    # Match IN clauses and replace
    def replace_in_clause(match):
        content = match.group(1)  # Extract inside IN(...)
        post_in = match.group(2)

        return f'IN ({content.replace("__more__","%s").replace("_","%s")}){post_in}'

    sql = re.sub(r'IN\s*\((.*)\)(\s+\w)', replace_in_clause, sql, flags=re.IGNORECASE)

    return sql

def extract_table_names(statement):
    # Regular expressions to match different SQL statements
    insert_pattern = re.compile(r'INSERT INTO\s+["]?(\w+)["]?', re.IGNORECASE)
    select_pattern = re.compile(r'SELECT.*?FROM\s+["]?(\w+)["]?', re.IGNORECASE)
    update_pattern = re.compile(r'UPDATE\s+["]?(\w+)["]?', re.IGNORECASE)
    delete_pattern = re.compile(r'DELETE FROM\s+["]?(\w+)["]?', re.IGNORECASE)

    table_names = set()

    # Find all matches for each pattern
    table_names.update(insert_pattern.findall(statement))
    table_names.update(select_pattern.findall(statement))
    table_names.update(update_pattern.findall(statement))
    table_names.update(delete_pattern.findall(statement))

    return next(iter(table_names), None)

def generate(input_file: PosixPath, db_name):
    all_schemas = Generate_ddls(input_file, str(db_name), os.path.curdir)
    generate_workload(input_file, all_schemas, str(db_name), os.path.curdir)
    file_name = db_name.with_suffix(".workload.sql")
    util_gen_stub(PosixPath(file_name))
    return
