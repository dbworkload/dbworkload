#!/usr/bin/python
import csv
import os
import re
from pathlib import PosixPath
from typing import Dict, Any

from dbworkload.models.placeholder_processor import replace_placeholders


def generate_workload(
    zip_content_location: str,
    all_schemas: Dict[str, Any],
    db_name: str,
    output_file_location: str,
    mapping: Dict[str, Any] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Reads a TSV file named 'crdb_internal.node_statement_statistics.txt' from each numeric node directory
    under zip_content_location/nodes (starting at 1), stopping when a directory doesn't exist.

    Filters rows by db_name and excludes rows where "job id=" appears in the application_name.
    For each txn_fingerprint_id, it only aggregates key values from the first node that contains it;
    if the txn_id is encountered in a later node, its key values are ignored.

    Finally, it writes the grouped key values to <db_name>.sql in the output_file_location.

    Args:
        zip_content_location: Path to the directory containing node statistics
        all_schemas: Dictionary containing schema information
        db_name: Name of the database to filter statements for
        output_file_location: Directory where the output SQL file will be written
        mapping: Optional dictionary containing table and column name mappings for anonymization

    Returns:
        Dictionary containing grouped transaction keys
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
            reader = csv.reader(tsv_file, delimiter="\t", quotechar='"')
            header = next(reader, None)
            if not header:
                raise ValueError(
                    f"TSV file is empty or missing a header row: {file_path}"
                )

            # Map column names to indices, mapping.
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
                    raise ValueError(
                        f"Missing expected column '{col}' in TSV header of file: {file_path}"
                    )

            # Process each row.
            for row in reader:
                if (
                    row[column_index["database_name"]] == db_name
                    and "job id=" not in row[column_index["application_name"]]
                ):
                    txn_id = row[column_index["txn_fingerprint_id"]]
                    if mapping:
                        anon_stmt = anonymize_sql_statement(
                            row[column_index["key"]], mapping
                        )
                        key_val = replace_placeholders(anon_stmt, all_schemas)
                    else:
                        key_val = replace_placeholders(
                            row[column_index["key"]], all_schemas
                        )
                    if re.search(r"\b(DEALLOCATE|WHEN)\b", key_val):
                        # TODO - not handled
                        continue

                    if txn_id in grouped_keys:
                        # Only add key values from the same node where this txn_id was first encountered.
                        if grouped_keys[txn_id]["node"] == node:
                            grouped_keys[txn_id]["keys"].append(key_val)
                        # If txn_id was first found in an earlier node, skip adding new key values.
                    else:
                        grouped_keys[txn_id] = {"node": node, "keys": [key_val]}

        node += 1  # Move on to the next node directory.

    output_path = os.path.join(output_file_location, f"{db_name}.sql")
    with open(output_path, mode="w", encoding="utf-8") as out_file:
        # Write each txn_fingerprint_id block.
        for txn_id, data in grouped_keys.items():
            keys = data["keys"]
            out_file.write("-------Begin Transaction------\nBEGIN;\n")
            for key in keys:
                out_file.write(key + ";\n")
            out_file.write("COMMIT;\n-------End Transaction-------\n\n\n")

    print(
        f"Successfully wrote {len(grouped_keys)} workload statements to {output_path}"
    )
    return grouped_keys


def anonymize_sql_statement(sql_statement: str, mapping: Dict[str, Any]) -> str:
    """
    Anonymize column and table names in SQL statements using the provided mapping.

    Args:
        sql_statement: A string containing any SQL statement
        mapping: The mapping dictionary with anonymized table and column names

    Returns:
        str: The anonymized SQL statement
    """
    # Make a copy of the original statement
    anonymized_sql = sql_statement

    # Step 1: Anonymize table names
    for original_table, anon_table in mapping["tables"].items():
        simple_name = original_table.split(".")[-1]

        # Replace table references with word boundaries
        anonymized_sql = re.sub(
            r"\bFROM\s+" + re.escape(simple_name) + r"\b",
            f"FROM {anon_table}",
            anonymized_sql,
            flags=re.IGNORECASE,
        )

        anonymized_sql = re.sub(
            r"\bJOIN\s+" + re.escape(simple_name) + r"\b",
            f"JOIN {anon_table}",
            anonymized_sql,
            flags=re.IGNORECASE,
        )

        anonymized_sql = re.sub(
            r"\bUPDATE\s+" + re.escape(simple_name) + r"\b",
            f"UPDATE {anon_table}",
            anonymized_sql,
            flags=re.IGNORECASE,
        )

        anonymized_sql = re.sub(
            r"\bINSERT\s+INTO\s+" + re.escape(simple_name) + r"\b",
            f"INSERT INTO {anon_table}",
            anonymized_sql,
            flags=re.IGNORECASE,
        )

    # Step 2: Create a comprehensive list of words to avoid replacing
    avoid_words = {
        "select", "from", "where", "group", "order", "by", "having", "limit",
        "insert", "update", "delete", "set", "into", "values", "returning",
        "join", "inner", "outer", "left", "right", "full", "on", "using",
        "union", "all", "intersect", "except", "case", "when", "then", "else",
        "end", "and", "or", "not", "null", "true", "false", "is", "in",
        "between", "like", "as", "of", "system", "time", "distinct", "exists",
        "any", "some", "offset", "asc", "desc", "interval", "count", "sum",
        "avg", "min", "max", "now",
    }

    # Step 3: Determine the primary table
    primary_table = None

    # Look for table patterns
    for pattern in [
        r"\bFROM\s+([^\s,();]+)",
        r"\bUPDATE\s+([^\s,();]+)",
        r"\bINSERT\s+INTO\s+([^\s,();]+)",
    ]:
        match = re.search(pattern, anonymized_sql, re.IGNORECASE)
        if match:
            table_ref = match.group(1)
            # Find the matching original table
            for original_table in mapping["tables"]:
                if (
                    original_table.split(".")[-1] == table_ref
                    or mapping["tables"][original_table] == table_ref
                ):
                    primary_table = original_table
                    break
            if primary_table:
                break

    # Step 4: Handle INSERT columns specifically
    if primary_table and "INSERT INTO" in anonymized_sql.upper():
        insert_match = re.search(
            r"INSERT\s+INTO\s+[^\s(]+\s*\(([^)]+)\)", anonymized_sql, re.IGNORECASE
        )

        if insert_match:
            columns_str = insert_match.group(1)
            columns = [col.strip() for col in columns_str.split(",")]

            # Use primary table's column mapping
            anonymized_columns = []
            for col in columns:
                if (
                    primary_table in mapping["columns"]
                    and col in mapping["columns"][primary_table]
                ):
                    anonymized_columns.append(mapping["columns"][primary_table][col])
                else:
                    # If not found in primary table, check other tables
                    found = False
                    for table in mapping["columns"]:
                        if col in mapping["columns"][table]:
                            anonymized_columns.append(mapping["columns"][table][col])
                            found = True
                            break
                    if not found:
                        anonymized_columns.append(col)

            # Replace the column list
            new_columns_str = ", ".join(anonymized_columns)
            anonymized_sql = re.sub(
                r"(INSERT\s+INTO\s+[^\s(]+\s*\()([^)]+)(\))",
                r"\1" + new_columns_str + r"\3",
                anonymized_sql,
                flags=re.IGNORECASE,
            )

    # Step 5: For each table in the mapping, explicitly replace its columns
    processed_columns = set()

    for table_name, table_columns in mapping["columns"].items():
        for original_col, anon_col in table_columns.items():
            # Skip if already processed or if it's a word to avoid
            if original_col in processed_columns or original_col.lower() in avoid_words:
                continue

            # Use word boundaries to replace only whole words
            anonymized_sql = re.sub(
                r"\b" + re.escape(original_col) + r"\b", anon_col, anonymized_sql
            )

            processed_columns.add(original_col)

    # Step 6: Special handling for specific keywords that should not be anonymized
    for keyword in ["valid", "INTERVAL", "TIMESTAMPTZ", "LIMIT"]:
        # If the keyword was accidentally anonymized, restore it
        for anon_col in processed_columns:
            anonymized_sql = re.sub(
                r"\b" + re.escape(anon_col) + r"\b" + keyword, keyword, anonymized_sql
            )

    return anonymized_sql 