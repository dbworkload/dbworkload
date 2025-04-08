import os
import csv
import re
import string
import subprocess
import random


def generate_ddls(
    zip_content_location,
    db_name,
    output_file_location,
    cluster_url,
    output_file_name,
    anonymize,
):
    """
    Reads a TSV file named 'crdb_internal.create_statements.txt' from zip_content_location,
    filters rows by db_name and descriptor_type='table', updates each CREATE statement to
    prefix schema references with <db_name>.<schema>, then writes them to <db_name>.ddl
    in the output_file_location directory.

    We only retain the MOST RECENT CREATE statement for each table, but preserve the
    ordering of the table's first appearance in the file.
    """

    create_statement_file_name = "crdb_internal.create_statements.txt"
    file_path = os.path.join(zip_content_location, create_statement_file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Could not find TSV file: " + file_path)

    output_path = os.path.join(output_file_location, output_file_name)

    with open(file_path, mode="r", newline="", encoding="utf-8") as tsv_file:
        # Use the csv reader for tab-separated data.
        # If your data has embedded quotes/tabs/newlines, you may need more robust settings.
        reader = csv.reader(tsv_file, delimiter="\t", quotechar='"')

        # Attempt to read the header.
        header = next(reader, None)
        if not header:
            raise ValueError("TSV file is empty or missing a header row.")

        # Build a mapping of column names to their indices.
        column_index = {col: i for i, col in enumerate(header)}

        # Validate required columns exist.
        required_columns = [
            "database_name",
            "create_statement",
            "schema_name",
            "descriptor_type",
            "descriptor_name",
        ]
        for col in required_columns:
            if col not in column_index:
                raise ValueError(f"Missing expected column '{col}' in TSV header.")

        # Dictionary to hold the MOST RECENT create statement per table.
        # Key: "dbName.schemaName.tableName", Value: updated create statement.
        table_to_statement = {}

        # Keep track of the order in which we *first* encounter each table.
        order_of_tables = []
        seen_tables = set()

        # Regex for robust schema replacement:
        # - \b means "word boundary" so that we only match "schema."
        # - We'll replace that with "dbName.schema."
        # If your schema name can have special chars, you may need to escape them.
        schema_pattern_cache = {}

        for record in reader:
            # Only process rows for the given db_name and tables.
            if (
                record[column_index["database_name"]] == db_name
                and record[column_index["descriptor_type"]] == "table"
                and record[column_index["schema_name"]] == "public"
            ):

                schema = record[column_index["schema_name"]]
                create_stmt = record[column_index["create_statement"]]
                table_name = record[column_index["descriptor_name"]]

                full_table_name = f"{db_name}.{schema}.{table_name}"

                # Compile a regex for each schema only once.
                if schema not in schema_pattern_cache:
                    # Example pattern: r"\bmy_schema\."
                    schema_pattern = re.compile(rf"\b{re.escape(schema)}\.")
                    schema_pattern_cache[schema] = schema_pattern

                # Replace "schema." with "dbName.schema."
                # We assume the statements are well-formed and mention the table as "schema.table".
                updated_stmt = schema_pattern_cache[schema].sub(
                    f"{db_name}.{schema}.", create_stmt
                )

                # Add in an IF NOT EXISTS clause
                if not re.search(r"IF\s+NOT\s+EXISTS", updated_stmt, re.IGNORECASE):
                    # Insert "IF NOT EXISTS" after "CREATE TABLE"
                    updated_stmt = re.sub(
                        r"^(CREATE\s+TABLE\s+)",
                        r"\1IF NOT EXISTS ",
                        updated_stmt,
                        flags=re.IGNORECASE,
                    )

                # If this is the first time seeing this table, remember its order.
                if full_table_name not in seen_tables:
                    order_of_tables.append(full_table_name)
                    seen_tables.add(full_table_name)

                # Always store the most recent create statement for this table.
                table_to_statement[full_table_name] = updated_stmt

        # Prepare final statements in the order each table was first encountered.
        statements = [table_to_statement[t] for t in order_of_tables]
        count = len(statements)

    all_schemas = {}
    mapping = {}

    # Write results to the output file.
    if not anonymize:
        with open(output_path, mode="w", encoding="utf-8") as out_file:
            # Prepend the CREATE DATABASE statement.
            out_file.write(f"create database if not exists {db_name};\n\n")

            # Append each create statement followed by a semicolon and extra newline.
            for stmt in statements:
                out_file.write(stmt + ";\n\n")
                schema = parse_ddl(stmt)
                table_name = schema.table_name.split(".")[-1]
                all_schemas[table_name] = schema
    else:
        # Anonymize the schemas before writing them out to the file

        # Generate a database alias
        # FIXME: hard coding this to "db" always, as other strings didn't seem
        # to work. More investigation needed here.
        db_alias = "db"

        for stmt in statements:
            schema = parse_ddl(stmt)
            table_name = schema.table_name.split(".")[-1]
            all_schemas[table_name] = schema

        anon, mapping, anon_db_name = anonymize_schema(all_schemas, db_alias)

        # Process all statements at once
        anonymized_statements = anonymize_multiple_tables(statements, mapping)

        # Write all results to the file
        with open(output_path, mode="w", encoding="utf-8") as out_file:
            out_file.write(f"create database if not exists {anon_db_name};\n\n")

            for anon_stmt in anonymized_statements:
                if not anon_stmt.startswith("Error:"):
                    out_file.write(anon_stmt + "\n\n")
                    # Re-write meta-data to be anonymized
                    schema = parse_ddl(anon_stmt)
                    table_name = schema.table_name.split(".")[-1]
                    all_schemas[table_name] = schema
                else:
                    # Handle the error case
                    print(f"Error for statement: {anon_stmt}")

    if cluster_url:
        print(f"Attempting to create schema on cluster")
        subprocess.run(
            ["cockroach", "sql", "--url", cluster_url, "-f", output_path], check=True
        )

    print(f"Successfully wrote {count} create statements to {output_path}")
    return all_schemas, mapping


import uuid


def anonymize_schema(schema_dict, db_alias):
    """
    Anonymize a schema dictionary where keys are table names and values are TableSchema objects.
    Uses fully qualified table names in the mapping.

    Args:
        schema_dict: A dictionary where keys are table names and values are TableSchema objects
        db_alias: Alias for anonymized database

    Returns:
        tuple: (anonymized_schema_dict, mapping_dict, db_name)
            - anonymized_schema_dict: Dictionary with anonymized table names and schema structures
            - mapping_dict: A nested dictionary mapping original names to anonymized names
            - db_name: The anonymized database name
    """
    # Create anonymized database name
    db_name = db_alias

    # Initialize mapping dictionary with a structure that preserves table-column relationships
    mapping = {
        "database": db_name,  # Store the anonymized database name
        "tables": {},  # Maps original table names to anonymized names
        "columns": {},  # Will contain table-specific column mappings
    }

    # Initialize result dictionary
    anonymized_schema_dict = {}

    # Counter for table and column anonymization
    table_counter = 1

    # Process each table in the dictionary
    for original_table_name, table_schema in schema_dict.items():
        # Get the fully qualified table name from the schema
        fully_qualified_name = table_schema.table_name

        # Extract database name if present in fully qualified name
        parts = fully_qualified_name.split(".")
        if len(parts) >= 3:  # Pattern: db.schema.table
            original_db = parts[0]
            schema_name = parts[1]
            table_bare_name = parts[2]
        elif len(parts) == 2:  # Pattern: schema.table
            original_db = None
            schema_name = parts[0]
            table_bare_name = parts[1]
        else:  # Pattern: table
            original_db = None
            schema_name = "public"
            table_bare_name = fully_qualified_name

        # Store original database name if found
        if original_db and "original_database" not in mapping:
            mapping["original_database"] = original_db

        # Generate anonymized table name
        anonymized_table = f"{db_name}.public.t{table_counter}"

        # Store both the key and the fully qualified name in the mapping
        mapping["tables"][original_table_name] = anonymized_table
        mapping["tables"][fully_qualified_name] = anonymized_table

        # Create a table-specific column mapping using both table name versions
        mapping["columns"][original_table_name] = {}
        mapping["columns"][fully_qualified_name] = {}

        # Start column counter for this table
        column_counter = 1

        # Create a new TableSchema with the anonymized table name
        anonymized_table_schema = TableSchema(anonymized_table)

        # Process columns
        for col_name, column in table_schema.columns.items():
            # Generate anonymized column name
            anonymized_column = f"c{column_counter}"

            # Store mapping with table context (for both table name versions)
            mapping["columns"][original_table_name][col_name] = anonymized_column
            mapping["columns"][fully_qualified_name][col_name] = anonymized_column
            column_counter += 1

            # Create a new Column with anonymized name but same properties
            new_col = Column(
                name=anonymized_column,
                col_type=column.col_type,
                is_nullable=column.is_nullable,
                is_primary_key=column.is_primary_key,
            )

            # Add column to the anonymized schema
            anonymized_table_schema.add_column(new_col)

        # Process primary keys
        anonymized_primary_keys = []
        for pk in table_schema.primary_keys:
            if pk in mapping["columns"][original_table_name]:
                anonymized_primary_keys.append(
                    mapping["columns"][original_table_name][pk]
                )

        # Set primary keys in the anonymized schema
        anonymized_table_schema.set_primary_keys(anonymized_primary_keys)

        # Add the anonymized table schema to the result dictionary
        anonymized_schema_dict[anonymized_table] = anonymized_table_schema

        # Increment table counter for next table
        table_counter += 1

    return anonymized_schema_dict, mapping, db_name


def anonymize_create_table(
    create_statement, mapping, constraint_mapping=None, index_mapping=None
):
    """
    Anonymize a CREATE TABLE statement using the provided mapping structure.
    Also anonymizes constraint and index names.

    Args:
        create_statement: A string containing the CREATE TABLE statement
        mapping: The mapping dictionary with anonymized table and column names
        constraint_mapping: Optional dict tracking constraint names across tables
        index_mapping: Optional dict tracking index names across tables

    Returns:
        tuple: (anonymized_statement, updated_constraint_mapping, updated_index_mapping)
            - anonymized_statement: The anonymized CREATE TABLE statement
            - updated_constraint_mapping: Updated constraint mapping dictionary
            - updated_index_mapping: Updated index mapping dictionary
    """
    import re

    # Initialize constraint and index mappings if not provided
    if constraint_mapping is None:
        constraint_mapping = {"counter": 1, "names": {}}
    if index_mapping is None:
        index_mapping = {"counter": 1, "names": {}}

    # First, extract the table name - handle qualified names with dots
    table_pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)", re.IGNORECASE
    )
    table_match = table_pattern.search(create_statement)

    if not table_match:
        return (
            "Error: Could not find table name in CREATE TABLE statement.",
            constraint_mapping,
            index_mapping,
        )

    original_table = table_match.group(1).strip('"').strip("`")

    # Check if the table exists in our mapping
    if original_table not in mapping["tables"]:
        # Try to find a matching table by comparing the end of fully qualified names
        found = False
        for mapped_table in mapping["tables"].keys():
            if mapped_table.endswith("." + original_table) or original_table.endswith(
                "." + mapped_table
            ):
                original_table = mapped_table
                found = True
                break

        if not found:
            return (
                f"Error: Table '{original_table}' not found in the mapping.",
                constraint_mapping,
                index_mapping,
            )

    # Get the anonymized table name
    anonymized_table = mapping["tables"][original_table]

    # Replace the table name in the statement
    anonymized_statement = table_pattern.sub(
        f"CREATE TABLE IF NOT EXISTS {anonymized_table}", create_statement
    )

    # Find the position of the opening and closing parentheses
    try:
        # Find the opening parenthesis after CREATE TABLE
        open_paren_index = anonymized_statement.index("(")

        # Find the matching closing parenthesis by counting
        close_paren_index = -1
        paren_level = 1
        for i in range(open_paren_index + 1, len(anonymized_statement)):
            if anonymized_statement[i] == "(":
                paren_level += 1
            elif anonymized_statement[i] == ")":
                paren_level -= 1
                if paren_level == 0:
                    close_paren_index = i
                    break

        if close_paren_index == -1:
            return (
                "Error: Could not find matching parentheses in CREATE TABLE statement.",
                constraint_mapping,
                index_mapping,
            )

        # Extract the content between parentheses
        content = anonymized_statement[open_paren_index + 1 : close_paren_index]

        # The rest of the statement (after the closing parenthesis)
        suffix = anonymized_statement[close_paren_index + 1 :]

        # Split into lines based on commas, accounting for nested parentheses
        lines = []
        current_line = ""
        paren_level = 0

        for char in content:
            if char == "(":
                paren_level += 1
                current_line += char
            elif char == ")":
                paren_level -= 1
                current_line += char
            elif char == "," and paren_level == 0:
                lines.append(current_line.strip())
                current_line = ""
            else:
                current_line += char

        if current_line.strip():
            lines.append(current_line.strip())

        # Now anonymize each line
        anonymized_lines = []

        if original_table in mapping["columns"]:
            column_mapping = mapping["columns"][original_table]

            for line in lines:
                line = line.strip()

                # Check if this is a constraint or index definition
                constraint_match = re.match(
                    r"^\s*CONSTRAINT\s+([^\s]+)", line, re.IGNORECASE
                )
                index_match = re.match(r"^\s*INDEX\s+([^\s]+)", line, re.IGNORECASE)

                if constraint_match:
                    # Anonymize constraint name
                    constraint_name = constraint_match.group(1)

                    # Check if it's a primary key constraint
                    is_pk = "PRIMARY KEY" in line.upper()

                    if constraint_name not in constraint_mapping["names"]:
                        if is_pk:
                            # For primary keys, use "p{num}_pkey" format
                            anonymized_constraint = (
                                f"p{constraint_mapping['counter']}_pkey"
                            )
                        else:
                            # For other constraints, use "c{num}" format
                            anonymized_constraint = f"c{constraint_mapping['counter']}"

                        constraint_mapping["names"][
                            constraint_name
                        ] = anonymized_constraint
                        constraint_mapping["counter"] += 1

                    # Replace the constraint name
                    line = re.sub(
                        r"(CONSTRAINT\s+)" + re.escape(constraint_name),
                        r"\1" + constraint_mapping["names"][constraint_name],
                        line,
                        flags=re.IGNORECASE,
                    )

                    # Also anonymize column references
                    for col_name, anon_col in column_mapping.items():
                        # Look for the column name with word boundaries
                        line = re.sub(
                            r"\b" + re.escape(col_name) + r"\b", anon_col, line
                        )

                elif index_match:
                    # Anonymize index name
                    index_name = index_match.group(1)

                    if index_name not in index_mapping["names"]:
                        anonymized_index = f"idx_id{index_mapping['counter']}"
                        index_mapping["names"][index_name] = anonymized_index
                        index_mapping["counter"] += 1

                    # Replace the index name
                    line = re.sub(
                        r"(INDEX\s+)" + re.escape(index_name),
                        r"\1" + index_mapping["names"][index_name],
                        line,
                        flags=re.IGNORECASE,
                    )

                    # Also anonymize column references
                    for col_name, anon_col in column_mapping.items():
                        # Look for the column name with word boundaries
                        line = re.sub(
                            r"\b" + re.escape(col_name) + r"\b", anon_col, line
                        )

                elif re.match(
                    r"^\s*(PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK)",
                    line,
                    re.IGNORECASE,
                ):
                    # For other constraint types without explicit naming, just anonymize column references
                    for col_name, anon_col in column_mapping.items():
                        # Look for the column name with word boundaries
                        line = re.sub(
                            r"\b" + re.escape(col_name) + r"\b", anon_col, line
                        )

                else:
                    # It's a column definition - replace just the column name at the start
                    col_match = re.match(r"^\s*([^\s]+)", line)
                    if col_match:
                        col_name = col_match.group(1).strip('"').strip("`")
                        if col_name in column_mapping:
                            anon_col = column_mapping[col_name]
                            line = re.sub(r"^\s*" + re.escape(col_name), anon_col, line)

                anonymized_lines.append(line)

        # Reconstruct the statement with the anonymized content
        anonymized_content = ",\n  ".join(anonymized_lines)
        prefix = anonymized_statement[: open_paren_index + 1]

        # Check if the suffix already contains a closing bracket and semicolon
        if not suffix.strip():
            # No suffix, add closing bracket and semicolon
            result = f"{prefix}\n  {anonymized_content}\n);"
        elif suffix.strip().startswith(";"):
            # Suffix starts with semicolon, add closing bracket
            result = f"{prefix}\n  {anonymized_content}\n){suffix}"
        elif suffix.strip().startswith(");"):
            # Suffix already has closing bracket and semicolon
            result = f"{prefix}\n  {anonymized_content}\n{suffix.strip()}"
        else:
            # Add both closing bracket and semicolon
            result = f"{prefix}\n  {anonymized_content}\n){suffix.strip()};"

        return result, constraint_mapping, index_mapping

    except Exception as e:
        return (
            f"Error processing CREATE TABLE statement: {str(e)}",
            constraint_mapping,
            index_mapping,
        )


def anonymize_multiple_tables(create_statements, mapping):
    """
    Anonymize multiple CREATE TABLE statements with consistent constraint and index naming.

    Args:
        create_statements: List of CREATE TABLE statements
        mapping: The mapping dictionary with anonymized table and column names

    Returns:
        list: List of anonymized CREATE TABLE statements
    """

    constraint_mapping = {"counter": 1, "names": {}}
    index_mapping = {"counter": 1, "names": {}}

    anonymized_statements = []

    for statement in create_statements:
        anonymized_stmt, constraint_mapping, index_mapping = anonymize_create_table(
            statement, mapping, constraint_mapping, index_mapping
        )
        anonymized_statements.append(anonymized_stmt)

    return anonymized_statements


class Column:
    def __init__(self, name, col_type, is_nullable, is_primary_key=False):
        self.name = name
        self.col_type = col_type
        self.is_nullable = is_nullable
        self.is_primary_key = is_primary_key

    def __str__(self):
        pk_flag = "PRIMARY KEY" if self.is_primary_key else ""
        null_status = "NULL" if self.is_nullable else "NOT NULL"
        return f":-:|'{self.name}','{self.col_type}','{null_status}','{pk_flag}'|:-:"


class TableSchema:
    def __init__(self, table_name):
        self.table_name = table_name
        self.columns = {}  # List of Column objects
        self.primary_keys = []  # List of primary key column names

    def add_column(self, column):
        self.columns[column.name] = column

    def set_primary_keys(self, pk_columns):
        self.primary_keys = pk_columns
        for col_name in self.columns:
            col = self.columns[col_name]
            if col.name in self.primary_keys:
                col.is_primary_key = True

    def __str__(self):
        output = f"Table: {self.table_name}\nColumns:\n"
        for col_name in self.columns:
            output += str(self.columns[col_name]) + "\n"
        output += f"Primary Keys: {', '.join(self.primary_keys) if self.primary_keys else 'None'}\n"
        return output


def parse_ddl(ddl):
    """
    Parses a CREATE TABLE statement and extracts:
    - Table name
    - Column names, types, and nullability constraints
    - Primary key columns
    """

    # Extract table name (with schema if present)
    table_match = re.search(
        r'CREATE TABLE IF NOT EXISTS\s+([\w."]+)', ddl, re.IGNORECASE
    )
    if not table_match:
        raise ValueError("Invalid DDL: Could not find table name.")

    table_name = table_match.group(1).replace('"', "")  # Remove quotes
    schema = TableSchema(table_name)

    # Extract only column definitions (ignore constraints like PRIMARY KEY, FOREIGN KEY, INDEX)
    column_definitions = re.findall(
        r'^\s*"?(.*?)"?\s+([\w,()]+)\s+(NULL|NOT NULL)?', ddl, re.MULTILINE
    )

    for col in column_definitions:
        column_name = col[0]
        column_type = col[1]
        is_nullable = col[2] is None or col[2].upper() == "NULL"
        if column_name.upper() not in [
            "CONSTRAINT",
            "UNIQUE",
            "PRIMARY",
            "FOREIGN",
            "INDEX",
            "CREATE",
        ]:
            schema.add_column(Column(column_name, column_type, is_nullable))

    # Extract PRIMARY KEY constraint
    pk_match = re.search(r"PRIMARY KEY\s*\((.*?)\)", ddl, re.IGNORECASE)
    if pk_match:
        pk_columns = [col.strip().split()[0] for col in pk_match.group(1).split(",")]
        schema.set_primary_keys(pk_columns)

    return schema
