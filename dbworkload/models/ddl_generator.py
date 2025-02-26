import os
import csv
import re

def Generate_ddls(zip_content_location, db_name, output_file_location, cluster_url):
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
        raise FileNotFoundError(f"Could not find TSV file: "+ file_path)

    output_path = os.path.join(output_file_location, f"{db_name}.schema.sql")

    with open(file_path, mode="r", newline="", encoding="utf-8") as tsv_file:
        # Use the csv reader for tab-separated data.
        # If your data has embedded quotes/tabs/newlines, you may need more robust settings.
        reader = csv.reader(tsv_file, delimiter='\t', quotechar='"')
        
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
            if (record[column_index["database_name"]] == db_name and
                    record[column_index["descriptor_type"]] == "table" and
                    record[column_index["schema_name"]] == "public"):
                
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
                if not re.search(r'IF\s+NOT\s+EXISTS', updated_stmt, re.IGNORECASE):
                   # Insert "IF NOT EXISTS" after "CREATE TABLE"
                   updated_stmt = re.sub(
                   r'^(CREATE\s+TABLE\s+)',
                   r'\1IF NOT EXISTS ',
                   updated_stmt,
                   flags=re.IGNORECASE
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
    # Write results to the output file.
    with open(output_path, mode="w", encoding="utf-8") as out_file:
        # Prepend the CREATE DATABASE statement.
        out_file.write(f"CREATE DATABASE IF NOT EXISTS {db_name};\n\n")

        # Append each create statement followed by a semicolon and extra newline.
        for stmt in statements:
            out_file.write(stmt + ";\n\n")
            schema = parse_ddl(stmt)
            table_name = schema.table_name.split('.')[-1]
            all_schemas[table_name]=schema

    print(f"Successfully wrote {count} create statements to {output_path}")
    return all_schemas

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
    table_match = re.search(r'CREATE TABLE IF NOT EXISTS\s+([\w."]+)', ddl, re.IGNORECASE)
    if not table_match:
        raise ValueError("Invalid DDL: Could not find table name.")
    
    table_name = table_match.group(1).replace('"', '')  # Remove quotes
    schema = TableSchema(table_name)

    # Extract only column definitions (ignore constraints like PRIMARY KEY, FOREIGN KEY, INDEX)
    column_definitions = re.findall(r'^\s*"?(.*?)"?\s+([\w,()]+)\s+(NULL|NOT NULL)?', ddl, re.MULTILINE)

    for col in column_definitions:
        column_name = col[0]
        column_type = col[1]
        is_nullable = col[2] is None or col[2].upper() == "NULL"
        if column_name.upper() not in ["CONSTRAINT", "UNIQUE", "PRIMARY", "FOREIGN", "INDEX", "CREATE"]:
            schema.add_column(Column(column_name, column_type, is_nullable))

    # Extract PRIMARY KEY constraint
    pk_match = re.search(r'PRIMARY KEY\s*\((.*?)\)', ddl, re.IGNORECASE)
    if pk_match:
        pk_columns = [col.strip().split()[0] for col in pk_match.group(1).split(",")]
        schema.set_primary_keys(pk_columns)

    return schema
