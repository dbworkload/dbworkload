import re
import random
import sqlparse
import datetime as dt
from typing import List, Dict, Tuple, Optional

def replace_placeholders(sql, all_schemas):
    """
    Replace placeholders in an SQL query:
    - `_` -> `%s`
    - `__more__` -> Expands based on the number of fields in VALUES (...) or IN (...)

    Returns the modified query.
    """

    table_names = extract_table_names(sql)
    if table_names is None:
        return sql
    schemas = [all_schemas[table_name] for table_name in table_names]

    def replace_values(values_match):
        values_content = values_match.group(1)  # Extract content inside VALUES(...)
        fields = [
            f.strip() for f in values_content.split(",")
        ]  # Split by comma and trim spaces

        expanded_values = []
        for field in fields:
            expanded_values.append(get_field_column(schemas, field))

        # Generate the correct VALUES clause
        placeholders = ", ".join(expanded_values)
        return f"({values_match.group(1)}){values_match.group(2)} ({placeholders}){values_match.group(4)}"

    # Replace the entire VALUES(...) with corrected placeholders
    sql = re.sub(
        r"\((.*)\)(.*VALUES\s*)\((.*)\)(\s+RETURNING|\s*;|$)",
        replace_values,
        sql,
        flags=re.IGNORECASE,
    )

    sql = replace_tokens(sql, schemas)
    sql = extract_set_conditions(sql, schemas)
    sql = extract_system_time(sql)
    return sql


def replace_tokens(sql, schemas):
    # Parse the query
    query = sqlparse.parse(sql)[0]
    statement = ""
    query_index = 0
    # Extract where conditions from the query
    while query_index < len(query.tokens):
        token = query.tokens[query_index]
        if token.value == "SELECT":
            query_index += 1
            statement += token.value
            while (
                query_index < len(query.tokens)
                and query.tokens[query_index].is_whitespace
            ):
                query_index += 1
                statement += " "  # exhausted all whitespaces
            statement += process_select_statements(query.tokens[query_index], schemas)
            query_index += 1
        elif type(token) == sqlparse.sql.Where:
            where_statement, query_index = process_where_token(
                token, schemas, query_index
            )
            statement += where_statement
        elif type(token) == sqlparse.sql.Token and token.value.strip() == "LIMIT":
            query_index += 1
            statement += token.value
            while (
                query_index < len(query.tokens)
                and query.tokens[query_index].is_whitespace
            ):
                query_index += 1
                statement += " "  # exhausted all whitespaces
            query_index += 1
            statement += str(random.randint(1, 100))
        else:
            query_index += 1
            statement += token.value
    return statement


def process_select_statements(token, schemas):
    statement = ""
    if isinstance(token, sqlparse.sql.IdentifierList):
        index = 0
        while index < len(token.tokens):
            if token.tokens[index].value.startswith("IFNULL"):
                pattern = r"(,\s*)_(\s*\))"
                match = re.search(
                    r"IFNULL\s*\(\s*sum\s*\(\s*([^)]+?)\s*\)\s*,",
                    token.tokens[index].value,
                )
                if match:
                    column_name = match.group(1).strip()
                    token.tokens[index].value = re.sub(
                        pattern,
                        r"\1" + get_field_column(schemas, column_name) + r"\2",
                        token.tokens[index].value,
                    )
            statement += token.tokens[index].value
            index += 1
    else:
        statement = token.value
    return statement


def process_where_token(where_token, schemas, query_index):
    parts = []
    index = 1  # Skip the initial "WHERE" keyword token
    while index < len(where_token.tokens):
        condition, index = extract_where_condition(where_token.tokens, index, schemas)
        parts.append(condition)
    return where_token.tokens[0].value + "".join(parts), query_index + 1


def extract_where_condition(tokens, idx, schemas):
    token = tokens[idx]
    if isinstance(token, sqlparse.sql.Identifier):
        return process_identifier_token(tokens, idx, schemas)
    elif token.ttype is not None and (
        token.value in [" ", "(", ")"] or token.is_keyword
    ):
        return token.value, idx + 1
    elif isinstance(token, sqlparse.sql.Comparison):
        return process_comparison_token(token, schemas), idx + 1
    elif isinstance(token, sqlparse.sql.Parenthesis):
        return process_parenthesis_token(token, tokens, idx, schemas)
    else:
        # Fallback for any other token types
        return token.value, idx + 1


def process_identifier_token(tokens, idx, schemas):
    token = tokens[idx]
    condition = token.value
    new_index = idx + 1
    while new_index < len(tokens) and tokens[new_index].is_whitespace:
        condition += " "
        new_index += 1
    if new_index < len(tokens) and tokens[new_index].value.strip().upper() == "IN":
        condition += tokens[new_index].value
        new_index += 1
        while new_index < len(tokens) and tokens[new_index].is_whitespace:
            condition += " "
            new_index += 1
        if (
            new_index < len(tokens)
            and isinstance(tokens[new_index], sqlparse.sql.Parenthesis)
            and len(tokens[new_index].tokens) > 1
            and isinstance(tokens[new_index].tokens[1], sqlparse.sql.IdentifierList)
        ):
            condition += f"({get_field_column(schemas, token.value)}, {get_field_column(schemas, token.value)})"
            return condition, new_index + 1
        elif (
            new_index < len(tokens)
            and isinstance(tokens[new_index], sqlparse.sql.Parenthesis)
            and len(tokens[new_index].tokens) > 1
            and tokens[new_index].tokens[1].is_keyword
        ):
            return (
                f"{condition}({replace_tokens(tokens[new_index].value[1:-1], schemas)})",
                new_index + 1,
            )

    if new_index < len(tokens) and tokens[new_index].value.strip().upper() == "BETWEEN":
        condition += tokens[new_index].value
        new_index += 1
        while new_index < len(tokens) and tokens[new_index].is_whitespace:
            condition += " "
            new_index += 1
        if new_index < len(tokens) and isinstance(
            tokens[new_index], sqlparse.sql.Parenthesis
        ):
            condition += f"({get_field_column(schemas, token.value)})"
            new_index += 1
            while new_index < len(tokens) and (
                tokens[new_index].is_whitespace
                or (
                    hasattr(tokens[new_index], "value")
                    and tokens[new_index].value.strip() == "AND"
                )
            ):
                condition += tokens[new_index].value
                new_index += 1
            if new_index < len(tokens) and isinstance(
                tokens[new_index], sqlparse.sql.Parenthesis
            ):
                condition += f"({get_field_column(schemas, token.value)})"
                return condition, new_index + 1
    return condition, new_index


def process_comparison_token(token, schemas):
    key, operator, value = re.split(
        r"((?:=|<|>|<=|>=|!=|LIKE)\s*)", token.value, maxsplit=1
    )
    if "_::INTERVAL" in value:
        field = re.sub(
            r"TIMESTAMP(TZ)?", "INTERVAL", get_field_column(schemas, key.strip())
        )
        new_val = value.replace("_::INTERVAL", f"{field}::INTERVAL")
        return f"{key}{operator.strip()}{new_val}"
    elif value.startswith("(") and value.endswith(")"):
        return f"{key}{operator.strip()}({replace_tokens(value[1:-1], schemas)})"
    else:
        new_val = re.sub(
            r"(?<![a-zA-Z0-9_])_(?![a-zA-Z0-9_])",
            get_field_column(schemas, key.strip()),
            value,
        )
        return f"{key}{operator}{new_val}"


def process_parenthesis_token(token, parent_tokens, parent_index, schemas):
    if len(token.tokens) > 1 and isinstance(
        token.tokens[1], sqlparse.sql.IdentifierList
    ):
        condition = token.tokens[0].value  # Opening parenthesis
        sub_index = 1
        fields = [field.strip() for field in token.tokens[1].value.split(",")]
        condition += token.tokens[sub_index].value
        sub_index += 1
        condition += token.tokens[sub_index].value  # Add keyword after identifier list
        new_index = parent_index + 1
        while new_index < len(parent_tokens) and parent_tokens[new_index].is_whitespace:
            condition += " "
            new_index += 1
        if (
            new_index < len(parent_tokens)
            and parent_tokens[new_index].value.strip().upper() == "IN"
        ):
            condition += parent_tokens[new_index].value
            new_index += 1
        while new_index < len(parent_tokens) and parent_tokens[new_index].is_whitespace:
            condition += " "
            new_index += 1
        if new_index < len(parent_tokens) and isinstance(
            parent_tokens[new_index], sqlparse.sql.Parenthesis
        ):
            mapped_fields = ", ".join(
                f"({get_field_column(schemas, field)}, {get_field_column(schemas, field)})"
                for field in fields
            )
            condition += f"({mapped_fields})"
            return condition, new_index + 1
    else:
        index = 1
        inner_conditions = "("
        while index < len(token.tokens):
            cond, index = extract_where_condition(token.tokens, index, schemas)
            inner_conditions += cond
        return inner_conditions, parent_index + 1


def extract_system_time(sql):
    system_time = get_system_time()
    # Match WHERE clause and stop at termination keywords (ORDER BY, GROUP BY, LIMIT, etc.)
    return re.sub(
        r"(\s+OF SYSTEM TIME\s+)(_)", f" OF SYSTEM TIME '-1s'", sql, flags=re.IGNORECASE
    )


def get_system_time():
    minutes = random.randint(0, 59)
    seconds = random.randint(0, 59)
    return (
        dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(minutes=minutes, seconds=seconds)
    ).strftime("%Y-%m-%d %H:%M:%S")


def extract_set_conditions(sql, schemas):
    def replace_set_clause(set_match):
        # The SET clause captured by group(3)
        set_str = set_match.group(3).strip()

        # Check if it is in the tuple form: (field1, field2) - (value1, value2)
        tuple_form = re.match(r"^\((.*?)\)\s*[-=]\s*\((.*?)\)$", set_str)
        if tuple_form:
            fields_str = tuple_form.group(1).strip()
            values_str = tuple_form.group(2).strip()

            fields = [f.strip() for f in fields_str.split(",")]
            values = [v.strip() for v in values_str.split(",")]
            set_clause = []
            for field, value in zip(fields, values):
                new_val = get_field_column(schemas, field)
                if value is not None:
                    # Replace the value with the dynamic value from schemas (or any processing)
                    new_val = re.sub(
                        r"(?<![a-zA-Z0-9_])_(?![a-zA-Z0-9_])", new_val, value
                    )
                set_clause.append(f"{field} = {new_val}")
            set_clause_str = ", ".join(set_clause)
        else:
            # Original "field=value" format (comma-separated)
            set_clause = []
            set_pairs = [pair.strip() for pair in set_str.split(",")]
            for set_pair in set_pairs:
                kv = [x.strip() for x in set_pair.split("=")]
                if len(kv) != 2:
                    continue  # or raise an error if appropriate
                field = kv[0]
                value = kv[1]
                new_val = get_field_column(schemas, field)
                if value is not None:
                    # Replace the value with the dynamic value from schemas (or any processing)
                    new_val = re.sub(
                        r"(?<![a-zA-Z0-9_])_(?![a-zA-Z0-9_])", new_val, value
                    )
                set_clause.append(f"{field} = {new_val}")
            set_clause_str = ", ".join(set_clause)

        return f"{set_match.group(1)}{set_match.group(2)}{set_clause_str}{set_match.group(4)}"

    # Match SET clause and stop at termination keywords (WHERE, ORDER BY, GROUP BY, LIMIT, etc.)
    return re.sub(
        r"(UPDATE\s+[\w.]+)(\s+SET\s+)(.+?)(\s+WHERE|\s+ORDER BY|\s+GROUP BY|\s+LIMIT|\s*;|$)",
        replace_set_clause,
        sql,
        flags=re.IGNORECASE,
    )


def get_field_column(schemas, field):
    for schema in schemas:
        if field in schema.columns:
            return str(schema.columns[field])
    raise ValueError(f"Field '{field}' not found in any schema")


def extract_table_names(statement):
    # Regular expressions to match different SQL statements
    insert_pattern = re.compile(
        r'^INSERT INTO\s+["]?(?:[\w.]+\.)?(\w+)["]?', re.IGNORECASE
    )
    select_pattern = re.compile(
        r'^SELECT\s+.*?\s+FROM\s+["]?(?:[\w.]+\.)?(\w+)["]?', re.IGNORECASE
    )
    update_pattern = re.compile(r'^UPDATE\s+["]?(?:[\w.]+\.)?(\w+)["]?', re.IGNORECASE)
    delete_pattern = re.compile(
        r'^DELETE FROM\s+["]?(?:[\w.]+\.)?(\w+)["]?', re.IGNORECASE
    )
    join_pattern = re.compile(r'JOIN\s+["]?(?:[\w.]+\.)?(\w+)["]?', re.IGNORECASE)

    table_names = set()

    # Find all matches for each pattern
    table_names.update(insert_pattern.findall(statement))
    table_names.update(select_pattern.findall(statement))
    table_names.update(update_pattern.findall(statement))
    table_names.update(delete_pattern.findall(statement))
    table_names.update(join_pattern.findall(statement))

    return list(filter(None, table_names))
