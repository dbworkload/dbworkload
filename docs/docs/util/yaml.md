# yaml

## Generate YAML data generation file from a DDL SQL file

`dbworkload` can assist with generating a stub of the _data generation definition_ file, required by the [`csv`](./csv.md) command.

### Example

1. Create a DDL file. Let's call it `bank.ddl`.

    ```sql
    -- file: bank.ddl

    CREATE TABLE ref_data (
        acc_no BIGINT PRIMARY KEY,
        external_ref_id UUID,
        created_time TIMESTAMPTZ,
        acc_details VARCHAR
    );

    CREATE TABLE orders (
        acc_no BIGINT NOT NULL,
        id UUID NOT NULL default gen_random_uuid(),
        status VARCHAR NOT NULL,
        amount DECIMAL(15, 2),
        ts TIMESTAMPTZ default now(),
        CONSTRAINT pk PRIMARY KEY (acc_no, id)
    );
    ```

2. Run the `yaml` command, passing in `bank.ddl` as the input.

    ```bash
    dbworkload util yaml -i bank.ddl
    ```

3. Open `bank.yaml` output and inspect YAML tree.
    This is a configuration file with instructions on what type of data to generate.
    Check section [Types](#types) for a full list of available object types and arguments.

    ```yaml
    ref_data:
      - count: 100
        sort-by: []
        columns:
          acc_no:
            type: integer
            args:
              min: -9223372036854775807
              max: 9223372036854775807
              seed: 64
              null_pct: 0.3
              array: 0
          external_ref_id:
            type: uuid
            args:
              seed: 76
              null_pct: 0.25
              array: 0
          created_time:
            type: timestamp
            args:
              start: "2000-01-01"
              end: "2024-12-31"
              format: "%Y-%m-%d %H:%M:%S.%f"
              seed: 78
              null_pct: 0.37
              array: 0
          acc_details:
            type: string
            args:
              min: 10
              max: 30
              prefix: ""
              seed: 54
              null_pct: 0.38
              array: 0
    orders:
      - count: 100
        sort-by: []
        columns:
          acc_no:
            type: integer
            args:
              min: -9223372036854775807
              max: 9223372036854775807
              seed: 83
              null_pct: 0.0
              array: 0
        id:
          type: uuid
          args:
            seed: 49
            null_pct: 0.0
            array: 0
        status:
          type: string
          args:
            min: 10
            max: 30
            prefix: ""
            seed: 58
            null_pct: 0.0
            array: 0
        amount:
          type: float
          args:
            min: 0
            max: 10000000000000
            round: 2
            seed: 11
            null_pct: 0.33
            array: 0
        ts:
          type: timestamp
          args:
            start: "2000-01-01"
            end: "2024-12-31"
            format: "%Y-%m-%d %H:%M:%S.%f"
            seed: 100
            null_pct: 0.26
            array: 0
    ```

## Types

Below is a table with all available generators and their arguments

| Generator | Description             | Arguments              | Default        |
|-----------|-------------------------|------------------------|----------------|
| constant  | Returns `value` over and over             | value `str`            |"simplefaker"   |
| sequence  | Returns an `int` increased by 1 starting from `start` | start `int`            | 0              |
| integer   | Returns an `int` between `min` and `max`         | min `int`              | 1              |
|           |                         | max `int`              | 1,000,000,000  |
| float     | Returns a decimal between `min` and `max` with `round` precision  | min `int`              | 1              |
|           |                         | max `int`              | 1,000,000      |
|           |                         | round `int`            | 2              |
| string    | Returns a `str` using chars `[A-Za-z0-9]` of length between `min` and `max` prefixed by `prefix` | min `int`              | 10             |
|           |                         | max `int`              | 50             |
|           |                         | prefix `str`           | <blank\>       |
| json      | Returns a JSON string of length between `min` and `max`         | min `int`              | 10             |
|           |                         | max `int`              | 50             |
| choice    | Picks an item `population`. Optionally set `[cum_]weights`. See [docs](https://docs.python.org/3/library/random.html#random.choice) | population `list[str]` | ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"] |
|           |                         | weights  `list[int]`   | empty list     |
|           |                         | cum_weights `list[int]`| empty list     |
| timestamp | Returns a random timestamp formatted as `format` between `start` and `end`.                        | start `str`            | "2000-01-01"   |
|           | `start` and `end` must be valid date representation                        | end `str`              | "2024-12-31"   |
|           | [strftime quick ref](https://strftime.org/)                        | format `str`           | "%Y-%m-%d %H:%M:%S.%f"|
| date      |                          | start `str`            | "2000-01-01"   |
|           |                         | end `str`              | "2024-12-31"   |
|           |                         | format `str`           | "%Y-%m-%d"     |
| time      |                         | start `str`            | "07:30:00"     |
|           |                         | end `str`              | "22:30:00"     |
|           |                         | micros `bool`          | false          |
| uuid      | Returns the string representation of a UUIDv4    |                        |                |
| bool      | Returns either 0 or 1          |                        |                |
| bit       | Returns a `bit` of size `size` | size `int`             | 10             |
| bytes     | Returns `bytes` of size `size` | size `int`             | 10             |
| custom    | Uses a python class defined in `path` | path `str` | |
|           | See [example usage](#custom-type-generator)          | | |

Furthermore, all but `sequence` take these common arguments.

| Arguments              | Description | Default        |
| -----------------------|-------------|----------------|
| seed `float`           | The [random generator](https://docs.python.org/3/library/random.html#random.Random) seed number | random         |
| null_pct `float`       | The percentange of NULL values, currently defined as an empty string `""` | 0              |
| array `int`            | Size of the ARRAY | 0              |

`json` does not take `array`.

`constant` does not take `seed` and `array` since it returns the same value.

### Custom type generator

With a custom generator, you can define your own logic to generate any type of random data.

The custom generator is nothing more than a python class that creates a python generator,
that is, it implements the `__next__()` function.

Below example illustrate how it works.

In the YAML file, you must pass `path` in the `args` section,
with the path to your python file.

Optionally, you can pass a `seed` and you can declare as many `args` as you want.
In this example, we only declare 1 arg, `my_arg`.

```yaml
my_table:
- count: 5
  sort-by: []
  columns:
    my_custom_generator:
      type: custom
      args:
        seed: 1
        path: play/greetings.py
        my_arg: Hello
```

The file `play/greetings.py` yields a generated value using your arbitrarily complex logic

```python
import random


class Greetings:
    def __init__(
        self,
        seed: float,
        my_arg: str = "",
        null_pct: float = 0,
        array: int = 0,
    ):
        self.array = array
        self.null_pct = null_pct
        self.rng: random.Random = random.Random(seed)
        self.my_arg = my_arg
        self.names = ['Joe', 'Xin', 'Sue', 'Mia', 'Ram']

    def __next__(self):
        if self.null_pct and self.rng.random() < self.null_pct:
            return ""
        else:
            return f"{self.my_arg} {self.rng.choice(self.names)}!"

```

Executing `dbworkload util csv` returns these rows

```text
Hello Ram!
Hello Xin!
Hello Joe!
Hello Ram!
Hello Mia!
```

The file name must match the class name: file `greetings.py` declares class `Greetings`.

The `__init__()` function must accept `seed`, `null_pct` and `array` as arguments.
You don't have to use them if you don't want to, but they are expected. 

## YAML Structure

The structure of the YAML file might look intuitive already, but here is a formal description.

For every `CREATE TABLE` statement in the input file, you'll find a key with the same name

```yaml hl_lines="1 6"
ref_data:
  - count: 100
    sort-by: []
    columns:
    # truncated for brevity
orders:
  - count: 100
    sort-by: []
    columns:
    # truncated for brevity
```

The value type of the table key is a **list**: notice the `-` prefixing the first item of the list, `count`.

This is helpful if you want to generate detasets with different seed numbers for the same table,
as shown in [an example below](#foreign-key-relationship).
Mostly though, the list will contain only 1 item.

The items in the list are of type dict with 3 keys:

- `count`: takes an `int` as a value and represents the desired count of rows in the dataset you want to generate.
- `sort-by`: takes a list of `str`. Here you pass the exact column names you want to sort by, in ascending order.
- `columns`: takes a dict as value. The dict uses the column names in the `CREATE TABLE` as keys.

Here's an example of generating 1000 rows sorted by `acc_no`, a column of type `integer` with default `args`.

```yaml hl_lines="2-4 6"
ref_data:
  - count: 1000
    sort-by:
      - acc_no
    columns:
      acc_no:
        type: integer
        # notice the absence of the `args` key
```

Each key in `columns` has a dict as value with 2 keys:

- `type`: the generator type you want to use.
- `args`: yet another dictionary with the specific generator arguments and values.

Here is an example of column `external_ref_id`, which features all available arguments for its type.

```yaml hl_lines="9-13"
ref_data:
  - count: 1000
    sort-by:
      - acc_no
    columns:
      acc_no:
        type: integer
      external_ref_id:
        type: uuid
        args:
          seed: 76
          null_pct: 0.25
          array: 0
```

See [Types](#types) right above for a full list of all available generator types and their arguments.

## Foreign Key relationship

A common scenario is to create datasets that must be linked, for example, because of a Foreign Key constraint in the table schemas.

Here is a quick example of how to use `seed` to generate same keys across 2 columns in different tables.

Example tables linked by a foreign key reletionship:

```sql hl_lines="12"
CREATE TABLE p (
    id UUID,
    v STRING,
    CONSTRAINT pk PRIMARY KEY (id)
);

CREATE TABLE c (
    id UUID,
    p_id UUID NOT NULL,
    v STRING,
    CONSTRAINT pk PRIMARY KEY (id),
    CONSTRAINT pid_in_p FOREIGN KEY (p_id) REFERENCES p (id)
);
```

Create a YAML file so that the data generated for the `p.id` column matches `c.p_id`.
Notice how the `seed` number, 1, is the same for table `p` and some rows in `c`.

```yaml hl_lines="8 20"
p:
  - count: 10
    sort-by: []
    columns:
      id:
        type: uuid
        args:
          seed: 1
      v:
        type: string
c:
- count: 2
  sort-by: []
  columns:
    id:
      type: uuid
    p_id:
      type: uuid
      args:
        seed: 1
    v:
      type: string
- count: 8
  sort-by: []
  columns:
    id:
      type: uuid
    p_id:
      type: uuid
    v:
      type: string
```

For table `p`, this will create:

- a 10 rows dataset.

For table `c`, it will create:

- a 2 rows dataset where `c.p_id` is the same as `p.id`;
- a 8 rows dataset with random rows.

That gives a FK relationship of 2:10 between `c` and `p`.

```bash hl_lines="3 4"
# file: p.0_0_0.tsv
 
19578af7-2a49-4cdc-95ff-6a05e79ff16b    Tp5zbRSFtugZQsYJEIERXjLrozw
877c814b-66b2-48f3-8d2e-80cbbd74cdb7    NGFfjmbegU7n
ceff7bf0-0dde-4c54-b3f2-1bcf7a7fb908    4zAUXFkiHGTOlvvHk6bq
c3aba496-5410-41e6-8d48-8da450d302fc    OsKz6zG3dPYvEcqBMltDQGqrfpVUnSbsBeiH
071f951d-e4ae-4c90-9b64-eb57a915b273    OCWXgMcnpNBaODfEXRCHiHqjqD2ZGn
e277823f-c19a-497f-8582-f29ef3fb9957    vpC0wqGT53vBkwgGUNrTtw6pbLsV3PEy3BnsqEASk0IWj0pQbI
bbe37e2c-d874-4ca2-8978-4463078e4206    ksMTt13ZFXVmnt41hnjvDtdJyoZ
32ad9dbf-0a25-463e-ba1f-f248edf3a62a    QtLnVqUoUOauO
fa98f2c6-00b9-4570-b36c-9673225a7afd    yREqhAzY1YBx3cQ990
ba66edaf-04e7-4217-beb2-671a8faa1530    rv14QnjStbeUjtpIAdAeVuIvETSj
```

Below the 2 rows CSV generated for table `c`.
The 2nd column, `p_id`, has matching values to the first column in above file.

```bash
# file: cat c.0_0_0.tsv 
 
5d037e2b-ce8f-4e7d-a99e-4f8efe6349f9    19578af7-2a49-4cdc-95ff-6a05e79ff16b    Q1OYAmNzDJHKrpCChkyzNQE7wuruIa8Hkb
ec8d601c-2500-4f6e-9fc6-697634f4bcd8    877c814b-66b2-48f3-8d2e-80cbbd74cdb7    VAY6PpDTDKxaDBdyVK6W02fMM6Eko492fe3pXTf9JEMgA
```

## See also

- [`dbworkload util yaml`](../cli.md#dbworkload-util-yaml)
- [Seed the database tables](../getting_started/2.md)
