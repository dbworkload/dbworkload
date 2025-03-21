# gen_stub

## Generating Python stub files

`dbworkload` requires a Python file that will be the foundation of the workload. In addition to setup and utility functions, this `.py` file will also require your SQL transactions in order to mirror the workload you want to re-create.

Each SQL transaction will be contained within its own python function. We can speed up creation of these functions via the `gen_stub` command, which accepts a `.sql` file as an argument.  

This page will only cover the stub functions generated, [you can read more about the rest of the workload class here.](../getting_started/3.md)

### Example

1. Create a `.sql` file with various SQL statements. Let's call it `bank.sql`.

    ```sql
    select * from t1 where id = 123;

    INSERT INTO t2 values (%s,%s);

    DELETE FROM 
        t3 where 
        id = %s returning *;

    SELECT now();
    ```

2. Run the `gen_stub` command, passing in `bank.sql` as the input.

    ```bash
    dbworkload util gen_stub -i bank.sql
    ```

3. Open `bank.py` output and inspect function stubs. Notice one function per statement.

    ```python
    .
    .
    setup() & utility functions
    .
    .

    # Workload function stubs
        
        def txn_0(self, conn: psycopg.Connection):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM t1
                    WHERE id = 123
                    """,
                    (
                        
                    ), 
                ).fetchall()
        
        def txn_1(self, conn: psycopg.Connection):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO t2
                    VALUES (%s,%s)
                    """,
                    (
                        # add bind parameter, 
                        # add bind parameter, 
                        
                    ), 
                )
        
        def txn_2(self, conn: psycopg.Connection):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE
                    FROM t3
                    WHERE id = %s RETURNING *
                    """,
                    (
                        # add bind parameter, 
                        
                    ), 
                ).fetchall()
        
        def txn_3(self, conn: psycopg.Connection):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT now()
                    """,
                    (
                        
                    ), 
                ).fetchall()
    ```

4. View helpful tips at the bottom of the file.

    ```python
    '''
    # Quick random generators reminder

    # random string of 25 chars
    self.random_str(25),

    # random int between 0 and 100k
    random.randint(0, 100000),

    # random float with 2 decimals 
    round(random.random()*1000000, 2)

    # now()
    dt.datetime.utcnow()

    # random timestamptz between certain dates,
    # expressed as unix ts
    dt.datetime.fromtimestamp(random.randint(1655032268, 1759232268))

    # random UUID
    uuid4()

    '''
    ```

5. Edit the workload

    Now that we have a stub and some hints, it's likely we'll need to make some edits and tune the workload to make it look more like what we want. Further reading on editing the workload can be found here: [Edit the workload](../getting_started/4.md)

## See also

- [`dbworkload util gen_stub`](../cli.md#dbworkload-util-gen_stub)
- [Create the workload class](../getting_started/3.md)
- [Edit the workload](../getting_started/4.md)
