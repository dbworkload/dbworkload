import psycopg
import random
import time
from uuid import uuid4
from collections import deque


class Bank:
    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag

        self.think_time: float = float(args.get("think_time", 5) / 1000)

        # Percentage of read operations compared to order operations
        self.read_pct: float = float(args.get("read_pct", 0) / 100)

        # initiate deque with 1 random UUID so a read won't fail
        self.order_tuples = deque([(0, uuid4())], maxlen=10000)

        # keep track of the current account number and id
        self.account_number = 0
        self.id = uuid4()

        # translation table for efficiently generating a string
        # -------------------------------------------------------
        # make translation table from 0..255 to A..Z, 0..9, a..z
        # the length must be 256
        self.tbl = bytes.maketrans(
            bytearray(range(256)),
            bytearray(
                [ord(b"a") + b % 26 for b in range(113)]
                + [ord(b"0") + b % 10 for b in range(30)]
                + [ord(b"A") + b % 26 for b in range(113)]
            ),
        )

    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        with conn.cursor() as cur:
            print(
                f"My thread ID is {id}. The total count of threads is {total_thread_count}"
            )
            print(cur.execute(f"select version()").fetchone()[0])

    # the loop() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, loop() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):
        if random.random() < self.read_pct:
            return [self.txn_read]
        return [self.txn_new_order, self.txn_order_exec]

    #####################
    # Utility Functions #
    #####################
    def __think__(self, conn: psycopg.Connection):
        time.sleep(self.think_time)

    def random_str(self, size: int = 12):
        return (
            random.getrandbits(8 * size)
            .to_bytes(size, "big")
            .translate(self.tbl)
            .decode()
        )

    # Workload function stubs

    def txn_read(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM orders
                WHERE acc_no = %s
                  AND id = %s
                """,
                random.choice(self.order_tuples),
            ).fetchall()

    def txn_new_order(self, conn: psycopg.Connection):
        # generate a random account number to be used for
        # for the order transaction
        self.account_number = random.randint(0, 999)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (acc_no, status, amount)
                VALUES (%s, 'Pending', %s) RETURNING id
                """,
                (
                    self.account_number,
                    round(random.random() * 1000000, 2),
                ),
            )

            # save the id that the server generated
            self.id = cur.fetchone()[0]

            # save the (acc_no, id) tuple to our deque list
            # for future read transactions
            self.order_tuples.append((self.account_number, self.id))

    def txn_order_exec(self, conn: psycopg.Connection):
        # with Psycopg, this is how you start an explicit transaction
        with conn.transaction() as tx:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM ref_data
                    WHERE acc_no = %s
                    """,
                    (self.account_number,),
                ).fetchall()

                # simulate microservice doing something...
                time.sleep(0.02)

                cur.execute(
                    """
                    UPDATE orders
                    SET status = 'Complete'
                    WHERE 
                        (acc_no, id) = (%s, %s)
                    """,
                    (
                        self.account_number,
                        self.id,
                    ),
                )
