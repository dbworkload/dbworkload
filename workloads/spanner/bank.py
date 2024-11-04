from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.transaction import Transaction
from google.cloud.spanner_v1 import param_types
import random
import time
from uuid import uuid4
from collections import deque
from base64 import encodebytes


class Bank:
    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag

        self.think_time: float = float(args.get("think_time", 5) / 1000)

        # Percentage of read operations compared to order operations
        self.read_pct: float = float(args.get("read_pct", 0) / 100)

        # initiate deque with 1 random UUID so a read won't fail
        self.order_tuples = deque([(0, encodebytes(uuid4().bytes))], maxlen=10000)

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
    def setup(self, spanner_db: Database, id: int, total_thread_count: int):
        with spanner_db.snapshot() as snapshot:
            print(
                f"My thread ID is {id}. The total count of threads is {total_thread_count}"
            )
            print(
                f"Hello from Google Spanner. It's {snapshot.execute_sql('select current_timestamp()').one()[0]}"
            )

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
    def __think__(self, spanner_db: Database):
        time.sleep(self.think_time)

    def random_str(self, size: int = 12):
        return (
            random.getrandbits(8 * size)
            .to_bytes(size, "big")
            .translate(self.tbl)
            .decode()
        )

    # Workload function stubs

    def txn_read(self, spanner_db: Database):
        with spanner_db.snapshot() as snapshot:
            acc_no, id = random.choice(self.order_tuples)

            results = snapshot.execute_sql(
                """
                SELECT *
                FROM orders
                WHERE acc_no = @acc_no
                  AND id = @id
                """,
                {
                    "acc_no": acc_no,
                    "id": id,
                },
            ).one_or_none()

            # print("txn_read ==> ", results)

    def txn_new_order(self, spanner_db: Database):
        # generate a random account number to be used for
        # for the order transaction
        self.account_number = random.randint(0, 999)

        def x(txn: Transaction):
            results = txn.execute_sql(
                """
                INSERT INTO orders (acc_no, id, status, amount)
                VALUES (@acc_no, @id, 'Pending', @amt) 
                THEN RETURN id
                """,
                {
                    "acc_no": self.account_number,
                    "id": encodebytes(uuid4().bytes),
                    "amt": round(random.random() * 1000000, 2),
                },
            )

            # save the id that the server generated
            self.id = results.one()[0]

            # save the (acc_no, id) tuple to our deque list
            # for future read transactions
            self.order_tuples.append((self.account_number, self.id))

        spanner_db.run_in_transaction(x)

    def txn_order_exec(self, spanner_db: Database):
        def x(txn: Transaction):
            r = txn.execute_sql(
                """
                SELECT *
                FROM ref_data
                WHERE acc_no = @acc_no
                """,
                {
                    "acc_no": self.account_number,
                },
            ).one_or_none()

            # simulate microservice doing something...
            time.sleep(0.02)

            txn.execute_sql(
                """
                UPDATE orders
                SET status = 'Complete'
                WHERE 
                    acc_no = @acc_no
                    and id =  @id
                """,
                {
                    "acc_no": self.account_number,
                    "id": self.id,
                },
            ).one_or_none()

        spanner_db.run_in_transaction(x)
