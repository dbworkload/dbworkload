import datetime as dt
import random
import time
import uuid

from mysql.connector import MySQLConnection


class Bank:
    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag
        # user passed a yaml/json, in python that's a dict object

        self.read_pct: float = float(args.get("read_pct", 50) / 100)

        self.lane: str = (
            random.choice(["ACH", "DEPO", "WIRE"])
            if not args.get("lane", "")
            else args["lane"]
        )

        # you can arbitrarely add any variables you want
        self.uuid_bytes: uuid.UUID = uuid.uuid4().bytes
        self.ts: dt.datetime = ""
        self.event: str = ""

    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, conn: MySQLConnection, id: int, total_thread_count: int):
        with conn.cursor() as cur:
            print(
                f"My thread ID is {id}. The total count of threads is {total_thread_count}"
            )
            cur.execute(f"select version()")
            print(f"MySQL version: {cur.fetchone()[0]}")

    # the loop() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, loop() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):
        if random.random() < self.read_pct:
            return [self.read]
        return [self.txn1_new, self.txn2_verify, self.txn3_finalize]

    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def read(self, conn: MySQLConnection):
        with conn.cursor() as cur:
            cur.execute(
                "select * from transactions where lane = %s and id = %s",
                (self.lane, self.uuid_bytes),
            )
            cur.fetchall()

    def txn1_new(self, conn: MySQLConnection):
        # simulate microservice doing something
        time.sleep(0.01)
        self.uuid_bytes = uuid.uuid4().bytes
        self.ts = dt.datetime.now()
        self.event = 0

        with conn.cursor() as cur:
            stmt = """
                insert into transactions values (%s, %s, %s, %s);
                """
            cur.execute(stmt, (self.lane, self.uuid_bytes, self.event, self.ts))

    # example on how to create a transaction with multiple queries
    def txn2_verify(self, conn: MySQLConnection):
        conn.start_transaction()

        with conn.cursor() as cur:
            cur.execute(
                "select * from ref_data where my_sequence = %s",
                (random.randint(0, 100000),),
            )
            cur.fetchone()

            # simulate microservice doing something
            time.sleep(0.01)
            self.ts = dt.datetime.now()
            self.event = 1

            stmt = """
                insert into transactions values (%s, %s, %s, %s);
                """
            # as we're inside a transaction, the below will not autocommit
            cur.execute(stmt, (self.lane, self.uuid_bytes, self.event, self.ts))

        conn.commit()

    def txn3_finalize(self, conn: MySQLConnection):
        conn.start_transaction()
        with conn.cursor() as cur:
            cur.execute(
                "select * from ref_data where my_sequence = %s",
                (random.randint(0, 100000),),
            )
            cur.fetchone()

            # simulate microservice doing something
            self.ts = dt.datetime.now()
            self.event = 2
            time.sleep(0.01)

            stmt = """
                insert into transactions values (%s, %s, %s, %s);
                """
            cur.execute(stmt, (self.lane, self.uuid_bytes, self.event, self.ts))
        conn.commit()
