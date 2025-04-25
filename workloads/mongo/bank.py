import datetime as dt
import random
import time
import uuid

from pymongo import MongoClient


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
        self.id: uuid.UUID = uuid.uuid4()
        self.ts: dt.datetime = ""
        self.event: str = ""

    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, client: MongoClient, id: int, total_thread_count: int):
        print(
            f"My thread ID is {id}. The total count of threads is {total_thread_count}"
        )
        print(f'MongoDB version: {client.server_info()["version"]}')

    # the loop() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, run() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):
        if random.random() < self.read_pct:
            return [self.read]
        return [self.txn1_new]  # , self.txn2_verify, self.txn3_finalize]

    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def read(self, client: MongoClient):
        client["bank"].transactions.find_one({"lane": self.lane, "id": str(self.id)})

    def txn1_new(self, client: MongoClient):
        # simulate microservice doing something
        self.id = uuid.uuid4()
        self.ts = dt.datetime.now()
        self.event = 0

        client["bank"].transactions.insert_one(
            {"lane": self.lane, "id": str(self.id), "event": self.event, "ts": self.ts}
        )

    # example on how to create a transaction with multiple queries
    # def txn2_verify(self, client: MongoClient):

    #     with conn.cursor() as cur:
    #         cur.execute(
    #             "select * from ref_data where my_sequence = %s",
    #             (random.randint(0, 100000),),
    #         )
    #         cur.fetchone()

    #         # simulate microservice doing something
    #         time.sleep(0.01)
    #         self.ts = dt.datetime.now()
    #         self.event = 1

    #         stmt = """
    #             insert into transactions values (%s, %s, %s, %s);
    #             """
    #         # as we're inside 'tx', the below will not autocommit
    #         cur.execute(stmt, (self.lane, self.uuid_bytes, self.event, self.ts))

    # def txn3_finalize(self, conn: MySQLConnection):
    #     with conn.cursor() as cur:
    #         cur.execute(
    #             "select * from ref_data where my_sequence = %s",
    #             (random.randint(0, 100000),),
    #         )
    #         cur.fetchone()

    #         # simulate microservice doing something
    #         self.ts = dt.datetime.now()
    #         self.event = 2
    #         time.sleep(0.01)

    #         stmt = """
    #         insert into transactions values (%s, %s, %s, %s);
    #         """
    #     cur.execute(stmt, (self.lane, self.uuid_bytes, self.event, self.ts))
    # conn.commit()
