import datetime as dt
import random
import time
import uuid

from cassandra.cluster import Session


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
        self.uuid: uuid.UUID = uuid.uuid4()
        self.ts: dt.datetime = ""
        self.event: str = ""

    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, session: Session, id: int, total_thread_count: int):
        print(
            f"My thread ID is {id}. The total count of threads is {total_thread_count}"
        )
        print(
            f"Cassandra verson: {session.execute('SELECT release_version FROM system.local').one().release_version}"
        )

    # the run() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, run() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):
        if random.random() < self.read_pct:
            return [self.read]
        return [self.txn1_new, self.txn2_verify, self.txn3_finalize]

    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def read(self, session: Session):
        session.execute(
            "select * from transactions where lane = %s and id = %s ALLOW FILTERING",
            (self.lane, self.uuid),
        ).one()

    def txn1_new(self, session: Session):
        # simulate microservice doing something
        self.uuid = uuid.uuid4()
        self.ts = dt.datetime.now()
        self.event = 0

        stmt = """
            insert into transactions (lane, id, event, ts) values (%s, %s, %s, %s);
            """
        session.execute(stmt, (self.lane, self.uuid, self.event, self.ts))

    def txn2_verify(self, session: Session):
        session.execute(
            "select * from ref_data where my_sequence = %s",
            (random.randint(0, 100000),),
        ).one()

        # simulate microservice doing something
        time.sleep(0.01)
        self.ts = dt.datetime.now()
        self.event = 1

        stmt = """
            insert into transactions (lane, id, event, ts) values (%s, %s, %s, %s);
            """
        session.execute(stmt, (self.lane, self.uuid, self.event, self.ts))

    def txn3_finalize(self, session: Session):
        session.execute(
            "select * from ref_data where my_sequence = %s",
            (random.randint(0, 100000),),
        ).one()

        # simulate microservice doing something
        self.ts = dt.datetime.now()
        self.event = 2
        time.sleep(0.01)

        stmt = """
            insert into transactions (lane, id, event, ts) values (%s, %s, %s, %s);
            """
        session.execute(stmt, (self.lane, self.uuid, self.event, self.ts))
