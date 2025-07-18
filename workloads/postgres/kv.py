import random
import time
import uuid
from collections import deque
from itertools import zip_longest

import psycopg

COL_TYPES = ["bytes", "uuid", "int", "string", "fixed"]
WRITE_MODES = ["insert", "upsert", "do_nothing"]


class Kv:
    def __init__(self, args: dict):
        # ARGS
        self.think_time: float = float(args.get("think_time", 10) / 1000)
        self.batch_size: int = int(args.get("batch_size", 1))
        self.cycle_size: int = int(args.get("cycle_size", 1))
        self.table_name: str = args.get("table_name", "kv")
        self.key_types: list = args.get("key_types", "bytes").split(",")
        self.key_sizes: list = [
            int(x) if x else None
            for x in str(
                args.get("key_sizes", ",".join(["32" for kt in self.key_types]))
            ).split(",")
        ]
        self.value_types: list = args.get("value_types", "bytes").split(",")
        self.value_sizes: list = [
            int(x) if x else None
            for x in str(args.get("value_sizes", "256")).split(",")
        ]
        self.seed: str = args.get("seed", None)
        self.read_pct: float = float(args.get("read_pct", 0) / 100)
        self.update_pct: float = self.read_pct + float(args.get("update_pct", 0) / 100)
        self.delete_pct: float = self.update_pct + float(
            args.get("delete_pct", 0) / 100
        )
        self.key_pool_size: int = int(args.get("key_pool_size", 10000))
        self.key_pool_preload_size: int = int(args.get("key_pool_preload_size", 0))
        self.write_mode: str = args.get("write_mode", "insert")
        self.aost: str = args.get("aost", "")
        self.fixed_value: str = args.get("fixed_value", None)
        self.stmts_per_txn: list = [
            int(x) for x in args.get("stmts_per_txn", "1-1").split("-")
        ]

        # type checks
        for k in self.key_types:
            if k not in COL_TYPES:
                raise ValueError(
                    f"The selected key_type '{k}' is invalid. The possible values are {', '.join(COL_TYPES)}"
                )

        for t in self.value_types:
            if t not in COL_TYPES:
                raise ValueError(
                    f"The selected value_type '{t}' is invalid. The possible values are {', '.join(COL_TYPES)}."
                )

        self.key_types_and_sizes = list(zip_longest(self.key_types, self.key_sizes))
        self.value_types_and_sizes = list(
            zip_longest(self.value_types, self.value_sizes)
        )

        # write_mode checks
        if self.write_mode not in WRITE_MODES:
            raise ValueError(
                f"The selected write_mode '{self.write_mode}' is invalid. The possible values are 'insert', 'upsert', 'do_nothing'."
            )

        self.command = "INSERT"
        if self.write_mode == "upsert":
            self.command = "UPSERT"

        self.suffix = ""
        if self.write_mode == "do_nothing":
            self.suffix = " ON CONFLICT DO NOTHING"

        # placeholders
        self.key_id = "k"
        for i in range(len(self.key_types) - 1):
            self.key_id = f"{self.key_id},k{i+1}"

        self.key_ph = ("%s," * len(self.key_types))[:-1]
        value_ph = ("%s," * len(self.value_types))[:-1]
        self.placeholders = (f"({self.key_ph},{value_ph})," * self.batch_size)[:-1]

        # create random generator
        # Not implemented as it needs a FR in dbworkload
        # self.rng = random.Random(int(self.seed) if self.seed else None)
        self.rng = random.Random(None)

        # make translation table from 0..255 to 97..122
        self.tbl = bytes.maketrans(
            bytearray(range(256)),
            bytearray(
                [ord(b"a") + b % 26 for b in range(113)]
                + [ord(b"0") + b % 10 for b in range(30)]
                + [ord(b"A") + b % 26 for b in range(113)]
            ),
        )

        # create pool to pick the key from
        self.key_pool = deque(
            (tuple(self.__get_data(t, s) for t, s in self.key_types_and_sizes),),
            maxlen=self.key_pool_size,
        )

        # AOST
        if self.aost:
            if self.aost == "fr":
                self.aost = "AS OF SYSTEM TIME follower_read_timestamp()"
            else:
                self.aost = f"AS OF SYSTEM TIME '{self.aost}'"

    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: id):
        with conn.cursor() as cur:
            print(
                f"My thread ID is {id}. The total thread count is {total_thread_count}."
            )
            if self.key_pool_preload_size:
                print(
                    f"Fetching {self.key_pool_preload_size} rows from {self.table_name}."
                )
                self.key_pool.clear()
                self.key_pool.extend(
                    cur.execute(
                        f"""select {self.key_id} 
                        from {self.table_name} as of system time follower_read_timestamp() 
                        limit {self.key_pool_preload_size}"""
                    ).fetchall()
                )

    def loop(self):
        def get_func():
            rnd = random.random()
            if rnd < self.read_pct:
                return [self.read_kv, self.__think__] * self.cycle_size
            elif rnd < self.update_pct:
                return [self.update_kv, self.__think__] * self.cycle_size
            elif rnd < self.delete_pct:
                return [self.delete_kv, self.__think__] * self.cycle_size
            return [self.write_kv, self.__think__] * self.cycle_size

        if self.stmts_per_txn[1] == 1:
            return get_func()
        else:
            l = []
            for _ in range(
                random.randint(self.stmts_per_txn[0], self.stmts_per_txn[1])
            ):
                l.extend(get_func())

            return [self.begin] + l + [self.commit]

    def __think__(self, conn: psycopg.Connection):
        time.sleep(self.think_time)

    def __get_data(self, data_type: str, size: int):
        if data_type == "bytes":
            return self.rng.getrandbits(8 * size).to_bytes(size, "big")
        elif data_type == "uuid":
            return uuid.UUID(int=self.rng.getrandbits(128), version=4)
        elif data_type == "int":
            return self.rng.randint(0, 2**63 - 1)
        elif data_type == "fixed":
            return self.fixed_value
        elif data_type == "string":
            return (
                self.rng.getrandbits(8 * size)
                .to_bytes(size, "big")
                .translate(self.tbl)
                .decode()
            )

    def begin(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute("BEGIN")

    def commit(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute("COMMIT")

    def read_kv(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {self.table_name} {self.aost} WHERE ({self.key_id}) = ({self.key_ph})",
                random.choice(self.key_pool),
            ).fetchone()

    def update_kv(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self.table_name} SET v = %s WHERE ({self.key_id}) = ({self.key_ph})",
                (
                    self.__get_data(self.value_types[0], self.value_sizes[0]),
                    *(random.choice(self.key_pool)),
                ),
            )

    def write_kv(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            args = []
            for _ in range(self.batch_size):
                k = tuple([self.__get_data(t, s) for t, s in self.key_types_and_sizes])
                self.key_pool.append(k)
                args.extend(k)
                args.extend(
                    [self.__get_data(t, s) for t, s in self.value_types_and_sizes]
                )

            cur.execute(
                f"{self.command} INTO {self.table_name} VALUES {self.placeholders} {self.suffix}",
                args,
            )

    def delete_kv(self, conn: psycopg.Connection):
        # make sure to keep at least 1 item in the pool
        if len(self.key_pool) <= 1:
            return

        key = random.choice(self.key_pool)
        self.key_pool.remove(key)

        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self.table_name} WHERE ({self.key_id}) = ({self.key_ph})",
                key,
            )
