from __future__ import annotations

import json
import random
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import fdb
import fdb.tuple


class Bank:
    def __init__(self, args: dict):
        self.think_time: float = float(args.get("think_time", 5) / 1000)
        self.read_pct: float = float(args.get("read_pct", 0) / 100)
        self.ref_data_count: int = int(args.get("ref_data_count", 1000))
        self.seed_ref_data: bool = bool(args.get("seed_ref_data", True))
        self.namespace: str = args.get("namespace", "dbworkload:bank")

        self.order_tuples = deque([(0, uuid4())], maxlen=10000)
        self.account_number = 0
        self.id = uuid4()

        self.tbl = bytes.maketrans(
            bytearray(range(256)),
            bytearray(
                [ord(b"a") + b % 26 for b in range(113)]
                + [ord(b"0") + b % 10 for b in range(30)]
                + [ord(b"A") + b % 26 for b in range(113)]
            ),
        )

    def setup(self, db: Any, id: int, total_thread_count: int):
        """Verify the FoundationDB connection and optionally seed ref_data."""
        print(f"My thread ID is {id}. The total count of threads is {total_thread_count}")

        if self.seed_ref_data and id == 0:
            acc_no = 0
            while acc_no < self.ref_data_count:
                tr = db.create_transaction()
                batch_start = acc_no

                try:
                    batch_end = min(acc_no + 100, self.ref_data_count)
                    while acc_no < batch_end:
                        tr[self.ref_data_key(acc_no)] = self.encode(
                            {
                                "acc_no": acc_no,
                                "external_ref_id": str(uuid4()),
                                "created_time": self.utc_now(),
                                "acc_details": self.random_str(48),
                            }
                        )
                        acc_no += 1

                    tr.commit().wait()
                except fdb.FDBError as e:
                    acc_no = batch_start
                    tr.on_error(e).wait()

    def loop(self):
        if random.random() < self.read_pct:
            return [self.txn_read]
        return [self.txn_new_order, self.txn_order_exec]

    def random_str(self, size: int = 12):
        return (
            random.getrandbits(8 * size)
            .to_bytes(size, "big")
            .translate(self.tbl)
            .decode()
        )

    def utc_now(self):
        return datetime.now(timezone.utc).isoformat()

    def encode(self, value: dict) -> bytes:
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def decode(self, value):
        if value is None:
            return None
        return json.loads(bytes(value).decode("utf-8"))

    def key(self, *parts):
        return fdb.tuple.pack((self.namespace, *parts))

    def ref_data_key(self, acc_no: int):
        return self.key("ref_data", acc_no)

    def order_key(self, acc_no: int, order_id):
        return self.key("orders", acc_no, str(order_id))

    def txn_read(self, db: Any):
        """Read one locally remembered order by account number and id."""
        acc_no, order_id = random.choice(self.order_tuples)
        tr = db.create_transaction()

        while True:
            try:
                self.decode(tr[self.order_key(acc_no, order_id)].wait())
                return
            except fdb.FDBError as e:
                tr.on_error(e).wait()

    def txn_new_order(self, db: Any):
        """Create a pending order and remember its key for later reads."""
        self.account_number = random.randint(0, self.ref_data_count - 1)
        self.id = uuid4()
        tr = db.create_transaction()
        order = self.encode(
            {
                "acc_no": self.account_number,
                "id": str(self.id),
                "status": "Pending",
                "amount": round(random.random() * 1000000, 2),
                "ts": self.utc_now(),
            }
        )

        while True:
            try:
                tr[self.order_key(self.account_number, self.id)] = order
                tr.commit().wait()
                break
            except fdb.FDBError as e:
                tr.on_error(e).wait()

        self.order_tuples.append((self.account_number, self.id))

    def txn_order_exec(self, db: Any):
        """Read reference data, wait briefly, and mark the order complete."""
        tr = db.create_transaction()

        while True:
            try:
                ref_data = tr[self.ref_data_key(self.account_number)].wait()
                order = self.decode(tr[self.order_key(self.account_number, self.id)].wait())

                time.sleep(0.02)

                if order is None:
                    order = {
                        "acc_no": self.account_number,
                        "id": str(self.id),
                        "status": "Pending",
                        "amount": 0,
                        "ts": self.utc_now(),
                    }

                if ref_data is not None:
                    order["external_ref_seen"] = self.decode(ref_data)["external_ref_id"]

                order["status"] = "Complete"
                order["completed_ts"] = self.utc_now()
                tr[self.order_key(self.account_number, self.id)] = self.encode(order)
                tr.commit().wait()
                return
            except fdb.FDBError as e:
                tr.on_error(e).wait()
