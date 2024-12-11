import psycopg
from dbworkload.utils import simplefaker
import yaml


class Seeder:
    def __init__(self, args: dict):
        self.yaml: str = args.get("yaml", None)
        self.batch_size: int = int(args.get("batch_size", 128))

        with open(self.yaml, "r") as f:
            self.load: dict = yaml.safe_load(f.read())

        self.sf = simplefaker.SimpleFaker()

    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        for table_name, table_details in self.load.items():
            for item in table_details:
                for col, col_details in item["columns"].items():
                    # get the list of simplefaker objects with different seeds
                    item["columns"][col] = self.sf.get_simplefaker_objects(
                        col_details["type"],
                        col_details.get("args", {}),
                        item["count"],
                        total_thread_count,
                    )

                # create a zip object so that generators are paired together
                z = list(zip(*[x for x in item["columns"].values()]))

                rows_chunk = self.sf.division_with_modulo(
                    item["count"], total_thread_count
                )

                # at this point, every thread will have the same zipped list of generators,
                # but each thread will need to pick its own generators.
                my_generators = z[id]
                my_chunk = rows_chunk[id]

                self.value_ph = ("%s," * len(my_generators))[:-1]

                if my_chunk > self.batch_size:
                    count = my_chunk // self.batch_size
                    rem = my_chunk % self.batch_size
                    iterations = self.batch_size
                else:
                    count = 1
                    rem = 0
                    iterations = my_chunk

                self.conn = conn
                for _ in range(count):
                    self.insert_batch(
                        table_name,
                        iterations,
                        *[
                            next(gen)
                            for _ in range(iterations)
                            for gen in my_generators
                        ],
                    )

                if rem > 0:
                    self.insert_batch(
                        table_name,
                        rem,
                        *[next(gen) for _ in range(rem) for gen in my_generators],
                    )

    def loop(self):
        raise ValueError("Insert job completed successfully!")

    def insert_batch(self, table_name: str, iterations: int, *args):
        placeholders = (f"({self.value_ph})," * iterations)[:-1]

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table_name}
                VALUES {placeholders}
                """,
                args,
            )
