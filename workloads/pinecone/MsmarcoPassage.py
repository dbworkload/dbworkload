import numpy as np
import random
from pinecone.db_data.index import Index


class Msmarcopassage:
    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag
        # user passed a yaml/json, in python that's a dict object
        self.next_input = self.__next_input()


    def __next_input(self):
        v = np.random.normal(size=384)
        return (v / np.linalg.norm(v)).tolist()


    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, index: Index, id: int, total_thread_count: int):
        print(
            f"My thread ID is {id}. The total count of threads is {total_thread_count}"
        )
        index_info = index.describe_index_stats()
        del index_info['_response_info']
        print(f"Index info: {index_info}")


    # the loop() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, run() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):
        return [
            self.search
        ]

    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def search(self, index: Index):
        # hard-coded dummy vector (384 dims)
        vector = [0.0] * 384

        result = index.query(
            vector=self.next_input,
            top_k=100,
            include_metadata=True,
        )

        matches = result["matches"]
        if len(matches) < 1:
            self.next_input = self.__next_input()

        else:
            if random.random() < 0.5:
                self.next_input = self.__next_input()

            else:
                farthest = matches[-1]
                # print(farthest["id"])
                # print(farthest["metadata"]["passage"])
                farthest_id = farthest["id"]
                fetched = index.fetch(ids=[farthest_id])
                vector = fetched["vectors"][farthest_id]["values"]
                # print(vector)
                self.next_input = vector


