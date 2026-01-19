import datetime as dt
import random
import time
import uuid
import asyncio

from pinecone import Pinecone
from pinecone.db_data.index import Index


class Msmarcopassage:
    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag
        # user passed a yaml/json, in python that's a dict object
        return

    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, index: Index, id: int, total_thread_count: int):
        print(
            f"My thread ID is {id}. The total count of threads is {total_thread_count}"
        )
        index_info = index.describe_index_stats()
        del index_info['_response_info']
        print(f"Index info: {index_inf}")


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
            vector=vector,
            top_k=5,
            include_metadata=True,
        )

        print("Sync search result:")
        print(result)

