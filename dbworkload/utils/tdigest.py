#!/usr/bin/python

import numpy as np
from fastdigest import TDigest, merge_all

MAX_CENTROIDS = 1000


def from_values(values) -> TDigest:
    return TDigest.from_values(
        np.asarray(values, dtype=float), max_centroids=MAX_CENTROIDS
    )


def from_centroids(centroids) -> TDigest:
    arr = np.asarray(centroids, dtype=float)

    if arr.size == 0:
        return TDigest(MAX_CENTROIDS)

    arr = np.atleast_2d(arr)
    return TDigest.from_values(arr[:, 0], arr[:, 1], max_centroids=MAX_CENTROIDS)


def combine(digests) -> TDigest:
    return merge_all(list(digests))


def centroids(td: TDigest) -> np.ndarray:
    return np.asarray(td.centroids, dtype=float).reshape(-1, 2)


def count(td: TDigest) -> int:
    return int(td.mass())
