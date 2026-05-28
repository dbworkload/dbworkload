#!/usr/bin/python

from dataclasses import dataclass, field


@dataclass
class ConnInfo:
    params: dict = field(default_factory=dict)
    extras: dict = field(default_factory=dict)
