---
hide:
  - navigation
  - toc
---

# dbworkload - DBMS workload utility

<center>![logo](assets/dbw.jpg){ width="250" }</center>

## Overview

The goal of `dbworkload` is to ease the creation and execution of bespoke database workload scripts.

The user is responsible for coding the workload logic as a Python `class`,
while `dbworkload` takes care of providing ancillary features, such as configuring the
workload concurrency, duration and/or iteration, and more.

The user, by coding the class, has complete control of the workload flow:
what transactions are executed in which order, and what statements are included in each transaction,
plus what data to insert and how to generate it and manipulate it.
