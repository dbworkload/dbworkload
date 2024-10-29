# Getting Started

The best way to understand how `dbworkload` works is by running through a good example.

We will be using PostgreSQL Server and CockroachDB, but the same logic applies to any of the
supported technologies.

!!! Note "Not using Postgres?"
    You can find equivalent files for other DBMS's in the
    <a href="https://github.com/dbworkload/dbworkload/tree/main/workloads" target="_blank">`workloads`</a> directory.

In this tutorial, we will go through the following tasks:

0. As a prerequisite, we setup our working environment.
1. We start with the DDL of a few tables and the SQL statements that are routinely executed against these tables.
2. We generate some random datasets for seeding the database tables.
3. We then create the tables in our database, and import the generated dataset.
4. From the SQL statements, we create the `dbworload` class file, that is, our workload file.
5. We run the workload, saving stats to a CSV file.
6. We collect the information and plot a chart to display the results of the test run.

By the end of this tutorial, you should have an understanding about how to use `dbworkload` to create your
own workload, run your benchmark tests, and display the results.
