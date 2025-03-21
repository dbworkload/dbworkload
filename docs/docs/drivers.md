# Drivers

Here is the list of the currently supported drivers.

## postgres

For technologies such as PostgreSQL, CockroachDB

Driver documentation: [Psycopg 3](https://www.psycopg.org/psycopg3/docs/).

```bash
# installation
pip install dbworkload[postgres]

# sample use
dbworkload run -w workloads/postgres/bank.py \
  --uri 'postgres://cockroach:cockroach@localhost:26257/bank?sslmode=require' \
  -l debug --args '{"read_pct":50}' -i 1 -c 1
```

## mysql

For technologies such as MySQL, TiDB, Singlestore

Driver documentation: [MySQL Connector/Python Developer Guide](https://dev.mysql.com/doc/connector-python/en/).

```bash
# installation
pip3 install dbworkload[mysql]

# sample use
dbworkload run -w workloads/mysql/bank.py \
  --uri 'user=root,password=London123,host=localhost,port=3306,database=bank,client_flags=SSL' \
   --driver mysql --args '{"read_pct":50}' -i 10
```

## mariadb

Driver documentation: [MariaDB Connector/Python](https://mariadb.com/docs/server/connect/programming-languages/python/).

```bash
# installation
pip3 install dbworkload[mariadb]

# sample use
dbworkload run -w workloads/mariadb/bank.py \
  --uri 'user=user1,password=password1,host=localhost,port=3306,database=bank' \
  --driver maria --args '{"read_pct":50}' -i 10
```

## oracle

Driver documentation: [python-oracledb’s documentation](https://python-oracledb.readthedocs.io/en/latest/index.html).

```bash
# installation
pip3 install dbworkload[oracle]

# sample use
dbworkload run -w workloads/oracle/bank.py \
  --uri "user=admin,password=password1,dsn="myhost.host2.us-east-1.rds.amazonaws.com:1521/OMS" \
  --driver oracle --args='{"read_pct":50}' -i 10
```

## sqlserver

Under construction...

Driver documentation: [Python SQL driver](https://learn.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-ver16).

```bash
# installation
pip3 install dbworkload[sqlserver]

# sample use
dbworkload run -w workloads/sqlserver/bank.py \
  --uri "" \
   --driver sqlserver --args='{"read_pct":50}' -i 10
```

## mongo

Driver documentation: [MongoDB PyMongo Documentation](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/).

```bash
# installation
pip3 install dbworkload[mongo]

# sample use
dbworkload run -w workloads/mongo/bank.py \
  --uri "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000" \
  --args='{"read_pct":50}' -i 10
```

## scylla-driver

For technologies such as Cassandra, ScyllaDB

Under construction...

Driver documentation: [Python Driver for Scylla and Apache Cassandra](https://python-driver.docs.scylladb.com/stable/).

```bash
# installation
pip3 install dbworkload[cassandra]

# sample use
dbworkload run -w workloads/cassandra/bank.py \
  --uri "" \
   --driver cassandra --args='{"read_pct":50}' -i 10
```

## spanner

For technologies such as Google Spanner

Driver documentation: [SpannerSQL Driver](https://cloud.google.com/spanner/docs/getting-started/python).

```bash
# installation
pip install dbworkload[spanner]

# sample use
dbworkload run -w workloads/spanner/bank.py \
  --driver spanner --uri 'instance=my-spanner-1, database=bank' \
  -l debug --args '{"read_pct":50}' -i 1 -c 1
```
