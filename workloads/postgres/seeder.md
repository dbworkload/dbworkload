# Seeder - seed a database from a YAML file

For certain DBMS technologies, importing CSV files might not be an easy option.
For these cases, you might want to consider seeding the database tables using plain old SQL INSERT statements.

While probably not optimal for any DBMS, it's certainly the simplest.

## Getting Started

Create a simple table such as

```sql
CREATE TABLE ref_data (mystr STRING PRIMARY KEY, myint INT2);
```

Create a yaml file `ref_data.yaml` with below content

```yaml
# file: ref_data.yaml
ref_data:
  - count: 20
    sort-by: []
    columns:
      mystr:
        type: string
        args:
          seed: 0
          min: 3
          max: 3
      myint:
        type: integer
        args:
          seed: 0
          max: 1000
```

Run the seeder workload.
Notice that we pass the path to the YAML file in the `args` paramter.

```bash
dbworkload run -w seeder.py --uri 'postgres://u:p@h:26257/defaultdb?sslmode=require' --args '{"yaml": "ref_data.yaml", "batch_size": 4}' -c 2
```

The table was populated as follows

```sql
> select * from ref_data;                                                                                                                                                                                                          
  mystr | myint
--------+--------
  3Ml   |   455
  9EV   |   349
  B9O   |   266
  EuR   |   730
  Got   |   902
  H1a   |    64
  IVi   |    75
  IkS   |   305
  NcM   |   750
  Nn2   |   150
  RXg   |   995
  ULm   |   409
  VOO   |   128
  WTc   |   427
  gBm   |   972
  hXE   |   730
  lqC   |   550
  o6H   |   364
  sD8   |   867
  yiq   |   556
(20 rows)
```

The result set is the same as if you had used the `dbworkload util csv` option.
