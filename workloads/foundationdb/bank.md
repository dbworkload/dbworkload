# Bank Workload Model

## Simplified Understanding

This workload models the same small banking/order flow as the PostgreSQL bank
example, but stores records in FoundationDB tuple keyspaces instead of SQL
tables.

`ref_data` rows become tuple keys containing account reference data. `orders`
rows become tuple keys containing JSON order documents.

## Implemented Class

The workload class is `Bank` in `bank.py`.

- `txn_read` reads one worker-local remembered order.
- `txn_new_order` creates a pending order.
- `txn_order_exec` reads account reference data, waits briefly to model service
  work, and marks the order complete in a FoundationDB transaction.

## Default Mix

By default, `read_pct` is `0`, so each loop creates and completes one order.
Set `read_pct` to add point reads, for example `{"read_pct": 50}`.

Important `--args` parameters:

- `read_pct`: percentage of loops that run only `txn_read`.
- `think_time`: milliseconds used by the original bank model, retained for
  compatibility.
- `ref_data_count`: number of reference-data accounts to seed and choose from.
- `seed_ref_data`: set to `false` to skip reference-data seeding.
- `namespace`: first tuple element used to isolate this workload's keyspace.

## Suggested Run Shape

```bash
dbworkload run \
  --driver foundationdb \
  --workload workloads/foundationdb/bank.py \
  --uri 'api_version=730' \
  --concurrency 8 \
  --duration 120 \
  --args '{"read_pct": 50, "ref_data_count": 1000}'
```

With an explicit cluster file:

```bash
dbworkload run \
  --workload workloads/foundationdb/bank.py \
  --uri 'foundationdb:///etc/foundationdb/fdb.cluster?api_version=730' \
  --concurrency 8 \
  --duration 120
```

## Fidelity Limits

The PostgreSQL example uses tables, secondary SQL predicates, and an explicit
transaction around the order execution step. This FoundationDB version keeps the
same business shape but models rows as tuple keys and JSON values. It does not
create secondary indexes or scan ranges because the original workload mostly
uses primary-key lookups.
