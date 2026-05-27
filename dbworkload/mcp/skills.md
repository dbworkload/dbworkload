# AI Rules for Generating `dbworkload` Python Classes

You are an expert developer specialized in generating custom workload
simulation scripts for the `dbworkload` framework. Generate simple, direct,
readable workload classes that look like code a human benchmark author would
maintain.

## Core Rules

1. Every simulation should be a standalone Python class with no shared global
   mutable state.
2. The class initializer should accept an `args: dict` configuration object.
3. Use `setup(conn, id, total_thread_count)` only for per-worker initialization,
   connection checks, optional data seeding, or worker-local caches.
4. `loop()` should usually return a list of transaction methods. For weighted
   mixes, prebuild and shuffle a schedule in `__init__`, then return that
   schedule from `loop()`.
5. Transaction methods should be named `txn_*`, include a short docstring, and
   execute their SQL directly.
6. Avoid nested `work()` closures and generic `_run()` wrappers unless custom
   error behavior is explicitly required.
7. Do not return `True` or `False` from transaction methods unless the framework
   or scenario specifically requires it.
8. Let `dbworkload` handle standard retry behavior. Catch database exceptions
   only for intentional workload semantics, custom metrics, or expected
   non-retryable business outcomes.
9. Generate per-step random data inside the worker instance to avoid shared
   mutable state and lock contention.
10. Keep the workload readable: prefer business-shaped transaction methods over
    over-abstracted helper layers.

## Generated Workload Bundle

Unless the user explicitly asks for only one file, generate a small runnable
workload bundle, not just a Python class:

- `<basename>.py`: the `dbworkload` class.
- `<basename>.sql`: minimal DDL needed to run the workload on a fresh test
  database.
- `<basename>.md`: concise explanation of the workload model and run
  instructions.

The workload basename, generated files, and Python class must correspond.

| Workload name | Python class | Python file | SQL file | Markdown file |
| --- | --- | --- | --- | --- |
| ABC | `ABC` | `abc.py` | `abc.sql` | `abc.md` |
| SuperKool | `SuperKool` | `super_kool.py` | `super_kool.sql` | `super_kool.md` |
| SportsSim | `SportsSim` | `sports_sim.py` | `sports_sim.sql` | `sports_sim.md` |

Rules:

- Python file: `<basename>.py`
- SQL file: `<basename>.sql`
- Markdown file: `<basename>.md`
- Python class: class-cased form of the workload name
- For acronyms, preserve the class acronym and lowercase the file basename:
  `ABC` becomes `abc.py`, `abc.sql`, and `abc.md`.

## Companion Markdown File

Every generated workload bundle should include `<basename>.md` with enough
context for a human to understand, run, and adjust the simulation.

Use this section structure by default:

```markdown
# <WorkloadName> Workload Model

## Simplified Understanding

## Implemented Class

## Default Mix

## Suggested Run Shape

## Fidelity Limits
```

The Markdown file should explain:

- what real workload is being modeled,
- the simplified interpretation used by the simulation,
- which `txn_*` methods map to which real workflows,
- default transaction mix or weights,
- important `--args` parameters,
- an example `dbworkload run` command,
- assumptions and fidelity limits.

Example run command:

```bash
dbworkload run \
  --driver postgres \
  --workload abc.py \
  --concurrency 128 \
  --duration 1200 \
  --args '{"account_count": 100000, "tlc_accounts": 2000, "tlc_pct": 80}'
```

## Minimal SQL DDL File

Every generated workload bundle should include `<basename>.sql` with only the
database objects required by the generated workload. It should be concise, not a
dump of a full production schema.

Include only what is needed:

- required schemas,
- required enum/type definitions,
- tables referenced by the workload,
- columns used by inserts, selects, updates, conflict targets, and predicates,
- primary keys and unique constraints needed by `ON CONFLICT`,
- indexes needed by explicit index hints or important query shapes.

Cross-check the Python and SQL files before finishing:

- every table referenced in `<basename>.py` exists in `<basename>.sql`,
- every inserted column exists,
- every updated or selected predicate column exists,
- every explicit index hint exists,
- every `ON CONFLICT (...)` target has a matching primary key or unique index,
- every custom enum cast used in SQL has a matching type definition.

## Final Artifact Checklist

Before considering a generated workload complete, verify:

- [ ] `<basename>.py` contains class `<ClassName>`.
- [ ] `<basename>.py` compiles.
- [ ] `<basename>.py` has direct `txn_*` methods with docstrings.
- [ ] `loop()` returns a list of transaction functions, preferably a prebuilt
      schedule for weighted mixes.
- [ ] `<basename>.sql` includes all required DDL.
- [ ] `<basename>.md` explains the workload and includes a runnable
      `dbworkload run` command.
- [ ] File basenames match across `.py`, `.sql`, and `.md`.

## Preferred Class Shape

Use `__init__(args)` for configuration, optional `setup()` for per-worker
initialization, `loop()` for the execution schedule, and direct `txn_*` methods
for simulated business transactions.

```python
import random

import psycopg


class WalletWorkload:
    def __init__(self, args: dict):
        self.account_count = int(args.get("account_count", 100000))
        self.hot_accounts = int(args.get("hot_accounts", 2000))
        self.hot_pct = float(args.get("hot_pct", 80)) / 100
        self.schedule_size = int(args.get("schedule_size", 100))
        self.loop_schedule = self.build_schedule()

    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        """Load worker-local state and verify the connection."""
        with conn.cursor() as cur:
            cur.execute("SELECT 1").fetchone()

    def loop(self):
        return self.loop_schedule

    def build_schedule(self):
        weighted_funcs = [
            (70, self.txn_wallet_read),
            (20, self.txn_wallet_debit),
            (10, self.txn_wallet_credit),
        ]

        total = sum(weight for weight, _ in weighted_funcs)
        schedule = []
        for weight, func in weighted_funcs:
            count = round((weight / total) * self.schedule_size)
            schedule.extend([func] * max(1, count))

        random.shuffle(schedule)
        return schedule[: self.schedule_size]

    def account_id(self):
        if random.random() < self.hot_pct:
            return random.randint(1, self.hot_accounts)
        return random.randint(1, self.account_count)

    def amount(self):
        return random.randint(1, 100)

    def txn_wallet_read(self, conn: psycopg.Connection):
        """Read the current wallet balance for an account."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT balance
                FROM account_balances
                WHERE account_id = %s
                """,
                (self.account_id(),),
            ).fetchone()

    def txn_wallet_debit(self, conn: psycopg.Connection):
        """Lock a wallet row, debit balance, and write a ledger row."""
        account_id = self.account_id()
        amount = self.amount()

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT balance
                    FROM account_balances
                    WHERE account_id = %s
                    FOR UPDATE
                    """,
                    (account_id,),
                )
                balance = cur.fetchone()[0]

                cur.execute(
                    """
                    UPDATE account_balances
                    SET balance = balance - %s
                    WHERE account_id = %s
                    """,
                    (amount, account_id),
                )

                cur.execute(
                    """
                    INSERT INTO account_statements(account_id, amount, balance)
                    VALUES (%s, %s, %s)
                    """,
                    (account_id, -amount, balance - amount),
                )

    def txn_wallet_credit(self, conn: psycopg.Connection):
        """Credit a wallet balance and write a ledger row."""
        account_id = self.account_id()
        amount = self.amount()

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE account_balances
                    SET balance = balance + %s
                    WHERE account_id = %s
                    RETURNING balance
                    """,
                    (amount, account_id),
                )
                balance = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO account_statements(account_id, amount, balance)
                    VALUES (%s, %s, %s)
                    """,
                    (account_id, amount, balance),
                )
```

## Transaction Method Guidance

- Keep each `txn_*` method focused on one simulated business workflow.
- Put the transaction body directly in the method.
- Add a one-line docstring that names the workflow being modeled.
- Use helper methods for data generation when they improve readability.
- Avoid broad `except Exception` wrappers around normal transaction code.
- Avoid wrapping every transaction in a generic `_run()` helper.
- Avoid nested `work()` functions inside transaction methods.

Preferred:

```python
def txn_bonus_entitlement(self, conn):
    """Read active bonus and lossback state for a ledger event."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bonus_id, lossback_pct
            FROM bonus_entitlements
            WHERE account_id = %s
              AND active
            """,
            (self.account_id(),),
        ).fetchall()
```

Avoid:

```python
def txn_bonus_entitlement(self, conn):
    def work():
        with conn.cursor() as cur:
            cur.execute("SELECT ...")

    return self._run(work)
```

## Weighted Loop Schedules

For weighted random workloads, prefer building a schedule once per worker
instance. This avoids unnecessary per-loop decision overhead and makes the
mix transparent.

```python
def __init__(self, args: dict):
    self.schedule_size = int(args.get("schedule_size", 100))
    self.loop_schedule = self.build_schedule()

def loop(self):
    return self.loop_schedule

def build_schedule(self):
    weighted_funcs = [
        (70, self.txn_read),
        (20, self.txn_write),
        (10, self.txn_batch),
    ]

    total = sum(weight for weight, _ in weighted_funcs)
    schedule = []
    for weight, func in weighted_funcs:
        count = round((weight / total) * self.schedule_size)
        schedule.extend([func] * max(1, count))

    random.shuffle(schedule)
    return schedule[: self.schedule_size]
```

Dynamic choices inside `loop()` are still acceptable when the workload truly
needs to choose a different transaction sequence based on worker-local state,
previous transaction results, or multi-step flows.

## `--args` Format

`--args` receives a JSON or YAML-style dictionary, or a path to a JSON/YAML
file. Do not use shell-style `key=value key=value` strings.

Preferred:

```bash
dbworkload run \
  --driver postgres \
  --workload wallet.py \
  --concurrency 128 \
  --duration 1200 \
  --args '{"account_count": 100000, "hot_accounts": 2000, "hot_pct": 80}'
```

Also valid:

```bash
dbworkload run \
  --workload wallet.py \
  --uri "postgresql://user:password@localhost:5432/app" \
  --args ./wallet-args.yaml
```

Avoid:

```bash
--args "account_count=100000 hot_accounts=2000 hot_pct=80"
```

Use `dbworkload` driver names such as `postgres`, `mysql`, `maria`, `oracle`,
`mongo`, `cassandra`, `spanner`, and `pinecone`. Do not use Python package names
such as `psycopg` as driver values.

## Retry and Error Guidance

`dbworkload` should own normal statement and transaction retry behavior. Generated
workloads should not add broad error wrappers by default.

Catch exceptions only when:

- the workload intentionally models a business-level failure,
- the exception is an expected non-retryable outcome,
- custom metrics or compensating logic are explicitly required,
- a driver-specific API requires a narrow exception handler.

When catching exceptions, catch the narrowest useful exception type and keep the
transaction method readable.
