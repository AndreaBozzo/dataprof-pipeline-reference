# dataprof-pipeline-reference

A batch loaded successfully. Should consumers see it?

This repository answers that for one local pipeline: [dlt](https://dlthub.com)
loads a snapshot into a private DuckDB schema,
[dataprof](https://github.com/AndreaBozzo/dataprof) and SQL gate it, and only
an accepted batch replaces what consumers read, atomically and with an audit
trail. Bad or unproven data never replaces the previous accepted publication.

```
snapshot -> dlt (frozen schema) -> candidate schema -> dataprof checks + SQL assertions
         -> accepted: published.customers, published.orders move together, event recorded
         -> otherwise: evidence kept, publication unchanged
```

## Run it

```bash
uv sync --locked
uv run python -m pipeline_reference demo --workspace .runs/demo
```

The demo loads a healthy batch, a batch with 8% of orders missing their
customer, and the repair. Abridged output:

```
== batch-002: 8% of orders without customer_id
run r20261010t095431_d37c37  batch batch-002  candidate cand_r20261010t095431_d37c37
  dataprof customers: pass
  dataprof orders: fail
    FAILED        max_null_percentage customer_id  observed 8.0  expected <= 2.0
    passed        max_null_count order_id  observed 0  expected <= 0
  sql:
    passed        unique.orders.order_id  observed 0  expected == 0
    passed        reference.orders.customer_id  observed 0  expected == 0
outcome: rejected
published: run r20261010t095429_df5437 (event p20261010t095431_29a76f)

...
publications:
  p20261010t095431_29a76f  publish  run r20261010t095429_df5437
  p20261010t095435_7d17d9  publish  run r20261010t095433_3ae1f1

demo: all expected outcomes observed
```

Query what consumers see:

```bash
uv run python -c "import duckdb; print(duckdb.connect('.runs/demo/reference.duckdb', read_only=True).sql('SELECT currency, count(*), sum(amount) FROM published.orders GROUP BY ALL ORDER BY 1').fetchall())"
```

## Commands

| Command | Does |
| --- | --- |
| `run --snapshot DIR` | load, gate, publish if accepted |
| `status` | active publication, runs, events |
| `resume RUN_ID` | continue an interrupted run |
| `reevaluate RUN_ID` | new evaluation under the current policy and dependencies; never publishes |
| `promote RUN_ID --reason TEXT` | gate a ready run again and publish it on purpose |
| `rollback PUBLICATION_ID --reason TEXT` | republish an earlier accepted batch |
| `export RUN_ID` | rewrite report files from the ledger |
| `generate --batch-id ID --out DIR [--scenario NAME]` | write a fixture snapshot |

All but `generate` take `--workspace DIR`. Exit codes: 0 accepted or done,
1 rejected, 2 inconclusive, 3 operational, input or stale-target error.

## Scope

- Full snapshots of one contract (customers and orders); see
  `src/pipeline_reference/contracts/v1/`.
- One runner per workspace, embedded DuckDB.
- Evidence covers retained input bytes and the runner's own writes; it does
  not attest against out-of-band database changes.

## Docs

[Architecture](docs/architecture.md) ·
[Operations](docs/operations.md) ·
[Compatibility](docs/compatibility.md) ·
[Findings](docs/findings.md)

## License

MIT or Apache-2.0, at your option.
