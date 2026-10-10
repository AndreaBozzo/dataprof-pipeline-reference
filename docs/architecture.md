# Architecture

## Ownership

| Part | Owns |
| --- | --- |
| dlt | extraction, normalization, loading, the frozen schema contract |
| dataprof | profiles, gate checks and their verdicts |
| DuckDB | storage, SQL assertions, transactions |
| this repo | runs, readiness, outcome, publication, recovery, evidence |

## Run lifecycle

```
created -> loading -> ready -> evaluated (accepted | rejected | inconclusive | operational_error)
                  \-> blocked (contract or readiness)
                  \-> load_failed | input_error
```

- Each attempt gets a run ID, a candidate schema `cand_<run_id>` and its own
  dlt pipeline. A retry never reuses a candidate.
- The run records the active publication when it is created (its base), its
  contract version, and digests of that version's `columns.json` and stored
  dlt schema. Later commands load the recorded version and refuse it if those
  files changed.
- Ready means: dlt reported no failed jobs, every load ID is complete in
  `_dlt_loads`, both tables exist, columns and types match `columns.json`,
  retained inputs still match the manifest.

## Gates

- dataprof: per-column null limits and a required completeness assessment,
  `scope="full_source"`, on the explicit business projection
  (`contracts/v1/policy.json`).
- SQL: volume floor (`min_rows`), business-key uniqueness, amount range,
  currency set, orders to customers reference.
- Outcome: any failed check rejects; otherwise any inconclusive check makes
  it inconclusive; otherwise accepted. An unknown verdict is an error.
- Not gated: overall and dimension scores (timeliness reads the clock, see
  [findings](findings.md) G1). They are kept in the stored reports.

## Publication

One transaction on the runner's connection:

1. profile and assert
2. decide publication and insert the evaluation with both its outcome and
   that decision
3. if publishing: replace both views, insert the event, move the pointer
4. commit

Who may publish: `run` and `resume` publish an accepted run only while its
base is still active (otherwise `stale_conflict`); `reevaluate` never
publishes; `promote` publishes an accepted run deliberately, with a reason.

An exception rolls back and is then recorded as an `operational_error`
evaluation in a new transaction. Exports are written after commit and can be
regenerated with `export`.

## Evidence

| Table | Holds |
| --- | --- |
| `control.runs` | batch ID and digest, manifest, candidate, base publication, contract version and digests, versions |
| `control.run_stages` | stage history per run |
| `control.evaluations` | policy and contract digests, projections, Arrow schemas, reports, gate results, SQL results, outcome, publication decision |
| `control.publications` | publish, promote and rollback events with previous target and reason |
| `control.active_publication` | current target per dataset group |

Rows are appended, never updated. Source digests cover retained input bytes only.
Nothing here attests against changes made to the database outside the runner.

## Limits

- One runner per workspace (OS lock); readers open the database after it exits.
- Full snapshots only; no incremental loads.
- One dataset contract, customers and orders, versioned. Not a general schema
  framework.
