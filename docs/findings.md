# Findings

Glue this reference writes because a library lacks something. Each entry names
the evidence and the smallest upstream change, and the issue once filed.

## dataprof

| ID | Gap | Evidence | Possible change |
| --- | --- | --- | --- |
| G1 | Timeliness compares dates with the current clock; the reference date is internal. Gates on the overall score or timeliness drift as the calendar moves. | `TimelinessCalculator::new` uses `Utc::now()`; `with_reference_date` is crate-private (0.12.0) | Public `reference_date` option, recorded in the report (AndreaBozzo/dataprof#903) |
| G2 | No per-column uniqueness check. `max_duplicate_rows` counts entire rows, so a reused business key in otherwise distinct rows passes. | `tests/test_scenarios.py::test_duplicate_ids_are_distinct_rows_to_dlt` | `check(unique_columns=[...])` from the exact distinct counts already computed (AndreaBozzo/dataprof#904) |
| G3 | No row-count check. An empty table fails only through `require_metrics`; a truncated batch passes. | `volume.*` assertions in `evaluate.py` | `check(min_rows=...)` (AndreaBozzo/dataprof#905) |
| G4 | The report drops the source type: Arrow `decimal128(12,2)` is reported as `Float`, `timestamp[us, tz=UTC]` as `Date`. | `arrow_schemas` stored beside each report in `control.evaluations` | Record the source physical type per column; would allow a column and type contract check (AndreaBozzo/dataprof#906) |
| G5 | A policy is keyword arguments; the reference serializes and digests it itself. | `Contract.policy_digest` | A policy object with load, serialize and digest; overlaps AndreaBozzo/dataprof#883 and AndreaBozzo/dataprof#849, not filed |
| G6 | Value-domain rules (`amount >= 0`, currency set) are SQL. Open question whether they belong in a profiler policy. | `range.*`, `domain.*` assertions | See below |
| G7 | A development build reports the last release's version, so evidence from it cannot be told apart from the release's. | `versions` in `control.evaluations`; `tests/test_compat.py` | Expose build identity (commit) in the module and the report (AndreaBozzo/dataprof#907) |

### G6: notes so far

- A range bound can be decided from statistics the report already holds
  (`stats.Numeric.min`), under the existing full-source and sampling rules.
  That fits a profile-derived check.
- Set membership needs the distinct values, which the report does not carry
  (text `most_frequent` is left empty in 0.12.0, AndreaBozzo/dataprof#758). That is a different
  capability.
- Still to gather: how often the two shapes appear in real pipelines, and how
  other profiling and validation tools draw the line.

## dlt

| ID | Gap | Evidence | Possible change |
| --- | --- | --- | --- |
| D1 | `schema_contract="freeze"` on a new dataset rejects tables the pipeline schema already knows; a stored schema must be imported per run. | `spikes/02_dlt_contracts.py` | dlt-hub/dlt#4539 |
| D2 | `pipeline.run()` changes `search_path` on a connection the caller passed in. `main` still resolves, so the risk is a same-named table being shadowed. | `spikes/01_route_and_transactions.py` | Not filed; low impact |
| D3 | A dataset named like the DuckDB catalog (the database file stem) fails with an ambiguous reference. dlt logs a warning first; the error then suggests a concurrent-pipeline retry. | bootstrap in `ingest.py` | Known: dlt-hub/dlt#176 and the warning |
| D4 | Without a contract, an uncoercible value loads as NULL plus a `__v_text` variant column. Documented behavior; listed because it hides type errors from a downstream null check. | `spikes/02_dlt_contracts.py` | None; the reference freezes the schema |

## DuckDB

None. Transactional view replacement and crash recovery behaved as needed
(`tests/test_publication.py`, `tests/test_recovery.py`).
