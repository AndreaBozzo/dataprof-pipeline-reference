# Findings

Glue this reference writes because a library lacks something. Each entry names
the evidence and the smallest upstream change, and the issue once filed.

## dataprof

| ID | Gap | Evidence | Possible change |
| --- | --- | --- | --- |
| G1 | Timeliness compares dates with the current clock; the reference date is internal. Gates on the overall score or timeliness drift as the calendar moves. | `TimelinessCalculator::new` uses `Utc::now()`; `with_reference_date` is crate-private (0.12.0) | Public `reference_date` option, recorded in the report (AndreaBozzo/dataprof#903) |
| G2 | No per-column uniqueness check. `max_duplicate_rows` counts entire rows, so a reused business key in otherwise distinct rows passes. | `tests/test_scenarios.py::test_duplicate_ids_are_distinct_rows_to_dlt` | `check(unique_columns=[...])` from the exact distinct counts already computed (AndreaBozzo/dataprof#904) |
| G3 | No row-count check. An empty table fails only through `require_metrics`; a truncated batch passes. | `truncated_snapshot` scenario | Shipped in 0.13.0 as `check(min_rows=...)` (AndreaBozzo/dataprof#905); policy v2 uses it and the SQL `volume.*` assertions are gone |
| G4 | The report drops the source type: Arrow `decimal128(12,2)` is reported as `Float`, `timestamp[us, tz=UTC]` as `Date`. | `arrow_schemas` stored beside each report in `control.evaluations` | Record the source physical type per column; would allow a column and type contract check (AndreaBozzo/dataprof#906) |
| G5 | A policy is keyword arguments; the reference serializes and digests it itself. | `Contract.policy_digest` | A policy object with load, serialize and digest; overlaps AndreaBozzo/dataprof#883 and AndreaBozzo/dataprof#849, not filed |
| G6 | Value-domain rules (`amount >= 0`, currency set) are SQL. | `range.*`, `domain.*` assertions | Bounds: AndreaBozzo/dataprof#908. Sets: stay SQL, see below |
| G7 | A development build reports the last release's version, so evidence from it cannot be told apart from the release's. | `versions` in `control.evaluations`; `tests/test_compat.py` | Shipped in 0.13.0: `generator` in every report and `capabilities().commit` (AndreaBozzo/dataprof#907); runs record it as `versions.dataprof_build` |
| G8 | Since 0.13.0 an absolute rule decides on the two-decimal value a check shows (AndreaBozzo/dataprof#1090). A 0% null limit passes a null rate below 0.005%, so the 0% limits on keys are exact only up to 20,000 rows. | `tests/test_scenarios.py::test_one_null_key_past_20000_rows_is_rejected` | Shipped in 0.13.1 as `check(max_null_count=...)` (AndreaBozzo/dataprof#1119); policy v3 uses it for every 0% limit |

### G6: bounds yes, sets no

- Bounds fit a profile-level check: a full scan reports exact `min` and `max`
  (`amount min=-1.0` on a planted negative), and a partial scan can still
  witness a violation. whylogs gates the same way (`greater_than_number`,
  `is_in_range` on a profile).
- Sets do not: the report holds no values, and a profile-derived set check is
  unsound at high cardinality. whylogs 1.6.4
  `frequent_strings_in_reference_set` passed with one invalid value among
  5,000 distinct valid ones; it caught the same value among 3. The sound
  tools test every row: dbt `accepted_values`, Soda `valid values`, Deequ
  `isContainedIn`.
- dataprof's own line (AndreaBozzo/dataprof#841): threshold policies over
  profile metrics, not arbitrary expectations.

## dlt

| ID | Gap | Evidence | Possible change |
| --- | --- | --- | --- |
| D1 | `schema_contract="freeze"` on a new dataset rejects tables the pipeline schema already knows; a stored schema must be imported per run. | `spikes/02_dlt_contracts.py` | By design: a new dataset resets the pipeline schema; the documented route is `import_schema_path` (dlt-hub/dlt#4539) |
| D2 | `pipeline.run()` changes `search_path` on a connection the caller passed in. `main` still resolves, so the risk is a same-named table being shadowed. | `spikes/01_route_and_transactions.py` | Not filed; low impact |
| D3 | A dataset named like the DuckDB catalog (the database file stem) fails with an ambiguous reference. dlt logs a warning first; the error then suggests a concurrent-pipeline retry. | bootstrap in `ingest.py` | Known: dlt-hub/dlt#176 and the warning |
| D4 | Without a contract, an uncoercible value loads as NULL plus a `__v_text` variant column. Documented behavior; listed because it hides type errors from a downstream null check. | `spikes/02_dlt_contracts.py` | None; the reference freezes the schema |

## DuckDB

None. Transactional view replacement and crash recovery behaved as needed
(`tests/test_publication.py`, `tests/test_recovery.py`).
