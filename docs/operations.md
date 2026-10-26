# Operations

All commands take `--workspace DIR`. Exit codes: 0 accepted or done, 1
rejected, 2 inconclusive, 3 operational, input or stale-target error. Usage
errors and unexpected failures also exit 3.

## Bring your own snapshot

A snapshot directory holds `customers.jsonl`, `orders.jsonl` and
`manifest.json`. `generate` writes one (contract v1) to copy from:

```bash
uv run python -m pipeline_reference generate --batch-id batch-001 --out inputs/batch-001
```

Amounts are decimal strings, timestamps RFC 3339 in UTC. The manifest names
its `contract_version`, and each file's `path` and lowercase hex `sha256`; a
manifest that is malformed in any field is an input error. A batch ID always
means the same bytes; a fix gets a new ID and `repairs_batch_id` naming the
old one.

## Volume floor

`min_rows` in each table's dataprof check rejects a snapshot below 100
customers or 1,000 orders. It catches an extract that stopped early (`truncated_snapshot`
scenario). Set it from the volume your source delivers; it is absolute, not
relative to the last publication.

## Recovery

| State (`status`) | Action |
| --- | --- |
| stage `created`, `loading` or `load_failed` | `resume RUN_ID` loads a fresh candidate from the retained inputs |
| stage `ready`, no outcome | `resume RUN_ID` re-checks readiness and evaluates |
| outcome `operational_error` | fix the cause, then `resume RUN_ID` |
| outcome present | `resume` reports it; nothing is republished |
| publication `stale_conflict` | a newer publication exists; load a new batch, or `promote` this run on purpose |
| `blocked` | the batch broke the contract; fix it under a new batch ID |

Rerunning `run --snapshot` on a batch that stopped before ready also retries it.

## Evaluate, then promote

An evaluation records two things: the quality `outcome` the gates reached, and
what happened to `publication` (`published`, `stale_conflict`,
`not_requested`, `already_active`, `not_eligible`).

- `reevaluate RUN_ID` gates a ready run again and records the evidence. It
  never publishes, so it is safe for policy and dependency experiments.
- `promote RUN_ID --reason "..."` gates the run again and, if accepted,
  publishes it in the same transaction, whatever is active now. The event
  records the reason.

## Rollback

```bash
uv run python -m pipeline_reference status --workspace W
uv run python -m pipeline_reference rollback PUBLICATION_ID --reason "..." --workspace W
```

The target's candidate must still exist and match its contract. The event
keeps the target's original evaluation; nothing is re-validated.

## Policy change

Edit the policy, `reevaluate RUN_ID` to see what it decides, and `promote` if
that run should be published. Each evaluation stores the policy and its
digest, so old and new decisions stay comparable. A re-evaluation of the active
run never unpublishes it; it reports the change and leaves the decision to
`rollback`.

## Schema change

Columns and the stored dlt schema are fixed per contract version. A run
records the digests of both, and resume, reevaluate, promote and rollback
refuse a run whose contract files changed since. To change the schema, add a
version:

```bash
cp -r src/pipeline_reference/contracts/v1 src/pipeline_reference/contracts/v2
rm src/pipeline_reference/contracts/v2/schema.dlt.yaml
# edit v2/columns.json: contract_version 2 and the new columns
uv run python -m pipeline_reference bootstrap-contract --version 2 --fixture KNOWN_GOOD_V2_SNAPSHOT
```

Snapshots name their contract version; `run` loads that version, and older
runs keep theirs. `PIPELINE_REFERENCE_CONTRACTS` points the runner at another
contracts directory with the same layout.

## Backup and restore

With no runner active, copy the whole workspace: `reference.duckdb`,
`inputs/` (retained snapshots) and `dlt/`, together with the contract
versions its runs used. Restore by copying them back. The ledger refers to
retained inputs by run ID, so the database alone is not a complete backup.

## Cleanup

Not automated. Candidates, retained inputs and dlt pipeline directories grow
by one set per run. Safe to drop by hand: candidate schemas, `inputs/<run_id>`
and `dlt/cand_<run_id>` of runs that are not the active publication, not a
rollback target you may need, and not resumable.
