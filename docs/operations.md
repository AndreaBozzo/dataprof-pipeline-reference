# Operations

All commands take `--workspace DIR`. Exit codes: 0 accepted or done, 1
rejected, 2 inconclusive, 3 operational, input or stale-target error.

## Bring your own snapshot

A snapshot directory holds `customers.jsonl`, `orders.jsonl` and
`manifest.json`. `generate` writes one to copy from:

```bash
uv run python -m pipeline_reference generate --batch-id batch-001 --out inputs/batch-001
```

Amounts are decimal strings, timestamps RFC 3339 in UTC. The manifest's
`sha256` values must match the files. A batch ID always means the same bytes;
a fix gets a new ID and `repairs_batch_id` naming the old one.

## Recovery

| State (`status`) | Action |
| --- | --- |
| stage `created`, `loading` or `load_failed` | `resume RUN_ID` loads a fresh candidate from the retained inputs |
| stage `ready`, no outcome | `resume RUN_ID` re-checks readiness and evaluates |
| outcome `operational_error` | fix the cause, then `resume RUN_ID` |
| outcome present | `resume` reports it; nothing is republished |
| `stale_conflict` | a newer publication exists; load a new batch instead |
| `blocked` | the batch broke the contract; fix it under a new batch ID |

Rerunning `run --snapshot` on a batch that stopped before ready also retries it.

## Rollback

```bash
uv run python -m pipeline_reference status --workspace W
uv run python -m pipeline_reference rollback PUBLICATION_ID --reason "..." --workspace W
```

The target's candidate must still exist and match the contract. The event
keeps the target's original evaluation; nothing is re-validated.

## Policy change

Edit `contracts/v1/policy.json`, then `reevaluate RUN_ID`. Each evaluation
stores the policy and its digest, so old and new decisions stay comparable.
A re-evaluation of the active run never unpublishes it; it reports the change
and leaves the decision to `rollback`.

## Schema change

A new contract version: copy `contracts/v1` to `v2`, edit `columns.json`,
then regenerate the stored dlt schema with
`uv run python -m pipeline_reference bootstrap-contract`. Snapshots name their
contract version in the manifest.

## Backup and restore

With no runner active, copy the whole workspace: `reference.duckdb`,
`inputs/` (retained snapshots) and `dlt/`. Restore by copying it back. The
ledger refers to retained inputs by run ID, so the database alone is not a
complete backup.

## Cleanup

Not automated. Candidates, retained inputs and dlt pipeline directories grow
by one set per run. Safe to drop by hand: candidate schemas, `inputs/<run_id>`
and `dlt/cand_<run_id>` of runs that are not the active publication, not a
rollback target you may need, and not resumable.
