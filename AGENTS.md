# AGENTS.md

## Boundaries

- dataprof decides its own checks. Do not reimplement or reinterpret its
  policy semantics here; a missing capability goes to `docs/findings.md`.
- dlt loads. Do not wrap `pipeline.run()` in an outer transaction.
- Every SQL identifier comes from the contract or a generated run ID and
  passes `contract.identifier()`. Source values are parameters.
- Ledger rows are appended, never updated.
- No network after `uv sync`.

## Commands

```bash
uv sync --locked
uv run ruff format --check .
uv run ruff check .
uv run pytest -q
uv run python -m pipeline_reference demo --workspace .runs/demo
```

Crash tests run the CLI in subprocesses with `PIPELINE_REFERENCE_FAULT`
set (see `src/pipeline_reference/faults.py`).

## Changes

- A new rule needs a scenario in `scenarios.py` that it catches.
- A dependency change follows `docs/compatibility.md`.
- No AI attribution in commits or PRs.
