"""Command line: ``python -m pipeline_reference <command>``.

Exit codes: 0 accepted or done, 1 rejected, 2 inconclusive, 3 operational,
input or stale-target error. These are this project's codes, not dataprof's.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

import yaml

from . import scenarios
from .contract import load_contract
from .source import write_snapshot
from .workflow import (
    ACCEPTED,
    REJECTED,
    Result,
    Workspace,
    WorkspaceLocked,
    export,
    reevaluate,
    resume,
    rollback,
    run_snapshot,
    status,
)

OPERATIONAL_EXIT = 3


def _emit(result: Result) -> int:
    for line in result.lines:
        print(line)
    return result.exit_code


def generate(
    scenario: str,
    out: Path,
    batch_id: str,
    *,
    seed: int = scenarios.SEED,
    customers: int = 1_000,
    orders: int = 10_000,
    repairs: str | None = None,
) -> Path:
    rows = scenarios.build(scenario, seed=seed, customers=customers, orders=orders)
    write_snapshot(
        out,
        rows,
        load_contract(),
        batch_id,
        generator={
            "name": "pipeline_reference.scenarios",
            "version": 1,
            "scenario": scenario,
            "seed": seed,
            "customers": customers,
            "orders": orders,
        },
        repairs_batch_id=repairs,
    )
    return out


def _published_summary(ws: Workspace) -> tuple[str | None, list[tuple]]:
    from . import ledger

    current = ledger.active(ws.db, load_contract().dataset_group)
    if current is None:
        return None, []
    rows = ws.db.execute(
        "SELECT currency, count(*) AS orders, sum(amount) AS amount "
        "FROM published.orders GROUP BY currency ORDER BY currency"
    ).fetchall()
    return current[0], rows


def demo(workspace: Path) -> int:
    """Healthy, rejected, repaired; exit 0 only when every expected outcome was seen."""
    if (workspace / "reference.duckdb").exists():
        print(f"{workspace} already holds a ledger; the demo needs a fresh workspace")
        return OPERATIONAL_EXIT
    inputs = workspace / "demo-inputs"
    second_seed = scenarios.SEED + 1
    steps = [
        ("batch-001", "healthy", scenarios.SEED, None, ACCEPTED),
        ("batch-002", "null_customer_ids", second_seed, None, REJECTED),
        ("batch-003", "healthy", second_seed, "batch-002", ACCEPTED),
    ]
    seen_ok = True
    published_runs: list[str] = []
    with Workspace(workspace) as ws:
        for batch, scenario, seed, repairs, expected in steps:
            title = f"{batch}: {scenarios.SCENARIOS[scenario].description}"
            if repairs:
                title += f", repairing {repairs}"
            print(f"\n== {title}")
            snapshot = generate(scenario, inputs / batch, batch, seed=seed, repairs=repairs)
            result = run_snapshot(ws, snapshot)
            _emit(result)
            target, rows = _published_summary(ws)
            if result.outcome == ACCEPTED:
                published_runs.append(result.run_id)
            expected_target = published_runs[-1] if published_runs else None
            print("published.orders by currency:")
            for currency, count, amount in rows:
                print(f"  {currency}  {count:>6} orders  {amount:>14}")
            if result.outcome != expected:
                print(f"!! expected {expected}, got {result.outcome}")
                seen_ok = False
            if target != expected_target:
                print(f"!! expected published run {expected_target}, got {target}")
                seen_ok = False
            if target is not None and sum(r[1] for r in rows) != 10_000:
                print("!! published.orders does not hold the accepted batch's 10000 orders")
                seen_ok = False
        print()
        _emit(status(ws))
    print("\ndemo: all expected outcomes observed" if seen_ok else "\ndemo: FAILED")
    return 0 if seen_ok else 1


def bootstrap_contract() -> int:
    from .ingest import bootstrap_schema

    contract = load_contract()
    with tempfile.TemporaryDirectory() as tmp:
        snapshot = write_snapshot(
            Path(tmp),
            scenarios.build("healthy", customers=50, orders=200),
            contract,
            "bootstrap",
        )
        schema = bootstrap_schema(contract, snapshot)
    contract.schema_path.write_text(
        yaml.safe_dump(schema, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
        newline="\n",
    )
    print(f"wrote {contract.schema_path}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline_reference")
    sub = parser.add_subparsers(dest="command", required=True)

    def with_ws(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
        p.add_argument("--workspace", type=Path, required=True)
        return p

    def with_cap(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--profile-max-rows",
            type=int,
            default=None,
            help="profile at most this many rows; full_source checks then stay inconclusive",
        )

    with_ws(sub.add_parser("demo", help="healthy, rejected and repaired batches end to end"))
    p = with_ws(sub.add_parser("run", help="load, gate and maybe publish one snapshot"))
    p.add_argument("--snapshot", type=Path, required=True)
    with_cap(p)
    with_ws(sub.add_parser("status", help="active publication, runs and events"))
    p = with_ws(sub.add_parser("resume", help="continue an interrupted run"))
    p.add_argument("run_id")
    with_cap(p)
    p = with_ws(sub.add_parser("reevaluate", help="append a new evaluation of a ready run"))
    p.add_argument("run_id")
    with_cap(p)
    p = with_ws(sub.add_parser("rollback", help="republish an earlier accepted publication"))
    p.add_argument("publication_id")
    p.add_argument("--reason", required=True)
    p = with_ws(sub.add_parser("export", help="rewrite a run's derived report files"))
    p.add_argument("run_id")
    p = sub.add_parser("generate", help="write a fixture snapshot")
    p.add_argument("--scenario", choices=sorted(scenarios.SCENARIOS), default="healthy")
    p.add_argument("--batch-id", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--seed", type=int, default=scenarios.SEED)
    p.add_argument("--customers", type=int, default=1_000)
    p.add_argument("--orders", type=int, default=10_000)
    p.add_argument("--repairs", default=None, help="batch ID this snapshot repairs")
    sub.add_parser("bootstrap-contract", help="maintainer: regenerate the stored dlt schema")

    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            generate(
                args.scenario,
                args.out,
                args.batch_id,
                seed=args.seed,
                customers=args.customers,
                orders=args.orders,
                repairs=args.repairs,
            )
            print(f"wrote {args.out}")
            return 0
        if args.command == "bootstrap-contract":
            return bootstrap_contract()
        if args.command == "demo":
            return demo(args.workspace)
        with Workspace(args.workspace) as ws:
            if args.command == "run":
                return _emit(run_snapshot(ws, args.snapshot, max_rows=args.profile_max_rows))
            if args.command == "status":
                return _emit(status(ws))
            if args.command == "resume":
                return _emit(resume(ws, args.run_id, max_rows=args.profile_max_rows))
            if args.command == "reevaluate":
                return _emit(reevaluate(ws, args.run_id, max_rows=args.profile_max_rows))
            if args.command == "rollback":
                return _emit(rollback(ws, args.publication_id, args.reason))
            if args.command == "export":
                return _emit(export(ws, args.run_id))
    except WorkspaceLocked as exc:
        print(f"error: {exc}", file=sys.stderr)
        return OPERATIONAL_EXIT
    parser.error(f"unknown command {args.command}")
    return OPERATIONAL_EXIT


if __name__ == "__main__":
    sys.exit(main())
