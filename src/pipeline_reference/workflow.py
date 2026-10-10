"""Orchestration: one runner, one workspace, one database."""

from __future__ import annotations

import contextlib
import json
import os
import platform
import subprocess
import tempfile
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

import duckdb
import portalocker

from . import faults, ingest, ledger, publish
from .contract import Contract, load_contract
from .evaluate import (
    ACCEPTED,
    INCONCLUSIVE,
    OPERATIONAL,
    REJECTED,
    STALE,
    Evidence,
    run_gates,
)
from .source import InputError, Snapshot, read_manifest, retain, verify

INPUT_ERROR = "input_error"
DONE = "done"
EXIT_CODES = {
    ACCEPTED: 0,
    DONE: 0,
    REJECTED: 1,
    INCONCLUSIVE: 2,
    STALE: 3,
    OPERATIONAL: 3,
    INPUT_ERROR: 3,
}
DATABASE = "reference.duckdb"


class WorkspaceLocked(RuntimeError):
    pass


@dataclass
class Result:
    outcome: str
    lines: list[str] = field(default_factory=list)
    run_id: str | None = None
    event_id: str | None = None

    @property
    def exit_code(self) -> int:
        return EXIT_CODES[self.outcome]


def versions() -> dict[str, Any]:
    found: dict[str, Any] = {"python": platform.python_version()}
    for name in ("dataprof", "dlt", "duckdb", "pyarrow", "portalocker"):
        found[name] = metadata.version(name)
    found["pipeline_reference"] = metadata.version("dataprof-pipeline-reference")
    found["commit"] = _commit()
    return found


def _commit() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


class Workspace:
    """Holds the workspace lock and the only writable connection.

    The lock is an OS lock on ``.lock``: a process that dies releases it, and
    the file existing says nothing about ownership.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.inputs = self.path / "inputs"
        self.pipelines = self.path / "dlt"
        self.exports = self.path / "exports"
        self._lock: portalocker.Lock | None = None
        self.conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> Workspace:
        self.path.mkdir(parents=True, exist_ok=True)
        lock = portalocker.Lock(
            str(self.path / ".lock"),
            mode="a",
            timeout=0,
            fail_when_locked=True,
            flags=portalocker.LockFlags.EXCLUSIVE | portalocker.LockFlags.NON_BLOCKING,
        )
        try:
            lock.acquire()
        except portalocker.exceptions.LockException as exc:
            raise WorkspaceLocked(f"{self.path} is in use by another runner") from exc
        self._lock = lock
        try:
            self.conn = duckdb.connect(str(self.path / DATABASE))
            self.conn.execute("SET TimeZone = 'UTC'")
            ledger.migrate(self.conn)
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *exc: object) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self._lock is not None:
            self._lock.release()
            self._lock = None

    @property
    def db(self) -> duckdb.DuckDBPyConnection:
        assert self.conn is not None, "workspace is not open"
        return self.conn

    def retained(self, run: ledger.Run) -> Snapshot:
        return Snapshot(self.inputs / run.run_id, run.manifest)


def describe_error(exc: BaseException) -> str:
    """The exception and its causes; a C-level error often carries its text in a cause."""
    return " <- ".join(repr(item) for item in ingest._chain(exc))


def _input_error(exc: BaseException) -> InputError | None:
    for item in ingest._chain(exc):
        if isinstance(item, InputError):
            return item
    return None


# -- run ---------------------------------------------------------------------


def run_snapshot(ws: Workspace, directory: Path, *, max_rows: int | None = None) -> Result:
    contract = load_contract()
    try:
        snapshot = read_manifest(Path(directory), contract)
        verify(snapshot)
    except InputError as exc:
        return Result(INPUT_ERROR, [f"input error: {exc}"])

    previous = ledger.runs_for_batch(ws.db, contract.dataset_group, snapshot.batch_id)
    for run in previous:
        if run.source_digest != snapshot.digest:
            return Result(
                INPUT_ERROR,
                [
                    f"input error: batch {snapshot.batch_id!r} was recorded by run {run.run_id} "
                    f"with digest {run.source_digest}; these files have {snapshot.digest}. "
                    "A repaired batch needs a new batch ID."
                ],
                run.run_id,
            )
    settled = [r for r in previous if r.stage not in ledger.RETRYABLE]
    if settled:
        result = describe(ws, contract, settled[-1])
        result.lines.insert(
            0,
            f"batch {snapshot.batch_id!r} already has run {settled[-1].run_id}; not loading it "
            "again (use `reevaluate` to apply a new policy or dependency set)",
        )
        return result
    retry_of = previous[-1].run_id if previous else None
    return _attempt(ws, contract, snapshot, retry_of=retry_of, max_rows=max_rows)


def _attempt(
    ws: Workspace,
    contract: Contract,
    snapshot: Snapshot,
    *,
    retry_of: str | None,
    max_rows: int | None,
) -> Result:
    run = ledger.create_run(
        ws.db,
        dataset_group=contract.dataset_group,
        manifest=snapshot.manifest,
        source_digest=snapshot.digest,
        contract_version=contract.version,
        versions=versions(),
        retry_of=retry_of,
    )
    header = [
        f"run {run.run_id}  batch {run.source_batch_id}  candidate {run.candidate_schema}",
    ]
    if retry_of:
        header.append(f"  retries {retry_of}, which stopped before its candidate was ready")
    try:
        retained = retain(snapshot, ws.inputs / run.run_id)
    except InputError as exc:
        ledger.set_stage(ws.db, run.run_id, INPUT_ERROR, {"error": str(exc)})
        return Result(INPUT_ERROR, header + [f"input error: {exc}"], run.run_id)

    ledger.set_stage(ws.db, run.run_id, "loading")
    try:
        loaded = ingest.load(ws.db, retained, contract, run.candidate_schema, ws.pipelines)
    except ingest.ContractViolation as exc:
        ledger.set_stage(ws.db, run.run_id, "blocked", {"problems": [str(exc)]})
        return Result(
            REJECTED,
            header + [f"  blocked during load: {exc}", "outcome: rejected", *_published(ws)],
            run.run_id,
        )
    except Exception as exc:
        found = _input_error(exc)
        if found is not None:
            ledger.set_stage(ws.db, run.run_id, INPUT_ERROR, {"error": str(found)})
            return Result(INPUT_ERROR, header + [f"input error: {found}"], run.run_id)
        ledger.set_stage(ws.db, run.run_id, "load_failed", {"error": describe_error(exc)})
        return Result(
            OPERATIONAL,
            header + [f"operational error during load: {describe_error(exc)}", *_published(ws)],
            run.run_id,
        )

    problems = loaded.blocked + ingest.check_ready(
        ws.db, retained, contract, run.candidate_schema, loaded.load_ids
    )
    detail = {"load_ids": loaded.load_ids, "absent_keys": loaded.absent_keys}
    if problems:
        ledger.set_stage(ws.db, run.run_id, "blocked", {**detail, "problems": problems})
        lines = [f"  blocked before evaluation: {p}" for p in problems]
        return Result(REJECTED, header + lines + ["outcome: rejected", *_published(ws)], run.run_id)
    ledger.set_stage(ws.db, run.run_id, "ready", detail)
    faults.crash("crash_after_ready")
    result = evaluate_and_publish(ws, contract, ledger.get_run(ws.db, run.run_id), max_rows)
    result.lines[:0] = header
    return result


# -- evaluation and publication ----------------------------------------------


def evaluate_and_publish(
    ws: Workspace, contract: Contract, run: ledger.Run, max_rows: int | None
) -> Result:
    conn = ws.db
    runtime = versions()
    conn.begin()
    try:
        evidence = run_gates(conn, contract, run.candidate_schema, max_rows=max_rows)
        outcome = evidence.outcome()
        current = ledger.active(conn, contract.dataset_group)
        is_active = current is not None and current[0] == run.run_id
        notes = []
        if outcome == ACCEPTED and not is_active:
            current_event = current[1] if current else None
            if current_event != run.base_event_id:
                outcome = STALE
                notes.append(
                    f"gates accepted, but publication {current_event} replaced "
                    f"{run.base_event_id} after this run started; not publishing"
                )
        elif is_active:
            notes.append("this run is the active publication; the evaluation is recorded only")
            if outcome != ACCEPTED:
                notes.append("the published data no longer passes; consider `rollback`")
        evaluation_id = ledger.insert_evaluation(
            conn,
            run.run_id,
            outcome,
            policy_version=contract.policy["policy_version"],
            policy_digest=contract.policy_digest,
            policy=contract.policy,
            projections=evidence.projections,
            arrow_schemas=evidence.arrow_schemas,
            reports=evidence.reports,
            gates=evidence.gates,
            sql_assertions=evidence.sql_assertions,
            detail={"profile_options": evidence.profile_options, "notes": notes},
            versions=runtime,
        )
        event_id = None
        if outcome == ACCEPTED and not is_active:
            event_id = publish.publish(conn, contract, run, evaluation_id)
        faults.crash("crash_before_commit")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        recorded = _record_operational(ws, run, exc, runtime)
        lines = [f"operational error during evaluation: {describe_error(exc)}"]
        if not recorded:
            lines.append("  the error could not be recorded in the ledger")
        return Result(OPERATIONAL, lines + _published(ws), run.run_id)
    faults.crash("crash_after_commit")
    lines = format_evidence(evidence) + [f"  note: {n}" for n in notes]
    lines += [f"outcome: {outcome}", *_published(ws)]
    try:
        export_evaluation(ws, run.run_id, evaluation_id)
    except Exception as exc:  # the ledger is authoritative; `export` can redo this
        lines.append(f"  warning: export failed ({exc!r}); run `export {run.run_id}`")
    return Result(outcome, lines, run.run_id, event_id)


def _record_operational(
    ws: Workspace, run: ledger.Run, exc: BaseException, runtime: dict[str, Any]
) -> bool:
    try:
        ws.db.begin()
        ledger.insert_evaluation(
            ws.db, run.run_id, OPERATIONAL, detail={"error": describe_error(exc)}, versions=runtime
        )
        ws.db.commit()
        return True
    except Exception:
        with contextlib.suppress(Exception):
            ws.db.rollback()
        return False


# -- resume, reevaluate, rollback ---------------------------------------------


def _ready_detail(ws: Workspace, run_id: str) -> dict[str, Any]:
    row = ws.db.execute(
        "SELECT detail FROM control.run_stages WHERE run_id = ? AND stage = 'ready' "
        "ORDER BY seq DESC LIMIT 1",
        [run_id],
    ).fetchone()
    return json.loads(row[0]) if row else {}


def _recheck(ws: Workspace, contract: Contract, run: ledger.Run) -> list[str]:
    detail = _ready_detail(ws, run.run_id)
    return ingest.check_ready(
        ws.db, ws.retained(run), contract, run.candidate_schema, detail.get("load_ids", [])
    )


def resume(ws: Workspace, run_id: str, *, max_rows: int | None = None) -> Result:
    contract = load_contract()
    try:
        run = ledger.get_run(ws.db, run_id)
    except LookupError as exc:
        return Result(INPUT_ERROR, [str(exc)])
    if run.stage in ledger.RETRYABLE:
        retained = ws.retained(run)
        try:
            verify(retained)
        except InputError as exc:
            return Result(
                INPUT_ERROR,
                [
                    f"run {run_id} stopped at {run.stage} and its retained input is unusable "
                    f"({exc}); start again with `run --snapshot`"
                ],
                run_id,
            )
        later = [
            r
            for r in ledger.runs_for_batch(ws.db, contract.dataset_group, run.source_batch_id)
            if r.retry_of == run_id
        ]
        if later:
            return Result(INPUT_ERROR, [f"run {run_id} was already retried as {later[-1].run_id}"])
        # The retry copies the old attempt's retained files into its own directory.
        return _attempt(ws, contract, retained, retry_of=run_id, max_rows=max_rows)
    if run.stage != "ready":
        return describe(ws, contract, run)
    latest = ledger.latest_evaluation(ws.db, run_id)
    if latest is not None and latest["outcome"] != OPERATIONAL:
        result = describe(ws, contract, run)
        result.lines.insert(0, f"run {run_id} was already decided; nothing republished")
        return result
    problems = _recheck(ws, contract, run)
    if problems:
        return Result(
            OPERATIONAL,
            [f"run {run_id} is no longer evaluable: {p}" for p in problems],
            run_id,
        )
    result = evaluate_and_publish(ws, contract, run, max_rows)
    result.lines.insert(0, f"resumed run {run_id}")
    return result


def reevaluate(ws: Workspace, run_id: str, *, max_rows: int | None = None) -> Result:
    """Append a new evaluation of a ready candidate; earlier ones are kept as they are."""
    contract = load_contract()
    try:
        run = ledger.get_run(ws.db, run_id)
    except LookupError as exc:
        return Result(INPUT_ERROR, [str(exc)])
    if run.stage != "ready":
        return Result(INPUT_ERROR, [f"run {run_id} is {run.stage}; only ready runs re-evaluate"])
    problems = _recheck(ws, contract, run)
    if problems:
        return Result(OPERATIONAL, [f"run {run_id}: {p}" for p in problems], run_id)
    result = evaluate_and_publish(ws, contract, run, max_rows)
    result.lines.insert(0, f"re-evaluated run {run_id}")
    return result


def rollback(ws: Workspace, event_id: str, reason: str) -> Result:
    """Republish the candidate of an earlier accepted publication, with an audit event."""
    contract = load_contract()
    target = ledger.publication(ws.db, event_id)
    if target is None:
        return Result(INPUT_ERROR, [f"no publication {event_id}"])
    current = ledger.active(ws.db, contract.dataset_group)
    if current is not None and current[0] == target["target_run_id"]:
        return Result(
            INPUT_ERROR, [f"run {target['target_run_id']} is already the active publication"]
        )
    run = ledger.get_run(ws.db, target["target_run_id"])
    problems = _recheck(ws, contract, run)
    if problems:
        return Result(
            OPERATIONAL,
            [f"cannot roll back to {event_id}: {p}" for p in problems],
            run.run_id,
        )
    conn = ws.db
    conn.begin()
    try:
        new_event = publish.publish(
            conn,
            contract,
            run,
            target["evaluation_id"],
            action="rollback",
            reason=reason,
        )
        faults.crash("crash_before_commit")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return Result(OPERATIONAL, [f"rollback failed: {exc!r}", *_published(ws)])
    return Result(
        DONE,
        [
            f"rolled back to run {run.run_id} (publication {event_id}) as event {new_event}",
            f"  reason: {reason}",
            "  the verdict shown for it is the one recorded when it was first accepted",
            *_published(ws),
        ],
        run.run_id,
        new_event,
    )


# -- reporting ---------------------------------------------------------------


def _published(ws: Workspace) -> list[str]:
    current = ledger.active(ws.db, load_contract().dataset_group)
    if current is None:
        return ["published: nothing has been published yet"]
    return [f"published: run {current[0]} (event {current[1]})"]


COMPARISONS = {"at_most": "<=", "at_least": ">="}


def _check_line(check: dict[str, Any]) -> str:
    status = {"passed": "passed", "failed": "FAILED", "not_evaluated": "not evaluated"}.get(
        check["status"], check["status"]
    )
    subject = check.get("column") or check.get("dimension") or ""
    expected = check.get("expected") or {}
    comparison = expected.get("comparison")
    if comparison in COMPARISONS:
        detail = f"observed {check.get('observed')}  expected {COMPARISONS[comparison]} "
        detail += str(expected.get("value"))
    else:
        detail = check.get("message", "")
    return f"    {status:<13} {check['code']} {subject}  {detail}".rstrip()


def format_evidence(evidence: Evidence | dict[str, Any]) -> list[str]:
    if isinstance(evidence, Evidence):
        gates, assertions = evidence.gates, evidence.sql_assertions
    else:
        gates, assertions = evidence.get("gates") or {}, evidence.get("sql_assertions") or []
    lines = []
    for table, gate in gates.items():
        lines.append(f"  dataprof {table}: {gate['verdict']}")
        lines.extend(_check_line(check) for check in gate["checks"])
    if assertions:
        lines.append("  sql:")
    for item in assertions:
        status = "passed" if item["passed"] else "FAILED"
        lines.append(
            f"    {status:<13} {item['id']}  observed {item['observed']}  "
            f"expected {item['comparator']} {item['threshold']}"
        )
    return lines


def describe(ws: Workspace, contract: Contract, run: ledger.Run) -> Result:
    lines = [f"run {run.run_id}  batch {run.source_batch_id}  stage {run.stage}"]
    latest = ledger.latest_evaluation(ws.db, run.run_id)
    if run.stage == "blocked":
        detail = ws.db.execute(
            "SELECT detail FROM control.run_stages WHERE run_id = ? ORDER BY seq DESC LIMIT 1",
            [run.run_id],
        ).fetchone()[0]
        for problem in json.loads(detail).get("problems", []):
            lines.append(f"  blocked: {problem}")
        outcome = REJECTED
    elif run.stage == INPUT_ERROR:
        outcome = INPUT_ERROR
    elif latest is None:
        outcome = OPERATIONAL
        lines.append("  not evaluated yet; `resume` continues it")
    else:
        lines += format_evidence(latest)
        outcome = latest["outcome"]
        for note in (latest.get("detail") or {}).get("notes", []):
            lines.append(f"  note: {note}")
        if outcome == OPERATIONAL:
            lines.append(f"  error: {(latest.get('detail') or {}).get('error')}")
    lines += [f"outcome: {outcome}", *_published(ws)]
    return Result(outcome, lines, run.run_id)


def status(ws: Workspace) -> Result:
    contract = load_contract()
    lines = _published(ws)
    runs = ledger.all_runs(ws.db)
    if runs:
        lines.append("recent runs:")
    for run in runs:
        latest = ledger.latest_evaluation(ws.db, run.run_id)
        outcome = latest["outcome"] if latest else "-"
        lines.append(
            f"  {run.run_id}  batch {run.source_batch_id:<12} stage {run.stage:<11} "
            f"outcome {outcome}"
        )
    events = ledger.publications(ws.db, contract.dataset_group)
    if events:
        lines.append("publications:")
    for event in events:
        reason = f"  reason: {event['reason']}" if event["reason"] else ""
        lines.append(
            f"  {event['event_id']}  {event['action']:<8} run {event['target_run_id']}{reason}"
        )
    return Result(DONE, lines)


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def export_evaluation(ws: Workspace, run_id: str, evaluation_id: str) -> Path:
    """Write the derived files for one evaluation; the ledger stays authoritative."""
    found = [e for e in ledger.evaluations(ws.db, run_id) if e["evaluation_id"] == evaluation_id]
    if not found:
        raise LookupError(f"no evaluation {evaluation_id} for run {run_id}")
    record = found[0]
    out = ws.exports / run_id / evaluation_id
    for table, report in (record["reports"] or {}).items():
        _write_atomic(out / f"{table}.report.json", json.dumps(report, indent=2) + "\n")
    decision = {k: v for k, v in record.items() if k != "reports"}
    decision["created_at"] = str(decision["created_at"])
    _write_atomic(out / "decision.json", json.dumps(decision, indent=2, sort_keys=True) + "\n")
    summary = format_evidence(record) + [f"outcome: {record['outcome']}"]
    _write_atomic(out / "summary.txt", "\n".join(summary) + "\n")
    return out


def export(ws: Workspace, run_id: str) -> Result:
    try:
        ledger.get_run(ws.db, run_id)
    except LookupError as exc:
        return Result(INPUT_ERROR, [str(exc)])
    written = [
        export_evaluation(ws, run_id, record["evaluation_id"])
        for record in ledger.evaluations(ws.db, run_id)
    ]
    if not written:
        return Result(INPUT_ERROR, [f"run {run_id} has no evaluation to export"])
    return Result(DONE, [f"exported {path}" for path in written], run_id)
