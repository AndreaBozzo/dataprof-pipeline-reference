"""The control schema: runs, evaluations, publications and the active pointer.

DuckDB holds the authoritative record. Rows are appended; an evaluation or a
publication event is never updated after it is written. Values are JSON text
in VARCHAR columns, so no extension has to be downloaded to read them.
"""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb

SCHEMA_VERSION = 2

MIGRATIONS: dict[int, list[str]] = {
    1: [
        "CREATE SCHEMA IF NOT EXISTS control",
        "CREATE SCHEMA IF NOT EXISTS published",
        """CREATE TABLE control.runs (
            run_id VARCHAR PRIMARY KEY,
            dataset_group VARCHAR NOT NULL,
            source_batch_id VARCHAR NOT NULL,
            source_digest VARCHAR NOT NULL,
            repairs_batch_id VARCHAR,
            retry_of VARCHAR,
            contract_version INTEGER NOT NULL,
            candidate_schema VARCHAR NOT NULL,
            base_event_id VARCHAR,
            manifest VARCHAR NOT NULL,
            versions VARCHAR NOT NULL,
            started_at TIMESTAMPTZ NOT NULL
        )""",
        # Stage changes are appended, so the history of a run is kept.
        """CREATE TABLE control.run_stages (
            run_id VARCHAR NOT NULL,
            seq INTEGER NOT NULL,
            stage VARCHAR NOT NULL,
            detail VARCHAR,
            recorded_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (run_id, seq)
        )""",
        """CREATE TABLE control.evaluations (
            evaluation_id VARCHAR PRIMARY KEY,
            run_id VARCHAR NOT NULL,
            seq INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            outcome VARCHAR NOT NULL,
            policy_version INTEGER,
            policy_digest VARCHAR,
            policy VARCHAR,
            projections VARCHAR,
            arrow_schemas VARCHAR,
            reports VARCHAR,
            gates VARCHAR,
            sql_assertions VARCHAR,
            detail VARCHAR,
            versions VARCHAR NOT NULL
        )""",
        """CREATE TABLE control.publications (
            event_id VARCHAR PRIMARY KEY,
            dataset_group VARCHAR NOT NULL,
            action VARCHAR NOT NULL,
            target_run_id VARCHAR NOT NULL,
            evaluation_id VARCHAR NOT NULL,
            previous_event_id VARCHAR,
            previous_run_id VARCHAR,
            committed_at TIMESTAMPTZ NOT NULL,
            reason VARCHAR
        )""",
        """CREATE TABLE control.active_publication (
            dataset_group VARCHAR PRIMARY KEY,
            run_id VARCHAR NOT NULL,
            event_id VARCHAR NOT NULL
        )""",
        "CREATE TABLE control.schema_version (version INTEGER NOT NULL, applied_at TIMESTAMPTZ)",
    ],
    # 0.1.1: the quality outcome and the publication decision are separate
    # facts, and a run records which contract files it was loaded under.
    # Rows written by version 1 keep NULL here: their outcome column may say
    # `stale_conflict`, and readers treat that as the old combined value.
    2: [
        "ALTER TABLE control.evaluations ADD COLUMN publication VARCHAR",
        "ALTER TABLE control.evaluations ADD COLUMN columns_digest VARCHAR",
        "ALTER TABLE control.evaluations ADD COLUMN dlt_schema_digest VARCHAR",
        "ALTER TABLE control.runs ADD COLUMN columns_digest VARCHAR",
        "ALTER TABLE control.runs ADD COLUMN dlt_schema_digest VARCHAR",
    ],
}

# Stages a run can be left in when the process stops before ready. A retry of
# such a run loads a fresh candidate; it never resumes the old one.
RETRYABLE = {"created", "loading", "load_failed"}


def now() -> datetime:
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    stamp = now().strftime("%Y%m%dt%H%M%S")
    return f"{prefix}{stamp}_{secrets.token_hex(3)}"


def migrate(conn: duckdb.DuckDBPyConnection) -> None:
    exists = conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'control' AND table_name = 'schema_version'"
    ).fetchone()[0]
    current = 0
    if exists:
        current = conn.execute("SELECT max(version) FROM control.schema_version").fetchone()[0]
    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"workspace ledger is version {current}; this runner knows {SCHEMA_VERSION}"
        )
    for version in range(current + 1, SCHEMA_VERSION + 1):
        conn.begin()
        try:
            for statement in MIGRATIONS[version]:
                conn.execute(statement)
            conn.execute("INSERT INTO control.schema_version VALUES (?, ?)", [version, now()])
            conn.commit()
        except BaseException:
            conn.rollback()
            raise


@dataclass(frozen=True)
class Run:
    run_id: str
    dataset_group: str
    source_batch_id: str
    source_digest: str
    repairs_batch_id: str | None
    retry_of: str | None
    contract_version: int
    candidate_schema: str
    base_event_id: str | None
    manifest: dict[str, Any]
    #: Digests of the contract files the run was loaded under; None on runs
    #: recorded before 0.1.1.
    columns_digest: str | None
    dlt_schema_digest: str | None
    stage: str


RUN_COLUMNS = (
    "r.run_id, r.dataset_group, r.source_batch_id, r.source_digest, r.repairs_batch_id, "
    "r.retry_of, r.contract_version, r.candidate_schema, r.base_event_id, r.manifest, "
    "r.columns_digest, r.dlt_schema_digest"
)
LATEST_STAGE = (
    "(SELECT stage FROM control.run_stages s WHERE s.run_id = r.run_id ORDER BY seq DESC LIMIT 1)"
)


def create_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    dataset_group: str,
    manifest: dict[str, Any],
    source_digest: str,
    contract_version: int,
    columns_digest: str,
    dlt_schema_digest: str | None,
    versions: dict[str, Any],
    retry_of: str | None = None,
) -> Run:
    """Record a run and the publication it starts from, in one transaction."""
    run_id = new_id("r")
    candidate = f"cand_{run_id}"
    conn.begin()
    try:
        base = active(conn, dataset_group)
        conn.execute(
            "INSERT INTO control.runs (run_id, dataset_group, source_batch_id, source_digest, "
            "repairs_batch_id, retry_of, contract_version, candidate_schema, base_event_id, "
            "manifest, versions, started_at, columns_digest, dlt_schema_digest) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run_id,
                dataset_group,
                manifest["source_batch_id"],
                source_digest,
                manifest.get("repairs_batch_id"),
                retry_of,
                contract_version,
                candidate,
                base[1] if base else None,
                json.dumps(manifest, sort_keys=True),
                json.dumps(versions, sort_keys=True),
                now(),
                columns_digest,
                dlt_schema_digest,
            ],
        )
        _stage(conn, run_id, "created", None)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return get_run(conn, run_id)


def _stage(conn: duckdb.DuckDBPyConnection, run_id: str, stage: str, detail: Any) -> None:
    seq = conn.execute(
        "SELECT coalesce(max(seq), 0) + 1 FROM control.run_stages WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO control.run_stages VALUES (?, ?, ?, ?, ?)",
        [run_id, seq, stage, None if detail is None else json.dumps(detail, sort_keys=True), now()],
    )


def set_stage(conn: duckdb.DuckDBPyConnection, run_id: str, stage: str, detail: Any = None) -> None:
    """Append a stage change in its own transaction."""
    conn.begin()
    try:
        _stage(conn, run_id, stage, detail)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def get_run(conn: duckdb.DuckDBPyConnection, run_id: str) -> Run:
    row = conn.execute(
        f"SELECT {RUN_COLUMNS}, {LATEST_STAGE} FROM control.runs r WHERE r.run_id = ?", [run_id]
    ).fetchone()
    if row is None:
        raise LookupError(f"no run {run_id}")
    values = list(row)
    values[9] = json.loads(values[9])
    return Run(*values)


def runs_for_batch(conn: duckdb.DuckDBPyConnection, group: str, batch_id: str) -> list[Run]:
    rows = conn.execute(
        "SELECT r.run_id FROM control.runs r WHERE dataset_group = ? AND source_batch_id = ? "
        "ORDER BY started_at, run_id",
        [group, batch_id],
    ).fetchall()
    return [get_run(conn, row[0]) for row in rows]


def all_runs(conn: duckdb.DuckDBPyConnection, limit: int = 20) -> list[Run]:
    rows = conn.execute(
        "SELECT run_id FROM control.runs ORDER BY started_at DESC, run_id DESC LIMIT ?", [limit]
    ).fetchall()
    return [get_run(conn, row[0]) for row in rows]


def active(conn: duckdb.DuckDBPyConnection, group: str) -> tuple[str, str] | None:
    """(run_id, event_id) of the active publication, or None before the first."""
    row = conn.execute(
        "SELECT run_id, event_id FROM control.active_publication WHERE dataset_group = ?",
        [group],
    ).fetchone()
    return (row[0], row[1]) if row else None


def insert_evaluation(
    conn: duckdb.DuckDBPyConnection, run_id: str, outcome: str, **fields: Any
) -> str:
    """Append an evaluation; caller owns the transaction."""
    evaluation_id = new_id("e")
    seq = conn.execute(
        "SELECT coalesce(max(seq), 0) + 1 FROM control.evaluations WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    encoded = {
        key: (None if fields.get(key) is None else json.dumps(fields[key], sort_keys=True))
        for key in (
            "policy",
            "projections",
            "arrow_schemas",
            "reports",
            "gates",
            "sql_assertions",
            "detail",
        )
    }
    conn.execute(
        f"INSERT INTO control.evaluations ({', '.join(EVALUATION_FIELDS)}) "
        f"VALUES ({', '.join('?' * len(EVALUATION_FIELDS))})",
        [
            evaluation_id,
            run_id,
            seq,
            now(),
            outcome,
            fields.get("policy_version"),
            fields.get("policy_digest"),
            encoded["policy"],
            encoded["projections"],
            encoded["arrow_schemas"],
            encoded["reports"],
            encoded["gates"],
            encoded["sql_assertions"],
            encoded["detail"],
            json.dumps(fields["versions"], sort_keys=True),
            fields.get("publication"),
            fields.get("columns_digest"),
            fields.get("dlt_schema_digest"),
        ],
    )
    return evaluation_id


EVALUATION_FIELDS = (
    "evaluation_id",
    "run_id",
    "seq",
    "created_at",
    "outcome",
    "policy_version",
    "policy_digest",
    "policy",
    "projections",
    "arrow_schemas",
    "reports",
    "gates",
    "sql_assertions",
    "detail",
    "versions",
    #: What happened to publication: published, stale_conflict,
    #: not_requested, already_active or not_eligible.
    "publication",
    "columns_digest",
    "dlt_schema_digest",
)
JSON_FIELDS = {
    "policy",
    "projections",
    "arrow_schemas",
    "reports",
    "gates",
    "sql_assertions",
    "detail",
    "versions",
}


def evaluations(conn: duckdb.DuckDBPyConnection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"SELECT {', '.join(EVALUATION_FIELDS)} FROM control.evaluations "
        "WHERE run_id = ? ORDER BY seq",
        [run_id],
    ).fetchall()
    result = []
    for row in rows:
        record = dict(zip(EVALUATION_FIELDS, row, strict=True))
        for key in JSON_FIELDS:
            if record[key] is not None:
                record[key] = json.loads(record[key])
        result.append(record)
    return result


def latest_evaluation(conn: duckdb.DuckDBPyConnection, run_id: str) -> dict[str, Any] | None:
    found = evaluations(conn, run_id)
    return found[-1] if found else None


def publication(conn: duckdb.DuckDBPyConnection, event_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT event_id, dataset_group, action, target_run_id, evaluation_id, "
        "previous_event_id, previous_run_id, committed_at, reason "
        "FROM control.publications WHERE event_id = ?",
        [event_id],
    ).fetchone()
    if row is None:
        return None
    keys = (
        "event_id",
        "dataset_group",
        "action",
        "target_run_id",
        "evaluation_id",
        "previous_event_id",
        "previous_run_id",
        "committed_at",
        "reason",
    )
    return dict(zip(keys, row, strict=True))


def publications(conn: duckdb.DuckDBPyConnection, group: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT event_id FROM control.publications WHERE dataset_group = ? "
        "ORDER BY committed_at, event_id",
        [group],
    ).fetchall()
    return [publication(conn, row[0]) for row in rows]


def events_for_run(conn: duckdb.DuckDBPyConnection, run_id: str) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            "SELECT event_id FROM control.publications WHERE target_run_id = ? "
            "ORDER BY committed_at",
            [run_id],
        ).fetchall()
    ]
