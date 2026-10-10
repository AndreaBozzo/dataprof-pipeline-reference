"""Publication: the two consumer views, the active pointer and the audit event.

Every function here runs inside a transaction the caller opened, so the views,
the pointer and the event commit together or not at all.
"""

from __future__ import annotations

import duckdb

from . import faults, ledger
from .contract import Contract, identifier


def replace_views(conn: duckdb.DuckDBPyConnection, contract: Contract, schema: str) -> None:
    for index, (table, spec) in enumerate(contract.tables.items()):
        conn.execute(
            f"CREATE OR REPLACE VIEW published.{identifier(table)} AS {spec.projection(schema)}"
        )
        if index == 0:
            faults.raise_at("between_views")


def publish(
    conn: duckdb.DuckDBPyConnection,
    contract: Contract,
    run: ledger.Run,
    evaluation_id: str,
    *,
    action: str = "publish",
    reason: str | None = None,
) -> str:
    """Point both views at ``run``'s candidate and record the event.

    The caller has already compared the active publication with the one its
    decision was based on, in this same transaction; with one writer per
    workspace nothing can move it in between.
    """
    group = contract.dataset_group
    current = ledger.active(conn, group)
    replace_views(conn, contract, run.candidate_schema)
    event_id = ledger.new_id("p")
    conn.execute(
        "INSERT INTO control.publications VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            event_id,
            group,
            action,
            run.run_id,
            evaluation_id,
            current[1] if current else None,
            current[0] if current else None,
            ledger.now(),
            reason,
        ],
    )
    conn.execute("DELETE FROM control.active_publication WHERE dataset_group = ?", [group])
    conn.execute(
        "INSERT INTO control.active_publication VALUES (?, ?, ?)", [group, run.run_id, event_id]
    )
    return event_id
