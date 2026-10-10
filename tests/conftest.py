from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from pipeline_reference import ledger, scenarios
from pipeline_reference.__main__ import generate
from pipeline_reference.workflow import Workspace

# Small enough to keep the suite quick, large enough that 8% and 2% are
# hundreds and tens of rows.
CUSTOMERS = 200
ORDERS = 2_000


@pytest.fixture
def snapshots(tmp_path: Path):
    def make(scenario: str, batch_id: str, *, seed: int = scenarios.SEED, **kw) -> Path:
        out = tmp_path / "inputs" / batch_id
        return generate(
            scenario, out, batch_id, seed=seed, customers=CUSTOMERS, orders=ORDERS, **kw
        )

    return make


@pytest.fixture
def ws_path(tmp_path: Path) -> Path:
    return tmp_path / "workspace"


@pytest.fixture
def ws(ws_path: Path) -> Iterator[Workspace]:
    with Workspace(ws_path) as workspace:
        yield workspace


def cli(ws_path: Path, *args: str, fault: str | None = None) -> subprocess.CompletedProcess:
    """Run the CLI in its own process, optionally with a fault point armed."""
    env = dict(os.environ)
    env.pop("PIPELINE_REFERENCE_FAULT", None)
    if fault:
        env["PIPELINE_REFERENCE_FAULT"] = fault
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pipeline_reference",
            args[0],
            "--workspace",
            str(ws_path),
            *args[1:],
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )


def published_state(ws: Workspace) -> dict:
    """Everything a consumer or auditor could see of publication."""
    conn = ws.db
    return {
        "active": ledger.active(conn, "shop"),
        "events": conn.execute(
            "SELECT event_id, action, target_run_id FROM control.publications ORDER BY event_id"
        ).fetchall(),
        "views": conn.execute(
            "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = 'published' "
            "ORDER BY view_name"
        ).fetchall(),
    }


def view_targets(ws: Workspace) -> set[str]:
    """Candidate schemas the published views read from."""
    found = set()
    for _, sql in published_state(ws)["views"]:
        found.update(part.split(".")[0].strip('"') for part in sql.split() if "cand_" in part)
    return found
