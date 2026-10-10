"""The runbook in docs/operations.md, command by command, with its exit codes."""

from __future__ import annotations

import re
import subprocess
import sys

from conftest import CUSTOMERS, ORDERS, cli


def _generate(out, batch_id, scenario="healthy", seed="1"):
    return subprocess.run(
        [
            sys.executable, "-m", "pipeline_reference", "generate",
            "--batch-id", batch_id, "--out", str(out), "--scenario", scenario, "--seed", seed,
            "--customers", str(CUSTOMERS), "--orders", str(ORDERS),
        ],
        capture_output=True, text=True, timeout=120,
    )  # fmt: skip


def test_runbook(tmp_path):
    ws = tmp_path / "ws"
    assert _generate(tmp_path / "b1", "b1").returncode == 0
    assert _generate(tmp_path / "b2", "b2", "null_customer_ids", "2").returncode == 0
    assert _generate(tmp_path / "b3", "b3", seed="3").returncode == 0

    empty = cli(ws, "status")
    assert empty.returncode == 0
    assert "nothing has been published yet" in empty.stdout

    assert cli(ws, "run", "--snapshot", str(tmp_path / "b1")).returncode == 0
    assert cli(ws, "run", "--snapshot", str(tmp_path / "b2")).returncode == 1
    assert cli(ws, "run", "--snapshot", str(tmp_path / "b3")).returncode == 0
    capped = _generate(tmp_path / "b4", "b4", seed="4")
    assert capped.returncode == 0
    inconclusive = cli(ws, "run", "--snapshot", str(tmp_path / "b4"), "--profile-max-rows", "100")
    assert inconclusive.returncode == 2, inconclusive.stdout

    listing = cli(ws, "status").stdout
    events = re.findall(r"(p\w+)\s+publish\s+run (r\w+)", listing)
    assert len(events) == 2
    first_event, first_run = events[0]

    rolled = cli(ws, "rollback", first_event, "--reason", "runbook check")
    assert rolled.returncode == 0, rolled.stdout
    assert f"published: run {first_run}" in rolled.stdout
    again = cli(ws, "rollback", first_event, "--reason", "twice")
    assert again.returncode == 3
    assert "already the active publication" in again.stdout

    exported = cli(ws, "export", first_run)
    assert exported.returncode == 0
    assert list((ws / "exports" / first_run).glob("*/summary.txt"))

    unknown = cli(ws, "resume", "r_missing")
    assert unknown.returncode == 3
