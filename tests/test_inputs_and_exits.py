"""Malformed manifests are input errors, and exit codes keep their meaning."""

from __future__ import annotations

import json

import duckdb
import pytest
from conftest import cli

from pipeline_reference import __main__ as cli_module
from pipeline_reference import ledger
from pipeline_reference.contract import load_contract
from pipeline_reference.workflow import Workspace, describe, run_snapshot

SHA = "0" * 64


def _corrupt(directory, edit):
    path = directory / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    edit(manifest)
    path.write_text(json.dumps(manifest), encoding="utf-8")


CORRUPTIONS = {
    "sha256 missing": lambda m: m["files"]["orders"].pop("sha256"),
    "sha256 not hex": lambda m: m["files"]["orders"].update(sha256="xyz"),
    "files not an object": lambda m: m.update(files=["orders.jsonl"]),
    "file entry not an object": lambda m: m["files"].update(orders="orders.jsonl"),
    "manifest version": lambda m: m.update(manifest_version=2),
    "contract version text": lambda m: m.update(contract_version="1"),
    "contract version boolean": lambda m: m.update(contract_version=True),
    "empty batch id": lambda m: m.update(source_batch_id=""),
    "repairs not text": lambda m: m.update(repairs_batch_id=5),
    "generator not an object": lambda m: m.update(generator="fixture"),
}


@pytest.mark.parametrize("name", sorted(CORRUPTIONS))
def test_a_malformed_manifest_is_an_input_error(ws, snapshots, name):
    directory = snapshots("healthy", "b1")
    _corrupt(directory, CORRUPTIONS[name])

    result = run_snapshot(ws, directory)

    assert result.outcome == "input_error", result.lines
    assert result.exit_code == 3
    assert ledger.all_runs(ws.db) == []


def test_a_manifest_that_is_not_an_object_is_an_input_error(ws, snapshots):
    directory = snapshots("healthy", "b1")
    (directory / "manifest.json").write_text("[]", encoding="utf-8")
    assert run_snapshot(ws, directory).outcome == "input_error"


def test_the_cli_reports_a_missing_digest_as_exit_3(ws_path, snapshots):
    directory = snapshots("healthy", "b1")
    _corrupt(directory, CORRUPTIONS["sha256 missing"])

    result = cli(ws_path, "run", "--snapshot", str(directory))

    assert result.returncode == 3, result.stdout + result.stderr
    assert "files.orders.sha256" in result.stdout
    assert "Traceback" not in result.stderr


def test_an_unexpected_failure_exits_3_not_1(ws_path, monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(cli_module, "status", boom)

    assert cli_module.main(["status", "--workspace", str(ws_path)]) == 3
    assert "operational error" in capsys.readouterr().err


def test_a_usage_error_exits_3_not_2():
    with pytest.raises(SystemExit) as exit_info:
        cli_module.main(["run"])
    assert exit_info.value.code == 3


def test_a_version_1_ledger_migrates_and_keeps_its_old_outcomes(ws_path):
    """A workspace written by 0.1.0 opens under 0.1.1 without rewriting history."""
    ws_path.mkdir(parents=True)
    conn = duckdb.connect(str(ws_path / "reference.duckdb"))
    for statement in ledger.MIGRATIONS[1]:
        conn.execute(statement)
    conn.execute("INSERT INTO control.schema_version VALUES (1, now())")
    conn.execute(
        "INSERT INTO control.runs VALUES ('r1', 'shop', 'b1', 'd', NULL, NULL, 1, 'cand_r1', "
        "NULL, '{}', '{}', now())"
    )
    conn.execute("INSERT INTO control.run_stages VALUES ('r1', 1, 'ready', NULL, now())")
    conn.execute(
        "INSERT INTO control.evaluations (evaluation_id, run_id, seq, created_at, outcome, "
        "versions) VALUES ('e1', 'r1', 1, now(), 'stale_conflict', '{}')"
    )
    conn.close()

    with Workspace(ws_path) as ws:
        version = ws.db.execute("SELECT max(version) FROM control.schema_version").fetchone()[0]
        run = ledger.get_run(ws.db, "r1")
        record = ledger.latest_evaluation(ws.db, "r1")
        result = describe(ws, load_contract(1), run)

    assert version == 2
    assert (run.columns_digest, run.dlt_schema_digest) == (None, None)
    assert (record["outcome"], record["publication"]) == ("stale_conflict", None)
    assert result.exit_code == 3
