"""Contracts resolve by version: a v1 publication survives the introduction of v2."""

from __future__ import annotations

import json
import shutil

import pytest
from conftest import CUSTOMERS, ORDERS, published_state, view_targets

from pipeline_reference import contract, ledger, scenarios
from pipeline_reference.__main__ import bootstrap_contract
from pipeline_reference.source import write_snapshot
from pipeline_reference.workflow import reevaluate, resume, rollback, run_snapshot


@pytest.fixture
def contracts(tmp_path, monkeypatch):
    root = tmp_path / "contracts"
    shutil.copytree(contract.CONTRACTS, root)
    monkeypatch.setenv(contract.CONTRACTS_VARIABLE, str(root))
    return root


def _rows_with_channel(seed):
    rows = scenarios.build("healthy", seed=seed, customers=CUSTOMERS, orders=ORDERS)
    for index, order in enumerate(rows["orders"]):
        order["channel"] = "web" if index % 3 else "store"
    return rows


@pytest.fixture
def v2(contracts, tmp_path):
    """Contract v2: orders gain a nullable `channel`. Bootstrapped like a maintainer would."""
    shutil.copytree(contracts / "v1", contracts / "v2")
    (contracts / "v2" / "schema.dlt.yaml").unlink()
    path = contracts / "v2" / "columns.json"
    columns = json.loads(path.read_text(encoding="utf-8"))
    columns["contract_version"] = 2
    columns["tables"]["orders"]["columns"]["channel"] = {
        "dlt": {"data_type": "text", "nullable": True},
        "duckdb": "VARCHAR",
    }
    path.write_text(json.dumps(columns), encoding="utf-8")
    loaded = contract.load_contract(2)
    fixture = write_snapshot(tmp_path / "v2-fixture", _rows_with_channel(1), loaded, "boot")
    assert bootstrap_contract(2, fixture.directory) == 0
    return contract.load_contract(2)


def test_a_v1_publication_survives_v2(ws, snapshots, v2, tmp_path):
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    second_dir = write_snapshot(tmp_path / "b2", _rows_with_channel(7), v2, "b2").directory

    second = run_snapshot(ws, second_dir)

    assert (first.outcome, second.outcome) == ("accepted", "accepted"), second.lines
    assert ledger.get_run(ws.db, second.run_id).contract_version == 2
    columns = [r[0] for r in ws.db.execute("DESCRIBE published.orders").fetchall()]
    assert "channel" in columns

    first_event = ledger.events_for_run(ws.db, first.run_id)[0]
    rolled = rollback(ws, first_event, "v2 consumers not ready")

    assert rolled.outcome == "done", rolled.lines
    assert view_targets(ws) == {ledger.get_run(ws.db, first.run_id).candidate_schema}
    columns = [r[0] for r in ws.db.execute("DESCRIBE published.orders").fetchall()]
    assert "channel" not in columns
    assert reevaluate(ws, first.run_id).outcome == "accepted"
    assert reevaluate(ws, second.run_id).outcome == "accepted"


def test_runs_record_the_contract_files_they_loaded_under(ws, snapshots):
    result = run_snapshot(ws, snapshots("healthy", "b1"))
    run = ledger.get_run(ws.db, result.run_id)
    loaded = contract.load_contract(1)
    assert (run.columns_digest, run.dlt_schema_digest) == (
        loaded.columns_digest,
        loaded.dlt_schema_digest,
    )
    evaluation = ledger.latest_evaluation(ws.db, result.run_id)
    assert evaluation["columns_digest"] == loaded.columns_digest
    assert evaluation["dlt_schema_digest"] == loaded.dlt_schema_digest


def test_a_contract_version_edited_in_place_is_refused(ws, snapshots, contracts):
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    run_snapshot(ws, snapshots("healthy", "b2", seed=7))
    path = contracts / "v1" / "columns.json"
    columns = json.loads(path.read_text(encoding="utf-8"))
    columns["tables"]["orders"]["columns"]["amount"]["duckdb"] = "DOUBLE"
    path.write_text(json.dumps(columns), encoding="utf-8")
    before = published_state(ws)

    for result in (
        reevaluate(ws, first.run_id),
        resume(ws, first.run_id),
        rollback(ws, ledger.events_for_run(ws.db, first.run_id)[0], "test"),
    ):
        assert result.outcome == "operational_error", result.lines
        assert "contract v1 changed since run" in result.lines[0]
    assert published_state(ws) == before


def test_a_snapshot_naming_an_unknown_contract_is_an_input_error(ws, snapshots):
    directory = snapshots("healthy", "b1")
    path = directory / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["contract_version"] = 9
    path.write_text(json.dumps(manifest), encoding="utf-8")

    result = run_snapshot(ws, directory)

    assert result.outcome == "input_error"
    assert "no contract v9" in result.lines[0]


def test_bootstrap_of_another_version_needs_its_own_fixture(contracts):
    shutil.copytree(contracts / "v1", contracts / "v2")
    path = contracts / "v2" / "columns.json"
    columns = json.loads(path.read_text(encoding="utf-8"))
    columns["contract_version"] = 2
    path.write_text(json.dumps(columns), encoding="utf-8")
    assert bootstrap_contract(2, None) == 3
