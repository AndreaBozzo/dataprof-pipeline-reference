"""`reevaluate` records evidence only; `promote` publishes on purpose."""

from __future__ import annotations

import json
import shutil

import pytest
from conftest import published_state

from pipeline_reference import contract, ledger
from pipeline_reference.workflow import promote, reevaluate, run_snapshot


@pytest.fixture
def contracts(tmp_path, monkeypatch):
    """A writable copy of the packaged contracts, used by the runner."""
    root = tmp_path / "contracts"
    shutil.copytree(contract.CONTRACTS, root)
    monkeypatch.setenv(contract.CONTRACTS_VARIABLE, str(root))
    return root


def _relax_null_limit(root, limit):
    path = root / "v1" / "policy.json"
    policy = json.loads(path.read_text(encoding="utf-8"))
    policy["dataprof"]["orders"]["check"]["max_null_percentage"]["customer_id"] = limit
    path.write_text(json.dumps(policy), encoding="utf-8")


def test_reevaluating_under_a_relaxed_policy_does_not_publish(ws, snapshots, contracts):
    run_snapshot(ws, snapshots("healthy", "b1"))
    rejected = run_snapshot(ws, snapshots("null_customer_ids", "b2", seed=7))
    before = published_state(ws)
    _relax_null_limit(contracts, 10.0)

    again = reevaluate(ws, rejected.run_id)

    assert again.outcome == "accepted", again.lines
    assert "publication: not published: `reevaluate` records evidence only" in again.lines
    assert published_state(ws) == before
    records = ledger.evaluations(ws.db, rejected.run_id)
    assert [(r["outcome"], r["publication"]) for r in records] == [
        ("rejected", "not_eligible"),
        ("accepted", "not_requested"),
    ]
    assert records[0]["policy_digest"] != records[1]["policy_digest"]


def test_reevaluating_an_older_publication_is_not_a_stale_conflict(ws, snapshots):
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    run_snapshot(ws, snapshots("healthy", "b2", seed=7))

    again = reevaluate(ws, first.run_id)

    assert again.outcome == "accepted"
    assert again.exit_code == 0
    assert ledger.latest_evaluation(ws.db, first.run_id)["publication"] == "not_requested"


def test_promote_publishes_with_a_fresh_evaluation_and_a_reason(ws, snapshots, contracts):
    run_snapshot(ws, snapshots("healthy", "b1"))
    rejected = run_snapshot(ws, snapshots("null_customer_ids", "b2", seed=7))
    _relax_null_limit(contracts, 10.0)

    result = promote(ws, rejected.run_id, "8% guest orders accepted for the Q4 import")

    assert result.outcome == "accepted", result.lines
    assert ledger.active(ws.db, "shop") == (rejected.run_id, result.event_id)
    event = ledger.publication(ws.db, result.event_id)
    assert event["action"] == "promote"
    assert event["reason"] == "8% guest orders accepted for the Q4 import"
    latest = ledger.latest_evaluation(ws.db, rejected.run_id)
    assert event["evaluation_id"] == latest["evaluation_id"]
    assert latest["publication"] == "published"


def test_promote_does_not_publish_a_rejected_run(ws, snapshots):
    run_snapshot(ws, snapshots("healthy", "b1"))
    rejected = run_snapshot(ws, snapshots("null_customer_ids", "b2", seed=7))
    before = published_state(ws)

    result = promote(ws, rejected.run_id, "try anyway")

    assert result.outcome == "rejected"
    assert result.exit_code == 1
    assert published_state(ws) == before
    assert ledger.latest_evaluation(ws.db, rejected.run_id)["publication"] == "not_eligible"


def test_promote_refuses_the_active_run(ws, snapshots):
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    result = promote(ws, first.run_id, "again")
    assert result.outcome == "input_error"
    assert "already the active publication" in result.lines[0]
