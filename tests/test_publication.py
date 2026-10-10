"""Publication is all or nothing, never stale, and rollback is audited."""

from __future__ import annotations

import os
import shutil

import pytest
from conftest import cli, published_state, view_targets

from pipeline_reference import faults, ledger
from pipeline_reference.workflow import Workspace, reevaluate, rollback, run_snapshot


@pytest.fixture
def armed(monkeypatch):
    def arm(point: str) -> None:
        monkeypatch.setenv(faults.VARIABLE, point)

    return arm


def test_failure_between_view_replacements_changes_nothing(ws, snapshots, armed):
    run_snapshot(ws, snapshots("healthy", "b1"))
    before = published_state(ws)
    armed("between_views")
    result = run_snapshot(ws, snapshots("healthy", "b2", seed=7))
    assert result.outcome == "operational_error", result.lines
    assert published_state(ws) == before
    outcomes = [e["outcome"] for e in ledger.evaluations(ws.db, result.run_id)]
    assert outcomes == ["operational_error"]


def test_arrow_producer_failure_is_an_operational_error(ws, snapshots, armed):
    run_snapshot(ws, snapshots("healthy", "b1"))
    before = published_state(ws)
    armed("arrow_producer")
    result = run_snapshot(ws, snapshots("healthy", "b2", seed=7))
    assert result.outcome == "operational_error", result.lines
    assert "injected fault in the Arrow producer" in "\n".join(result.lines)
    assert published_state(ws) == before


def test_resumed_old_run_does_not_overwrite_newer_publication(ws_path, snapshots):
    assert cli(ws_path, "run", "--snapshot", str(snapshots("healthy", "b1"))).returncode == 0
    crashed = cli(
        ws_path,
        "run",
        "--snapshot",
        str(snapshots("healthy", "b2", seed=7)),
        fault="crash_after_ready",
    )
    assert crashed.returncode == faults.CRASH_EXIT
    assert (
        cli(ws_path, "run", "--snapshot", str(snapshots("healthy", "b3", seed=8))).returncode == 0
    )
    with Workspace(ws_path) as ws:
        old = ledger.runs_for_batch(ws.db, "shop", "b2")[0]
        newer = ledger.runs_for_batch(ws.db, "shop", "b3")[0]
        before = published_state(ws)
    assert old.stage == "ready"

    resumed = cli(ws_path, "resume", old.run_id)

    assert resumed.returncode == 3, resumed.stdout
    assert "outcome: stale_conflict" in resumed.stdout
    with Workspace(ws_path) as ws:
        assert published_state(ws) == before
        assert ledger.active(ws.db, "shop")[0] == newer.run_id


def test_rollback_moves_views_and_pointer_together(ws, snapshots):
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    second = run_snapshot(ws, snapshots("healthy", "b2", seed=7))
    first_event = ledger.events_for_run(ws.db, first.run_id)[0]

    result = rollback(ws, first_event, "b2 totals disputed by finance")

    assert result.outcome == "done", result.lines
    state = published_state(ws)
    first_run = ledger.get_run(ws.db, first.run_id)
    assert state["active"] == (first.run_id, result.event_id)
    assert view_targets(ws) == {first_run.candidate_schema}
    event = ledger.publication(ws.db, result.event_id)
    assert event["action"] == "rollback"
    assert event["reason"] == "b2 totals disputed by finance"
    assert event["previous_run_id"] == second.run_id
    # The historical verdict is kept, not re-earned: the event points at it.
    assert event["evaluation_id"] == ledger.publication(ws.db, first_event)["evaluation_id"]


def test_rollback_refuses_a_candidate_that_no_longer_matches(ws, snapshots):
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    run_snapshot(ws, snapshots("healthy", "b2", seed=7))
    schema = ledger.get_run(ws.db, first.run_id).candidate_schema
    ws.db.execute(f'ALTER TABLE "{schema}".orders ADD COLUMN note VARCHAR')
    before = published_state(ws)

    result = rollback(ws, ledger.events_for_run(ws.db, first.run_id)[0], "test")

    assert result.outcome == "operational_error"
    assert "orders.note is not in contract v1" in "\n".join(result.lines)
    assert published_state(ws) == before


def test_rollback_after_rollback_is_based_on_the_rollback(ws, snapshots):
    """A run created before a rollback is stale after it."""
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    run_snapshot(ws, snapshots("healthy", "b2", seed=7))
    rolled = rollback(ws, ledger.events_for_run(ws.db, first.run_id)[0], "test")
    assert ledger.active(ws.db, "shop") == (first.run_id, rolled.event_id)
    third = run_snapshot(ws, snapshots("healthy", "b3", seed=8))
    assert third.outcome == "accepted"
    assert ledger.get_run(ws.db, third.run_id).base_event_id == rolled.event_id


def test_same_batch_replay_creates_no_run(ws, snapshots):
    directory = snapshots("healthy", "b1")
    first = run_snapshot(ws, directory)
    before = published_state(ws)

    again = run_snapshot(ws, directory)

    assert again.outcome == "accepted"
    assert again.run_id == first.run_id
    assert "already has run" in again.lines[0]
    assert len(ledger.runs_for_batch(ws.db, "shop", "b1")) == 1
    assert published_state(ws) == before


def test_same_batch_id_with_other_bytes_is_an_input_error(ws, snapshots, tmp_path):
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    other = snapshots("healthy", "b1-other", seed=7)
    changed = tmp_path / "changed"
    shutil.copytree(other, changed)
    manifest = (changed / "manifest.json").read_text().replace('"b1-other"', '"b1"')
    (changed / "manifest.json").write_text(manifest)

    result = run_snapshot(ws, changed)

    assert result.outcome == "input_error"
    assert result.exit_code == 3
    assert first.run_id in result.lines[0]
    assert len(ledger.runs_for_batch(ws.db, "shop", "b1")) == 1


def test_manifest_digest_mismatch_is_an_input_error(ws, snapshots):
    directory = snapshots("healthy", "b1")
    with (directory / "orders.jsonl").open("ab") as handle:
        handle.write(b"\n")
    result = run_snapshot(ws, directory)
    assert result.outcome == "input_error"
    assert "does not match the manifest" in result.lines[0]
    assert ledger.all_runs(ws.db) == []


def test_retained_inputs_are_read_only_copies(ws, snapshots):
    result = run_snapshot(ws, snapshots("healthy", "b1"))
    retained = ws.inputs / result.run_id / "orders.jsonl"
    assert retained.is_file()
    assert not os.access(retained, os.W_OK)


def test_reevaluating_a_rejected_run_appends_an_evaluation(ws, snapshots):
    run_snapshot(ws, snapshots("healthy", "b1"))
    rejected = run_snapshot(ws, snapshots("null_customer_ids", "b2", seed=7))
    first = ledger.latest_evaluation(ws.db, rejected.run_id)

    again = reevaluate(ws, rejected.run_id)

    assert again.outcome == "rejected"
    records = ledger.evaluations(ws.db, rejected.run_id)
    assert [r["seq"] for r in records] == [1, 2]
    assert records[0] == first
