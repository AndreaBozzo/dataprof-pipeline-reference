"""Process death at each boundary; recovery follows the ledger."""

from __future__ import annotations

import time

from conftest import cli, published_state

from pipeline_reference import faults, ledger
from pipeline_reference.workflow import Workspace


def _runs(ws_path, batch):
    with Workspace(ws_path) as ws:
        return ledger.runs_for_batch(ws.db, "shop", batch)


def test_crash_during_load_retries_into_a_fresh_candidate(ws_path, snapshots):
    crashed = cli(
        ws_path, "run", "--snapshot", str(snapshots("healthy", "b1")), fault="crash_during_load"
    )
    assert crashed.returncode == faults.CRASH_EXIT
    [first] = _runs(ws_path, "b1")
    assert first.stage == "loading"
    with Workspace(ws_path) as ws:
        assert published_state(ws)["active"] is None

    resumed = cli(ws_path, "resume", first.run_id)

    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    first_again, retry = _runs(ws_path, "b1")
    assert retry.retry_of == first.run_id
    assert retry.candidate_schema != first.candidate_schema
    with Workspace(ws_path) as ws:
        assert ledger.active(ws.db, "shop")[0] == retry.run_id


def test_rerunning_a_crashed_load_also_retries(ws_path, snapshots):
    directory = str(snapshots("healthy", "b1"))
    assert cli(ws_path, "run", "--snapshot", directory, fault="crash_during_load").returncode == 86
    assert cli(ws_path, "run", "--snapshot", directory).returncode == 0
    first, retry = _runs(ws_path, "b1")
    assert retry.retry_of == first.run_id


def test_crash_before_commit_leaves_no_trace_of_publication(ws_path, snapshots):
    assert cli(ws_path, "run", "--snapshot", str(snapshots("healthy", "b1"))).returncode == 0
    with Workspace(ws_path) as ws:
        before = published_state(ws)
    crashed = cli(
        ws_path,
        "run",
        "--snapshot",
        str(snapshots("healthy", "b2", seed=7)),
        fault="crash_before_commit",
    )
    assert crashed.returncode == faults.CRASH_EXIT
    [run] = _runs(ws_path, "b2")
    with Workspace(ws_path) as ws:
        assert published_state(ws) == before
        assert ledger.evaluations(ws.db, run.run_id) == []

    resumed = cli(ws_path, "resume", run.run_id)

    assert resumed.returncode == 0, resumed.stdout
    with Workspace(ws_path) as ws:
        assert ledger.active(ws.db, "shop")[0] == run.run_id
        assert len(ledger.events_for_run(ws.db, run.run_id)) == 1


def test_crash_after_commit_is_reported_not_republished(ws_path, snapshots):
    crashed = cli(
        ws_path, "run", "--snapshot", str(snapshots("healthy", "b1")), fault="crash_after_commit"
    )
    assert crashed.returncode == faults.CRASH_EXIT
    [run] = _runs(ws_path, "b1")
    with Workspace(ws_path) as ws:
        before = published_state(ws)
    assert before["active"][0] == run.run_id
    assert not (ws_path / "exports" / run.run_id).exists()

    resumed = cli(ws_path, "resume", run.run_id)

    assert resumed.returncode == 0
    assert "already decided; nothing republished" in resumed.stdout
    with Workspace(ws_path) as ws:
        assert published_state(ws) == before
    exported = cli(ws_path, "export", run.run_id)
    assert exported.returncode == 0
    assert list((ws_path / "exports" / run.run_id).glob("*/orders.report.json"))


def test_second_runner_fails_promptly_without_writing(ws_path, snapshots):
    directory = str(snapshots("healthy", "b1"))
    with Workspace(ws_path):
        started = time.monotonic()
        second = cli(ws_path, "run", "--snapshot", directory)
        elapsed = time.monotonic() - started
    assert second.returncode == 3
    assert "in use by another runner" in second.stderr
    assert elapsed < 30
    assert _runs(ws_path, "b1") == []


def test_lock_is_released_when_the_holder_dies(ws_path, snapshots):
    directory = str(snapshots("healthy", "b1"))
    assert cli(ws_path, "run", "--snapshot", directory, fault="crash_after_ready").returncode == 86
    assert cli(ws_path, "status").returncode == 0
