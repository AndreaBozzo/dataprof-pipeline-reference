"""Each defect is caught by the layer meant to catch it, and never published."""

from __future__ import annotations

import dataprof
import duckdb
import pytest
from conftest import ORDERS, published_state, view_targets

from pipeline_reference import ledger
from pipeline_reference.contract import load_contract
from pipeline_reference.scenarios import SCENARIOS
from pipeline_reference.workflow import run_snapshot

# scenario -> (layer that must decide, text the evidence must contain)
CAUGHT_BY = {
    "null_customer_ids": ("dataprof", "max_null_percentage"),
    "duplicate_order_ids": ("sql", "unique.orders.order_id"),
    "orphan_customer_ids": ("sql", "reference.orders.customer_id"),
    "negative_amount": ("sql", "range.orders.amount"),
    "unknown_currency": ("sql", "domain.orders.currency"),
    "extra_column": ("load", "columns` are frozen"),
    "bad_amount": ("load", "data_types` are frozen"),
    "missing_amount": ("load", "NOT NULL constraint failed"),
    "empty_orders": ("readiness", "table orders was not created"),
    "capped_profile": ("dataprof", "not_evaluated"),
}


def test_healthy_snapshot_is_published(ws, snapshots):
    result = run_snapshot(ws, snapshots("healthy", "b1"))
    assert result.outcome == "accepted", result.lines
    run = ledger.get_run(ws.db, result.run_id)
    assert view_targets(ws) == {run.candidate_schema}
    assert ws.db.execute("SELECT count(*) FROM published.orders").fetchone()[0] == ORDERS
    columns = [r[0] for r in ws.db.execute("DESCRIBE published.orders").fetchall()]
    assert columns == ["order_id", "customer_id", "amount", "currency", "ordered_at"]


def test_nothing_published_before_first_acceptance(ws, snapshots):
    result = run_snapshot(ws, snapshots("null_customer_ids", "b1"))
    assert result.outcome == "rejected"
    assert published_state(ws) == {"active": None, "events": [], "views": []}
    assert "published: nothing has been published yet" in result.lines


@pytest.mark.parametrize("name", sorted(CAUGHT_BY))
def test_defect_is_caught_and_previous_publication_stays(ws, snapshots, name):
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    assert first.outcome == "accepted"
    before = published_state(ws)

    scenario = SCENARIOS[name]
    result = run_snapshot(ws, snapshots(name, "b2", seed=7), max_rows=scenario.profile_max_rows)

    assert result.outcome == scenario.expected, result.lines
    assert published_state(ws) == before
    layer, text = CAUGHT_BY[name]
    run = ledger.get_run(ws.db, result.run_id)
    if layer in ("load", "readiness"):
        assert run.stage == "blocked"
        detail = ws.db.execute(
            "SELECT detail FROM control.run_stages WHERE run_id = ? ORDER BY seq DESC LIMIT 1",
            [run.run_id],
        ).fetchone()[0]
        assert text in detail, detail
        assert ledger.latest_evaluation(ws.db, run.run_id) is None
        return
    evaluation = ledger.latest_evaluation(ws.db, run.run_id)
    failed_sql = [a["id"] for a in evaluation["sql_assertions"] if not a["passed"]]
    verdicts = {t: g["verdict"] for t, g in evaluation["gates"].items()}
    if layer == "sql":
        assert failed_sql == [text], failed_sql
        # dataprof has no business-key, domain or cross-table rule; it passes.
        assert verdicts == {"customers": "pass", "orders": "pass"}
    else:
        assert failed_sql == []
        statuses = [c["status"] for c in evaluation["gates"]["orders"]["checks"]]
        assert text in str(evaluation["gates"]["orders"]) and (
            "failed" in statuses or "not_evaluated" in statuses
        )


def test_duplicate_ids_are_distinct_rows_to_dlt(ws, snapshots):
    """The entire-row duplicate count cannot see a reused business key."""
    run_snapshot(ws, snapshots("healthy", "b1"))
    result = run_snapshot(ws, snapshots("duplicate_order_ids", "b2", seed=7))
    run = ledger.get_run(ws.db, result.run_id)
    schema = run.candidate_schema
    physical = ws.db.execute(
        f'SELECT count(*) - count(DISTINCT _dlt_id) FROM "{schema}".orders'
    ).fetchone()[0]
    business = ws.db.execute(
        f'SELECT count(*) - count(DISTINCT order_id) FROM "{schema}".orders'
    ).fetchone()[0]
    assert (physical, business) == (0, 10)


def test_null_rate_observed_matches_planted(ws, snapshots):
    run_snapshot(ws, snapshots("healthy", "b1"))
    result = run_snapshot(ws, snapshots("null_customer_ids", "b2", seed=7))
    evaluation = ledger.latest_evaluation(ws.db, result.run_id)
    check = next(
        c
        for c in evaluation["gates"]["orders"]["checks"]
        if c["code"] == "max_null_percentage" and c["column"] == "customer_id"
    )
    assert check["status"] == "failed"
    assert check["observed"] == pytest.approx(8.0)


def test_one_null_key_past_20000_rows_is_rejected():
    """One null order_id in 20,001 rows is a null rate of 0.005%, shown as 0.0 (G8)."""
    contract = load_contract()
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE orders AS SELECT "
        "CASE WHEN i = 0 THEN NULL ELSE 'O' || i END AS order_id, "
        "'C' || (i % 500) AS customer_id, 10.00::DECIMAL(12,2) AS amount, "
        "'EUR' AS currency, TIMESTAMPTZ '2026-10-01 00:00:00+00' AS ordered_at "
        "FROM range(20001) r(i)"
    )
    report = dataprof.profile(conn.sql("SELECT * FROM orders"))
    gate = report.check(**contract.policy["dataprof"]["orders"]["check"])
    failed = [(c["code"], c["column"]) for c in gate.to_dict()["checks"] if c["status"] == "failed"]
    assert failed == [("max_null_count", "order_id")]
    conn.close()


def test_truncated_snapshot_is_caught_by_the_volume_floor(ws, snapshots):
    """A consistent but cut-short extract passes every rule except dataprof's row floor."""
    first = run_snapshot(ws, snapshots("healthy", "b1"))
    before = published_state(ws)

    result = run_snapshot(ws, snapshots("truncated_snapshot", "b2", seed=7))

    assert result.outcome == "rejected", result.lines
    assert published_state(ws) == before
    assert ws.db.execute("SELECT count(*) FROM published.orders").fetchone()[0] == ORDERS
    evaluation = ledger.latest_evaluation(ws.db, result.run_id)
    assert all(a["passed"] for a in evaluation["sql_assertions"])
    failed = {
        table: [c["code"] for c in gate["checks"] if c["status"] == "failed"]
        for table, gate in evaluation["gates"].items()
    }
    assert failed == {"customers": ["min_rows"], "orders": ["min_rows"]}
    assert first.outcome == "accepted"
