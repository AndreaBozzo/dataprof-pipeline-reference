"""The profiled numbers are the data's numbers, whichever Arrow route carries them."""

from __future__ import annotations

import json

import dataprof
import dlt
import pytest
from conftest import ORDERS

from pipeline_reference import ledger
from pipeline_reference.contract import load_contract
from pipeline_reference.workflow import run_snapshot


def _columns(report) -> dict[str, dict]:
    return {c["name"]: c for c in json.loads(report.to_json())["column_profiles"]}


@pytest.fixture
def candidate(ws, snapshots):
    result = run_snapshot(ws, snapshots("healthy", "b1"))
    assert result.outcome == "accepted"
    return ledger.get_run(ws.db, result.run_id).candidate_schema


def test_relation_matches_sql_truth(ws, candidate):
    projection = load_contract().tables["orders"].projection(candidate)
    columns = _columns(dataprof.profile(ws.db.sql(projection)))
    truth = ws.db.execute(
        f"SELECT count(*), count(*) - count(customer_id), count(DISTINCT order_id), "
        f"count(DISTINCT currency), min(amount), max(amount) FROM ({projection})"
    ).fetchone()
    rows, null_customers, distinct_orders, distinct_currencies, low, high = truth
    assert rows == ORDERS
    assert {c["total_count"] for c in columns.values()} == {rows}
    assert columns["customer_id"]["null_count"] == null_customers > 0
    assert columns["order_id"]["unique_count"] == distinct_orders
    assert columns["currency"]["unique_count"] == distinct_currencies
    numeric = columns["amount"]["stats"]["Numeric"]
    assert (numeric["min"], numeric["max"]) == (float(low), float(high))


def test_multiple_batches_profile_like_one(ws, candidate):
    projection = load_contract().tables["orders"].projection(candidate)
    batches = sum(1 for _ in ws.db.sql(projection).to_arrow_reader(97))
    assert batches > 1
    whole = _columns(dataprof.profile(ws.db.sql(projection)))
    chunked = _columns(dataprof.profile(ws.db.sql(projection).to_arrow_reader(97)))
    assert chunked == whole


def test_dlt_dataset_route_matches_relation(ws, candidate):
    spec = load_contract().tables["orders"]
    pipeline = dlt.pipeline(
        pipeline_name=candidate,
        pipelines_dir=str(ws.pipelines),
        destination=dlt.destinations.duckdb(ws.db),
        dataset_name=candidate,
    )
    table = pipeline.dataset()["orders"].select(*spec.columns).arrow()
    ws.db.execute("RESET search_path")
    via_dlt = _columns(dataprof.profile(table))
    via_relation = _columns(dataprof.profile(ws.db.sql(spec.projection(candidate))))
    assert via_dlt == via_relation
