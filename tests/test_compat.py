"""dataprof behaviors expected to change between releases.

Each expectation is recorded per dataprof version. A version with no
expectation fails on purpose: review the change, then record it here and in
docs/compatibility.md.
"""

from __future__ import annotations

import json

import dataprof
import duckdb
import pytest

VERSION = dataprof.__version__


def _check(observed: object, expectations: dict[str, object], issue: str) -> None:
    if VERSION not in expectations:
        pytest.fail(f"no recorded expectation for dataprof {VERSION} ({issue}); review and record")
    # A development build reports the last release's version, so a mismatch
    # here can also mean an unreleased change.
    assert observed == expectations[VERSION], (
        f"dataprof {VERSION} gave {observed!r}, recorded {expectations[VERSION]!r} ({issue})"
    )


@pytest.fixture
def conn():
    connection = duckdb.connect()
    yield connection
    connection.close()


def test_null_markers(conn):
    """5% of customer_id is the text "N/A"; against a 2% null ceiling."""
    conn.execute(
        "CREATE TABLE t AS SELECT CASE WHEN i % 20 = 0 THEN 'N/A' ELSE 'C' || i END "
        "AS customer_id FROM range(2000) r(i)"
    )
    report = dataprof.profile(conn.sql("SELECT customer_id FROM t"))
    verdict = report.check(max_null_percentage={"customer_id": 2.0}).verdict
    # 0.12.0 counts "N/A" as a value (#813); 0.13.0 counts it as null (#847).
    _check(
        verdict, {"0.12.0": "pass", "0.13.0": "fail", "0.13.1": "fail"}, "AndreaBozzo/dataprof#813"
    )


def test_zero_padded_codes(conn):
    conn.execute("CREATE TABLE t AS SELECT lpad(i::VARCHAR, 6, '0') AS code FROM range(500) r(i)")
    report = dataprof.profile(conn.sql("SELECT code FROM t"))
    data_type = json.loads(report.to_json())["column_profiles"][0]["data_type"]
    # 0.12.0 types zero-padded codes as integers (#814); 0.13.0 keeps them text (#868).
    _check(
        data_type,
        {"0.12.0": "Integer", "0.13.0": "String", "0.13.1": "String"},
        "AndreaBozzo/dataprof#814",
    )
