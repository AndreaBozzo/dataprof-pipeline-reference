"""Gates on a ready candidate: dataprof policies and SQL business assertions.

dataprof decides what its policy says (pass, fail, inconclusive). SQL covers
what one profiled table cannot say: business-key uniqueness, value domains,
cross-table references and volume. This module only combines the two; it does
not reinterpret a dataprof verdict.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import dataprof
import duckdb
import pyarrow as pa

from . import faults
from .contract import Contract, digest, identifier

VERDICTS = ("pass", "fail", "inconclusive")
ACCEPTED, REJECTED, INCONCLUSIVE = "accepted", "rejected", "inconclusive"
STALE, OPERATIONAL = "stale_conflict", "operational_error"


class UnknownVerdict(RuntimeError):
    """dataprof returned a verdict this runner does not know; never promote on it."""


@dataclass
class Evidence:
    projections: dict[str, str] = field(default_factory=dict)
    arrow_schemas: dict[str, list[list[str]]] = field(default_factory=dict)
    reports: dict[str, Any] = field(default_factory=dict)
    gates: dict[str, Any] = field(default_factory=dict)
    sql_assertions: list[dict[str, Any]] = field(default_factory=list)
    profile_options: dict[str, Any] = field(default_factory=dict)

    @property
    def verdicts(self) -> dict[str, str]:
        return {table: gate["verdict"] for table, gate in self.gates.items()}

    def outcome(self) -> str:
        """Rejected on any known violation; inconclusive only without one."""
        failed_sql = [a for a in self.sql_assertions if not a["passed"]]
        verdicts = self.verdicts.values()
        if failed_sql or "fail" in verdicts:
            return REJECTED
        if "inconclusive" in verdicts:
            return INCONCLUSIVE
        return ACCEPTED


def _failing_stream(relation: duckdb.DuckDBPyRelation) -> pa.RecordBatchReader:
    """A producer that delivers one batch and then fails, as a broken source would."""
    reader = relation.to_arrow_reader(100)

    def batches() -> Iterator[pa.RecordBatch]:
        yield reader.read_next_batch()
        raise faults.InjectedFault("injected fault in the Arrow producer")

    return pa.RecordBatchReader.from_batches(reader.schema, batches())


def profile_table(
    conn: duckdb.DuckDBPyConnection,
    contract: Contract,
    schema: str,
    table: str,
    max_rows: int | None,
) -> tuple[str, list[list[str]], Any]:
    spec = contract.tables[table]
    projection = spec.projection(schema)
    arrow_schema = conn.sql(projection + " LIMIT 0").to_arrow_table().schema
    hints = contract.policy["dataprof"][table].get("hints", {})
    relation = conn.sql(projection)
    source: Any = relation
    if faults.active() == "arrow_producer":
        source = _failing_stream(relation)
    report = dataprof.profile(source, name=f"{schema}.{table}", max_rows=max_rows, **hints)
    return projection, [[f.name, str(f.type)] for f in arrow_schema], report


def run_gates(
    conn: duckdb.DuckDBPyConnection,
    contract: Contract,
    schema: str,
    *,
    max_rows: int | None = None,
) -> Evidence:
    evidence = Evidence(profile_options={"max_rows": max_rows})
    for table in contract.tables:
        projection, arrow_schema, report = profile_table(conn, contract, schema, table, max_rows)
        gate = report.check(**contract.policy["dataprof"][table]["check"])
        if gate.verdict not in VERDICTS:
            raise UnknownVerdict(f"{table}: dataprof verdict {gate.verdict!r}")
        evidence.projections[table] = projection
        evidence.arrow_schemas[table] = arrow_schema
        evidence.reports[table] = json.loads(report.to_json())
        evidence.gates[table] = gate.to_dict()
    evidence.sql_assertions = sql_assertions(conn, contract, schema)
    return evidence


def _assertions(contract: Contract, schema: str) -> list[dict[str, Any]]:
    s = identifier(schema)
    sql_policy = contract.policy["sql"]
    found = []
    for table, spec in contract.tables.items():
        t = f"{s}.{identifier(table)}"
        key = identifier(spec.key)
        found.append(
            {
                "id": f"unique.{table}.{spec.key}",
                "description": f"{spec.key} identifies one {table} row",
                "sql": f"SELECT count({key}) - count(DISTINCT {key}) FROM {t}",
                "params": [],
                "comparator": "==",
                "threshold": 0,
            }
        )
    orders = f"{s}.{identifier('orders')}"
    customers = f"{s}.{identifier('customers')}"
    found += [
        {
            "id": "range.orders.amount",
            "description": "no negative amount",
            "sql": f"SELECT count(*) FROM {orders} WHERE amount < 0",
            "params": [],
            "comparator": "==",
            "threshold": 0,
        },
        {
            "id": "domain.orders.currency",
            "description": "currency is one of the documented codes",
            "sql": f"SELECT count(*) FROM {orders} WHERE NOT list_contains(?, currency)",
            "params": [sql_policy["currencies"]],
            "comparator": "==",
            "threshold": 0,
        },
        {
            "id": "reference.orders.customer_id",
            "description": "every non-null customer_id exists in customers",
            "sql": (
                f"SELECT count(*) FROM {orders} o WHERE o.customer_id IS NOT NULL "
                f"AND NOT EXISTS (SELECT 1 FROM {customers} c "
                "WHERE c.customer_id = o.customer_id)"
            ),
            "params": [],
            "comparator": "==",
            "threshold": 0,
        },
    ]
    return found


def sql_assertions(
    conn: duckdb.DuckDBPyConnection, contract: Contract, schema: str
) -> list[dict[str, Any]]:
    results = []
    for assertion in _assertions(contract, schema):
        observed = conn.execute(assertion["sql"], assertion["params"]).fetchone()[0]
        if assertion["comparator"] == ">=":
            passed = observed >= assertion["threshold"]
        else:
            passed = observed == assertion["threshold"]
        results.append(
            {
                "id": assertion["id"],
                "description": assertion["description"],
                "observed": observed,
                "comparator": assertion["comparator"],
                "threshold": assertion["threshold"],
                "passed": passed,
                "sql_digest": digest([assertion["sql"], assertion["params"]]),
            }
        )
    return results
