"""dlt loading into a run's candidate schema, and the readiness checks after it."""

from __future__ import annotations

import os

# dlt sends anonymous usage telemetry unless told not to; the reference runs
# offline once dependencies are installed.
os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")

import shutil  # noqa: E402
import tempfile  # noqa: E402
from collections.abc import Iterator  # noqa: E402
from dataclasses import dataclass, field  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any  # noqa: E402

import dlt  # noqa: E402
import duckdb  # noqa: E402
from dlt.common.schema.exceptions import DataValidationError  # noqa: E402

from . import faults  # noqa: E402
from .contract import Contract, identifier  # noqa: E402
from .source import KeyTracker, Snapshot, iter_records, verify  # noqa: E402

SCHEMA_NAME = "shop"
# Messages dlt and DuckDB give when the data, not the machine, broke the load.
CONTRACT_MARKERS = (
    "Contract on",
    "NOT NULL constraint failed",
    "Cannot coerce NULL",
)


@dataclass
class LoadResult:
    load_ids: list[str] = field(default_factory=list)
    absent_keys: dict[str, list[str]] = field(default_factory=dict)
    blocked: list[str] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return not self.blocked


class ContractViolation(Exception):
    """The batch broke the contract during load; the candidate is blocked."""


def _chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def is_contract_violation(exc: BaseException) -> bool:
    for item in _chain(exc):
        if isinstance(item, DataValidationError):
            return True
        if any(marker in str(item) for marker in CONTRACT_MARKERS):
            return True
    return False


def _resources(snapshot: Snapshot, contract: Contract, keys: KeyTracker) -> list[Any]:
    resources = []
    for name, spec in contract.tables.items():
        path = snapshot.directory / spec.file

        def records(path: Path = path, table: str = name) -> Iterator[dict[str, Any]]:
            yield from iter_records(path, table, keys, faults.on_record)

        resources.append(
            dlt.resource(
                records,
                name=name,
                columns=spec.dlt_columns,
                write_disposition="replace",
            )
        )
    return resources


def load(
    conn: duckdb.DuckDBPyConnection,
    snapshot: Snapshot,
    contract: Contract,
    candidate_schema: str,
    pipelines_dir: Path,
) -> LoadResult:
    """Load the retained snapshot into ``candidate_schema`` under the frozen contract.

    Each attempt is its own dlt pipeline, so a package a crashed attempt left
    pending is never finished into a later candidate.
    """
    identifier(candidate_schema)
    keys = KeyTracker.for_contract(contract)
    stored = contract.dlt_schema_dict()

    @dlt.source(schema=dlt.Schema.from_dict(stored))
    def shop() -> list[Any]:
        return _resources(snapshot, contract, keys)

    pipeline = dlt.pipeline(
        pipeline_name=candidate_schema,
        pipelines_dir=str(pipelines_dir),
        destination=dlt.destinations.duckdb(conn),
        dataset_name=candidate_schema,
    )
    try:
        info = pipeline.run(shop(), schema_contract="freeze")
    except Exception as exc:
        if is_contract_violation(exc):
            raise ContractViolation(_summary(exc)) from exc
        raise
    finally:
        # dlt points the caller's connection at the dataset; project SQL is
        # schema-qualified anyway, and resetting keeps other callers honest.
        conn.execute("RESET search_path")
    result = LoadResult(load_ids=list(info.loads_ids), absent_keys=keys.absent())
    if info.has_failed_jobs:
        result.blocked.append("dlt reported failed load jobs")
    return result


def _summary(exc: BaseException) -> str:
    for item in _chain(exc):
        text = str(item)
        for line in text.splitlines():
            if any(marker in line for marker in CONTRACT_MARKERS):
                return line.strip()
    return str(exc).splitlines()[0]


def check_ready(
    conn: duckdb.DuckDBPyConnection,
    snapshot: Snapshot,
    contract: Contract,
    candidate_schema: str,
    load_ids: list[str],
) -> list[str]:
    """Reasons the candidate cannot be evaluated; empty when it is ready."""
    problems = []
    try:
        verify(snapshot)
    except Exception as exc:
        problems.append(f"retained input changed: {exc}")
    schema = identifier(candidate_schema)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [candidate_schema],
        ).fetchall()
    }
    if "_dlt_loads" not in tables:
        problems.append("no _dlt_loads table: dlt did not complete a load")
    else:
        statuses = dict(
            conn.execute(
                f"SELECT load_id, status FROM {schema}._dlt_loads WHERE schema_name = ?",
                [SCHEMA_NAME],
            ).fetchall()
        )
        for load_id in load_ids:
            if statuses.get(load_id) != 0:
                problems.append(f"load {load_id} is not complete in _dlt_loads")
        if not load_ids:
            problems.append("dlt reported no completed load")
    for name, spec in contract.tables.items():
        if name not in tables:
            problems.append(f"table {name} was not created (empty or absent input)")
            continue
        actual = dict(
            conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                [candidate_schema, name],
            ).fetchall()
        )
        for column, expected_type in spec.duckdb_types.items():
            if column not in actual:
                problems.append(f"{name}.{column} is missing")
            elif actual[column] != expected_type:
                problems.append(f"{name}.{column} is {actual[column]}, expected {expected_type}")
        allowed = set(spec.columns) | set(contract.metadata_columns)
        for column in actual:
            if column not in allowed:
                problems.append(f"{name}.{column} is not in contract v{contract.version}")
    return problems


def bootstrap_schema(contract: Contract, fixture: Snapshot) -> dict[str, Any]:
    """Build the stored dlt schema from a non-frozen load of a known-good fixture.

    A maintainer command for a new contract version. ``freeze`` rejects every
    table in a fresh dataset unless the pipeline starts from a schema that
    already holds them, so the schema is captured once and versioned.
    """
    keys = KeyTracker.for_contract(contract)
    with tempfile.TemporaryDirectory() as tmp:
        conn = duckdb.connect(str(Path(tmp) / "bootstrap.duckdb"))
        try:
            pipeline = dlt.pipeline(
                pipeline_name="bootstrap",
                pipelines_dir=str(Path(tmp) / "dlt"),
                destination=dlt.destinations.duckdb(conn),
                # Not "bootstrap": a dataset named like the DuckDB catalog (the
                # file stem) makes dlt's unqualified references ambiguous.
                dataset_name="contract_bootstrap",
            )

            @dlt.source(name=SCHEMA_NAME)
            def shop() -> list[Any]:
                return _resources(fixture, contract, keys)

            info = pipeline.run(shop())
            if info.has_failed_jobs:
                raise RuntimeError("bootstrap load had failed jobs")
            schema = pipeline.default_schema.to_dict()
        finally:
            conn.close()
            shutil.rmtree(Path(tmp) / "dlt", ignore_errors=True)
    if keys.absent():
        raise RuntimeError(f"bootstrap fixture lacks keys: {keys.absent()}")
    return schema
