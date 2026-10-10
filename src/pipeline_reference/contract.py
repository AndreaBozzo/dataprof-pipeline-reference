"""The versioned dataset contract: columns, dlt schema and gate policy."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

CONTRACTS = Path(__file__).parent / "contracts"
#: Points the runner at another contracts directory, laid out like CONTRACTS.
CONTRACTS_VARIABLE = "PIPELINE_REFERENCE_CONTRACTS"
DEFAULT_VERSION = 1
SCHEMA_FILE = "schema.dlt.yaml"
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


def identifier(name: str) -> str:
    """Return ``name`` double-quoted, after checking it is a plain identifier.

    Every SQL identifier the runner writes comes from the contract or from
    internally generated run IDs, never from source data; this check makes a
    violation of that rule fail instead of reaching SQL.
    """
    if not IDENTIFIER.match(name):
        raise ValueError(f"not a safe identifier: {name!r}")
    return f'"{name}"'


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TableSpec:
    name: str
    file: str
    key: str
    columns: dict[str, dict[str, Any]]

    @property
    def dlt_columns(self) -> dict[str, dict[str, Any]]:
        return {name: dict(spec["dlt"]) for name, spec in self.columns.items()}

    @property
    def duckdb_types(self) -> dict[str, str]:
        return {name: spec["duckdb"] for name, spec in self.columns.items()}

    def projection(self, schema: str) -> str:
        """The explicit business-column projection consumers and gates see."""
        cols = ", ".join(identifier(c) for c in self.columns)
        return f"SELECT {cols} FROM {identifier(schema)}.{identifier(self.name)}"


class ContractNotFound(LookupError):
    """No contract directory for the requested version."""


@dataclass(frozen=True)
class Contract:
    version: int
    dataset_group: str
    tables: dict[str, TableSpec]
    metadata_columns: tuple[str, ...]
    policy: dict[str, Any]
    directory: Path
    columns_document: dict[str, Any]

    @property
    def policy_digest(self) -> str:
        return digest(self.policy)

    @property
    def columns_digest(self) -> str:
        return digest(self.columns_document)

    @property
    def dlt_schema_digest(self) -> str | None:
        """SHA-256 of the stored schema file, None before it is bootstrapped."""
        if not self.schema_path.is_file():
            return None
        return hashlib.sha256(self.schema_path.read_bytes()).hexdigest()

    @property
    def schema_path(self) -> Path:
        return self.directory / SCHEMA_FILE

    def dlt_schema_dict(self) -> dict[str, Any]:
        """The stored dlt schema that ``freeze`` validates each run against."""
        if not self.schema_path.is_file():
            raise FileNotFoundError(
                f"{self.schema_path} is missing; run "
                "`python -m pipeline_reference bootstrap-contract` to create it"
            )
        return yaml.safe_load(self.schema_path.read_text(encoding="utf-8"))


def contracts_root() -> Path:
    override = os.environ.get(CONTRACTS_VARIABLE)
    return Path(override) if override else CONTRACTS


def load_contract(version: int = DEFAULT_VERSION, root: Path | None = None) -> Contract:
    """The contract a run was or will be loaded under, by explicit version."""
    directory = (root or contracts_root()) / f"v{version}"
    if not (directory / "columns.json").is_file():
        raise ContractNotFound(f"no contract v{version} in {directory.parent}")
    columns = json.loads((directory / "columns.json").read_text(encoding="utf-8"))
    policy = json.loads((directory / "policy.json").read_text(encoding="utf-8"))
    if columns["contract_version"] != version:
        raise ValueError(f"{directory}: columns.json declares {columns['contract_version']}")
    tables = {}
    for name, spec in columns["tables"].items():
        identifier(name)
        for column in spec["columns"]:
            identifier(column)
        tables[name] = TableSpec(name, spec["file"], spec["key"], spec["columns"])
    identifier(columns["dataset_group"])
    return Contract(
        version=version,
        dataset_group=columns["dataset_group"],
        tables=tables,
        metadata_columns=tuple(columns["dlt_metadata_columns"]),
        policy=policy,
        directory=directory,
        columns_document=columns,
    )
