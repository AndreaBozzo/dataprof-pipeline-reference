"""Snapshot inputs: deterministic fixtures, manifests and retained copies.

A snapshot is a directory holding one JSONL file per table and a
``manifest.json`` naming the batch and the SHA-256 of every file. The digests
cover the input bytes only; they say nothing about the tables dlt builds from
them or the reports dataprof writes about those tables.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import shutil
import stat
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .contract import Contract

MANIFEST = "manifest.json"
MANIFEST_VERSION = 1
GENERATOR_VERSION = 1
DIGEST_SCOPE = "sha256 of the retained input bytes; not of loaded tables or reports"
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)
COUNTRIES = ("DE", "ES", "FR", "IT", "NL")
CURRENCIES = ("EUR", "GBP", "USD")


class InputError(Exception):
    """The snapshot cannot be used as given."""


@dataclass(frozen=True)
class Snapshot:
    directory: Path
    manifest: dict[str, Any]

    @property
    def batch_id(self) -> str:
        return self.manifest["source_batch_id"]

    @property
    def digest(self) -> str:
        return batch_digest(self.manifest)


Rows = dict[str, list[dict[str, Any]]]


def generate_rows(seed: int, customers: int, orders: int) -> Rows:
    """Healthy rows with fixed timestamps, so no expectation reads the clock."""
    rnd = random.Random(seed)
    customer_rows = [
        {
            "customer_id": f"C{i:05d}",
            "country_code": rnd.choice(COUNTRIES),
            "updated_at": _ts(BASE_TIME + timedelta(minutes=i)),
        }
        for i in range(customers)
    ]
    order_rows = []
    for i in range(orders):
        # 0.5% of orders have no customer (guest checkout), under the 2% ceiling.
        customer = None if rnd.random() < 0.005 else f"C{rnd.randrange(customers):05d}"
        cents = rnd.randrange(0, 50_000_00)
        order_rows.append(
            {
                "order_id": f"O{i:06d}",
                "customer_id": customer,
                "amount": f"{cents // 100}.{cents % 100:02d}",
                "currency": rnd.choice(CURRENCIES),
                "ordered_at": _ts(BASE_TIME + timedelta(seconds=37 * i)),
            }
        )
    return {"customers": customer_rows, "orders": order_rows}


def _ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def write_snapshot(
    directory: Path,
    rows: Rows,
    contract: Contract,
    batch_id: str,
    *,
    generator: dict[str, Any] | None = None,
    repairs_batch_id: str | None = None,
) -> Snapshot:
    """Write rows as a snapshot directory with its manifest."""
    directory.mkdir(parents=True, exist_ok=True)
    files = {}
    for table, spec in contract.tables.items():
        path = directory / spec.file
        with path.open("wb") as handle:
            for row in rows.get(table, []):
                handle.write(json.dumps(row, sort_keys=True).encode("utf-8") + b"\n")
        files[table] = {"path": spec.file, "sha256": file_sha256(path)}
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "dataset_group": contract.dataset_group,
        "source_batch_id": batch_id,
        "contract_version": contract.version,
        "files": files,
        "digest_scope": DIGEST_SCOPE,
        "generator": generator,
        "repairs_batch_id": repairs_batch_id,
    }
    (directory / MANIFEST).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    return Snapshot(directory, manifest)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def batch_digest(manifest: dict[str, Any]) -> str:
    """One digest for the batch: the per-file digests in table order."""
    lines = [f"{table}:{entry['sha256']}" for table, entry in sorted(manifest["files"].items())]
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _load_manifest(directory: Path) -> tuple[Path, dict[str, Any]]:
    path = directory / MANIFEST
    if not path.is_file():
        raise InputError(f"{directory}: no {MANIFEST}")
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InputError(f"{path}: not a JSON manifest: {exc}") from exc
    if not isinstance(manifest, dict):
        raise InputError(f"{path}: the manifest must be a JSON object")
    return path, manifest


def _whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def manifest_contract_version(directory: Path) -> int:
    """The contract version a snapshot declares, read before its contract is loaded."""
    path, manifest = _load_manifest(directory)
    version = manifest.get("contract_version")
    if not _whole(version) or version < 1:
        raise InputError(f"{path}: contract_version must be a positive whole number")
    return version


def read_manifest(directory: Path, contract: Contract) -> Snapshot:
    """Validate the whole manifest; every problem is an InputError naming the field."""
    path, manifest = _load_manifest(directory)
    if manifest.get("manifest_version") != MANIFEST_VERSION or not _whole(
        manifest.get("manifest_version")
    ):
        raise InputError(
            f"{path}: manifest_version must be {MANIFEST_VERSION}, "
            f"got {manifest.get('manifest_version')!r}"
        )
    batch = manifest.get("source_batch_id")
    if not isinstance(batch, str) or not batch:
        raise InputError(f"{path}: source_batch_id must be a non-empty string")
    if manifest.get("dataset_group") != contract.dataset_group:
        raise InputError(
            f"{path}: dataset group {manifest.get('dataset_group')!r}, "
            f"contract expects {contract.dataset_group!r}"
        )
    if manifest.get("contract_version") != contract.version:
        raise InputError(
            f"{path}: contract version {manifest.get('contract_version')!r}, "
            f"runner loaded {contract.version}"
        )
    repairs = manifest.get("repairs_batch_id")
    if repairs is not None and (not isinstance(repairs, str) or not repairs):
        raise InputError(f"{path}: repairs_batch_id must be a non-empty string or null")
    if manifest.get("generator") is not None and not isinstance(manifest["generator"], dict):
        raise InputError(f"{path}: generator must be an object or null")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise InputError(f"{path}: files must be an object keyed by table")
    if set(files) != set(contract.tables):
        raise InputError(
            f"{path}: files for {sorted(files)}, contract expects {sorted(contract.tables)}"
        )
    for table, entry in files.items():
        if not isinstance(entry, dict):
            raise InputError(f"{path}: files.{table} must be an object")
        if entry.get("path") != contract.tables[table].file:
            raise InputError(
                f"{path}: files.{table}.path is {entry.get('path')!r}, "
                f"contract expects {contract.tables[table].file!r}"
            )
        sha = entry.get("sha256")
        if not isinstance(sha, str) or not SHA256.match(sha):
            raise InputError(f"{path}: files.{table}.sha256 must be 64 lowercase hex digits")
    return Snapshot(directory, manifest)


def verify(snapshot: Snapshot) -> None:
    """Every file named by the manifest exists and has the recorded digest."""
    for table, entry in sorted(snapshot.manifest["files"].items()):
        path = snapshot.directory / entry["path"]
        if not path.is_file():
            raise InputError(f"{path}: missing ({table})")
        actual = file_sha256(path)
        if actual != entry["sha256"]:
            raise InputError(
                f"{path}: sha256 {actual} does not match the manifest's {entry['sha256']}"
            )


def retain(snapshot: Snapshot, destination: Path) -> Snapshot:
    """Copy the snapshot into the run's own directory and verify the copies.

    The copies are made read-only: the runner never writes to them again, and
    the bit makes an accidental write fail loudly. It is not a security control.
    """
    verify(snapshot)
    destination.mkdir(parents=True, exist_ok=False)
    names = [MANIFEST] + [entry["path"] for entry in snapshot.manifest["files"].values()]
    for name in names:
        shutil.copyfile(snapshot.directory / name, destination / name)
    retained = Snapshot(destination, snapshot.manifest)
    verify(retained)
    for name in names:
        os.chmod(destination / name, stat.S_IREAD)
    return retained


@dataclass
class KeyTracker:
    """Which expected keys each table's records carried at least once."""

    expected: dict[str, tuple[str, ...]]
    seen: dict[str, set[str]]

    @classmethod
    def for_contract(cls, contract: Contract) -> KeyTracker:
        return cls(
            {t: tuple(s.columns) for t, s in contract.tables.items()},
            {t: set() for t in contract.tables},
        )

    def absent(self) -> dict[str, list[str]]:
        return {
            table: [key for key in keys if key not in self.seen[table]]
            for table, keys in self.expected.items()
            if any(key not in self.seen[table] for key in keys)
        }


def iter_records(
    path: Path, table: str, keys: KeyTracker, on_record: Callable[[int], None] | None = None
) -> Iterator[dict[str, Any]]:
    """Yield JSON objects from a JSONL file, one per non-empty line.

    Values are passed to dlt as written; the frozen contract types them. A
    line that is not a JSON object is an input error naming the line.
    """
    with path.open("rb") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise InputError(f"{path}:{number}: not JSON: {exc}") from exc
            if not isinstance(record, dict):
                raise InputError(f"{path}:{number}: expected a JSON object")
            keys.seen[table].update(record)
            if on_record is not None:
                on_record(number)
            yield record
