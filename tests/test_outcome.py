"""How dataprof verdicts and SQL results combine into a workflow outcome."""

from __future__ import annotations

import pytest

from pipeline_reference import evaluate
from pipeline_reference.evaluate import Evidence
from pipeline_reference.workflow import run_snapshot


def _evidence(verdicts: dict[str, str], sql_passed: list[bool]) -> Evidence:
    return Evidence(
        gates={table: {"verdict": v, "checks": []} for table, v in verdicts.items()},
        sql_assertions=[{"id": f"a{i}", "passed": p} for i, p in enumerate(sql_passed)],
    )


@pytest.mark.parametrize(
    ("verdicts", "sql", "expected"),
    [
        ({"a": "pass", "b": "pass"}, [True], "accepted"),
        ({"a": "fail", "b": "inconclusive"}, [True], "rejected"),
        ({"a": "inconclusive", "b": "pass"}, [False], "rejected"),
        ({"a": "inconclusive", "b": "pass"}, [True], "inconclusive"),
        ({"a": "pass", "b": "pass"}, [True, False], "rejected"),
    ],
)
def test_known_violation_wins_over_missing_evidence(verdicts, sql, expected):
    assert _evidence(verdicts, sql).outcome() == expected


def test_unknown_verdict_never_promotes(ws, snapshots, monkeypatch):
    class Gate:
        verdict = "probably"

    real_profile = evaluate.dataprof.profile

    def profile(*args, **kwargs):
        report = real_profile(*args, **kwargs)
        report.check = lambda **_: Gate()  # ty: ignore[invalid-assignment]  # a test double
        return report

    monkeypatch.setattr(evaluate.dataprof, "profile", profile)
    result = run_snapshot(ws, snapshots("healthy", "b1"))
    assert result.outcome == "operational_error"
    assert "UnknownVerdict" in "\n".join(result.lines)
