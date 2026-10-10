"""Named scenarios: a healthy fixture plus one deliberate defect each.

Each scenario changes the healthy rows in one way and states the workflow
outcome it must produce. The defects are planted at known counts so tests can
compare observed numbers with the truth.
"""

from __future__ import annotations

import copy
import random
from collections.abc import Callable
from dataclasses import dataclass

from .source import Rows, generate_rows

SEED = 20261010


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    expected: str  # workflow outcome: accepted, rejected or inconclusive
    mutate: Callable[[Rows, random.Random], None]
    profile_max_rows: int | None = None


def _healthy(rows: Rows, rnd: random.Random) -> None:
    pass


def _null_customer_ids(rows: Rows, rnd: random.Random) -> None:
    """8% of orders lose their customer_id: four times the 2% ceiling."""
    orders = rows["orders"]
    already = sum(1 for r in orders if r["customer_id"] is None)
    target = round(len(orders) * 0.08)
    candidates = [r for r in orders if r["customer_id"] is not None]
    for row in rnd.sample(candidates, target - already):
        row["customer_id"] = None


def _duplicate_order_ids(rows: Rows, rnd: random.Random) -> None:
    """Ten orders reuse another order's ID; dlt's _dlt_id keeps the rows distinct."""
    orders = rows["orders"]
    for row in rnd.sample(orders[10:], 10):
        row["order_id"] = orders[rnd.randrange(10)]["order_id"]


def _orphan_customer_ids(rows: Rows, rnd: random.Random) -> None:
    """Five orders name customers that do not exist."""
    for index, row in enumerate(rnd.sample(rows["orders"], 5)):
        row["customer_id"] = f"C9{index:04d}X"


def _negative_amount(rows: Rows, rnd: random.Random) -> None:
    rnd.choice(rows["orders"])["amount"] = "-1.00"


def _unknown_currency(rows: Rows, rnd: random.Random) -> None:
    rnd.choice(rows["orders"])["currency"] = "XXX"


def _extra_column(rows: Rows, rnd: random.Random) -> None:
    for row in rows["orders"][:3]:
        row["discount"] = "1.00"


def _bad_amount(rows: Rows, rnd: random.Random) -> None:
    rnd.choice(rows["orders"])["amount"] = "twelve"


def _missing_amount(rows: Rows, rnd: random.Random) -> None:
    del rnd.choice(rows["orders"])["amount"]


def _empty_orders(rows: Rows, rnd: random.Random) -> None:
    rows["orders"].clear()


SCENARIOS = {
    s.name: s
    for s in (
        Scenario("healthy", "valid snapshot", "accepted", _healthy),
        Scenario(
            "null_customer_ids", "8% of orders without customer_id", "rejected", _null_customer_ids
        ),
        Scenario("duplicate_order_ids", "ten reused order IDs", "rejected", _duplicate_order_ids),
        Scenario(
            "orphan_customer_ids",
            "five orders naming unknown customers",
            "rejected",
            _orphan_customer_ids,
        ),
        Scenario("negative_amount", "one negative amount", "rejected", _negative_amount),
        Scenario("unknown_currency", "one currency outside the set", "rejected", _unknown_currency),
        Scenario("extra_column", "orders gain a discount field", "rejected", _extra_column),
        Scenario("bad_amount", "one amount that is not a number", "rejected", _bad_amount),
        Scenario("missing_amount", "one order without amount", "rejected", _missing_amount),
        Scenario("empty_orders", "no orders at all", "rejected", _empty_orders),
        Scenario(
            "capped_profile",
            "valid snapshot profiled with a deliberate row cap",
            "inconclusive",
            _healthy,
            profile_max_rows=500,
        ),
    )
}


def build(name: str, *, seed: int = SEED, customers: int = 1_000, orders: int = 10_000) -> Rows:
    scenario = SCENARIOS[name]
    rows = copy.deepcopy(generate_rows(seed, customers, orders))
    scenario.mutate(rows, random.Random(f"{seed}:{name}"))
    return rows
