import json
import os
import random
import shutil
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import dataprof
import dlt
import duckdb

WS = os.path.join(os.path.dirname(__file__), "ws")
shutil.rmtree(WS, ignore_errors=True)
os.makedirs(WS)
DB = os.path.join(WS, "ref.duckdb")


def rows(n_orders, null_pct, seed=7):
    rnd = random.Random(seed)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    customers = [
        {
            "customer_id": f"C{i:05d}",
            "country_code": rnd.choice(["IT", "DE", "FR"]),
            "updated_at": base + timedelta(minutes=i),
        }
        for i in range(1000)
    ]
    orders = []
    for i in range(n_orders):
        cid = None if rnd.random() < null_pct else f"C{rnd.randrange(1000):05d}"
        orders.append(
            {
                "order_id": f"O{i:06d}",
                "customer_id": cid,
                "amount": Decimal(rnd.randrange(0, 100000)) / 100,
                "currency": rnd.choice(["EUR", "USD"]),
                "ordered_at": base + timedelta(seconds=i),
            }
        )
    return customers, orders


COLS = {
    "customers": {
        "customer_id": {"data_type": "text", "nullable": False},
        "country_code": {"data_type": "text"},
        "updated_at": {"data_type": "timestamp", "timezone": True},
    },
    "orders": {
        "order_id": {"data_type": "text", "nullable": False},
        "customer_id": {"data_type": "text"},
        "amount": {"data_type": "decimal", "precision": 12, "scale": 2},
        "currency": {"data_type": "text"},
        "ordered_at": {"data_type": "timestamp", "timezone": True},
    },
}


def load(conn, run, customers, orders, contract=None):
    @dlt.source(name="shop")
    def shop():
        yield dlt.resource(
            customers,
            name="customers",
            columns=COLS["customers"],
            write_disposition="replace",
            schema_contract=contract,
        )
        yield dlt.resource(
            orders,
            name="orders",
            columns=COLS["orders"],
            write_disposition="replace",
            schema_contract=contract,
        )

    p = dlt.pipeline(
        pipeline_name=f"cand_{run}",
        pipelines_dir=os.path.join(WS, ".dlt"),
        destination=dlt.destinations.duckdb(conn),
        dataset_name=f"cand_{run}",
    )
    info = p.run(shop())
    return p, info


conn = duckdb.connect(DB)
print("tz before", conn.sql("select current_setting('TimeZone')").fetchone())
print("search_path before", conn.sql("select current_setting('search_path')").fetchone())

# A. healthy load with an external connection
c, o = rows(10_000, 0.002)
p, info = load(conn, "r1", c, o)
print("A has_failed_jobs", info.has_failed_jobs, "load ids", info.loads_ids)
print("A search_path after", conn.sql("select current_setting('search_path')").fetchone())
print("A conn still open:", conn.sql("select 1").fetchone())
print(
    "A tables",
    conn.sql(
        "select table_schema, table_name from information_schema.tables where table_schema like 'cand_r1%' order by 1,2"
    ).fetchall(),
)
print(
    "A types",
    conn.sql(
        "select table_name, column_name, data_type from information_schema.columns where table_schema='cand_r1' and table_name in ('orders','customers') order by 1, ordinal_position"
    ).fetchall(),
)
print(
    "A _dlt_loads",
    conn.sql("select load_id, status, schema_name from cand_r1._dlt_loads").fetchall(),
)

# B. empty orders resource
try:
    p2, info2 = load(conn, "r2", c, [])
    print(
        "B empty orders tables",
        conn.sql(
            "select table_name from information_schema.tables where table_schema='cand_r2' order by 1"
        ).fetchall(),
    )
except Exception as e:
    print("B empty error", type(e).__name__, str(e)[:300])

# B2. frozen contract on first load into a fresh dataset
try:
    load(conn, "r3", c, o, contract="freeze")
    print("B2 freeze on first load: accepted")
except Exception as e:
    print("B2 freeze on first load error", type(e).__name__, str(e)[:200])

# B3. unexpected column under freeze (reuse r1 pipeline so schema exists)
o_extra = [dict(r, discount=1) for r in o[:10]]
try:

    @dlt.source(name="shop")
    def shop2():
        yield dlt.resource(
            o_extra,
            name="orders",
            columns=COLS["orders"],
            write_disposition="replace",
            schema_contract="freeze",
        )

    p.run(shop2())
    print("B3 extra column under freeze: accepted (BAD)")
except Exception as e:
    print("B3 extra column under freeze error", type(e).__name__, str(e)[:200])

# D. dataprof on DuckDB relation, in a transaction
conn.execute("SET TimeZone='UTC'")
conn.execute("BEGIN")
rel = conn.sql("select order_id, customer_id, amount, currency, ordered_at from cand_r1.orders")
print("D arrow schema", rel.arrow().schema if hasattr(rel, "arrow") else None)
rel = conn.sql("select order_id, customer_id, amount, currency, ordered_at from cand_r1.orders")
rep = dataprof.profile(rel, name="orders")
print("D rows", rep.rows if hasattr(rep, "rows") else None)
g = rep.check(
    max_null_percentage={"customer_id": 2.0}, require_metrics=["schema"] if False else None
)
print("D verdict healthy", g.verdict, [(ch.code, ch.status, ch.observed) for ch in g.checks])
doc = json.loads(rep.to_json())
print("D report keys", sorted(doc)[:20])
for cp in doc.get("column_profiles", []):
    print(
        "   col", cp.get("name"), cp.get("data_type"), cp.get("null_count"), cp.get("total_count")
    )
rel = conn.sql("select order_id, customer_id, amount, currency, ordered_at from cand_r1.orders")
rep_cap = dataprof.profile(rel, name="orders", max_rows=500)
g2 = rep_cap.check(max_null_percentage={"customer_id": 2.0})
print("D verdict capped", g2.verdict, [(ch.code, ch.status) for ch in g2.checks])
g3 = rep.check(min_quality_score=50)
print(
    "D min_quality_score",
    g3.verdict,
    rep.quality_score,
    [
        d
        for d in ("completeness", "consistency", "uniqueness", "accuracy", "timeliness")
        if getattr(rep.quality, d, None) is not None
    ],
    rep.quality.timeliness,
)
conn.execute("COMMIT")

c8, o8 = rows(10_000, 0.08)
load(conn, "r4", c8, o8)
rep8 = dataprof.profile(
    conn.sql("select order_id, customer_id, amount, currency, ordered_at from cand_r4.orders")
)
print("D verdict 8% null", rep8.check(max_null_percentage={"customer_id": 2.0}).verdict)

# C. transactional view replacement with failure between the two
conn.execute("CREATE SCHEMA IF NOT EXISTS published")
conn.execute("BEGIN")
conn.execute(
    "CREATE OR REPLACE VIEW published.customers AS SELECT customer_id, country_code, updated_at FROM cand_r1.customers"
)
conn.execute(
    "CREATE OR REPLACE VIEW published.orders AS SELECT order_id, customer_id, amount, currency, ordered_at FROM cand_r1.orders"
)
conn.execute("COMMIT")
conn.execute("BEGIN")
conn.execute(
    "CREATE OR REPLACE VIEW published.customers AS SELECT customer_id, country_code, updated_at FROM cand_r4.customers"
)
conn.execute("ROLLBACK")
print(
    "C after rollback customers view targets",
    conn.sql("select sql from duckdb_views() where schema_name='published'").fetchall(),
)
conn.close()

# C2. process death mid-transaction: reopen and check
import subprocess

code = f"""
import duckdb, os
c = duckdb.connect(r'{DB}')
c.execute('BEGIN')
c.execute("CREATE OR REPLACE VIEW published.customers AS SELECT customer_id, country_code, updated_at FROM cand_r4.customers")
os._exit(9)
"""
r = subprocess.run([sys.executable, "-c", code])
print("C2 child exit", r.returncode)
conn = duckdb.connect(DB)
print(
    "C2 after crash",
    conn.sql("select view_name, sql from duckdb_views() where schema_name='published'").fetchall(),
)

# E. second connection from another process while open
code2 = f"import duckdb; duckdb.connect(r'{DB}')"
r = subprocess.run([sys.executable, "-c", code2], capture_output=True, text=True)
print(
    "E second process open rc",
    r.returncode,
    r.stderr.strip().splitlines()[-1][:200] if r.stderr else "",
)
print("versions", sys.version.split()[0], dataprof.__version__, dlt.__version__, duckdb.__version__)
