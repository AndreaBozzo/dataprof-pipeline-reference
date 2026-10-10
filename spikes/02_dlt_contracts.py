import os
import shutil

import dlt
import duckdb

WS = os.path.join(os.path.dirname(__file__), "ws2")
shutil.rmtree(WS, ignore_errors=True)
os.makedirs(WS)
conn = duckdb.connect(os.path.join(WS, "ref.duckdb"))

COLS = {
    "order_id": {"data_type": "text", "nullable": False},
    "customer_id": {"data_type": "text"},
    "amount": {"data_type": "decimal", "precision": 12, "scale": 2},
    "currency": {"data_type": "text"},
    "ordered_at": {"data_type": "timestamp", "timezone": True},
}

good = [
    {
        "order_id": "O1",
        "customer_id": "C1",
        "amount": "12.34",
        "currency": "EUR",
        "ordered_at": "2026-01-01T00:00:00Z",
    }
]
badtype = [
    {
        "order_id": "O1",
        "customer_id": "C1",
        "amount": "abc",
        "currency": "EUR",
        "ordered_at": "2026-01-01T00:00:00Z",
    }
]
missing = [
    {"order_id": "O1", "customer_id": "C1", "currency": "EUR", "ordered_at": "2026-01-01T00:00:00Z"}
]
extra = [dict(good[0], discount=1)]


def run(label, data, contract, pipeline_name, dataset):
    p = dlt.pipeline(
        pipeline_name=pipeline_name,
        pipelines_dir=os.path.join(WS, ".dlt"),
        destination=dlt.destinations.duckdb(conn),
        dataset_name=dataset,
    )
    res = dlt.resource(
        data, name="orders", columns=COLS, write_disposition="replace", schema_contract=contract
    )
    try:
        p.run(res)
        cols = conn.sql(
            f"select column_name, data_type from information_schema.columns where table_schema='{dataset}' and table_name='orders'"
        ).fetchall()
        print(
            label,
            "OK",
            cols,
            conn.sql(f"select amount, ordered_at from {dataset}.orders").fetchall(),
        )
    except Exception as e:
        print(label, "ERR", type(e).__name__, str(e).splitlines()[-1][:160])


# 1. no contract: coercion of string decimal/timestamp; bad type; missing; extra
run("nocontract good", good, None, "p_a", "c_a1")
run("nocontract badtype", badtype, None, "p_b", "c_b1")
run("nocontract missing", missing, None, "p_c", "c_c1")
run("nocontract extra", extra, None, "p_d", "c_d1")

# 2. one shared pipeline, fresh dataset per run, freeze after a non-frozen bootstrap
run("shared bootstrap", good, None, "p_shared", "c_s1")
run("shared freeze good new dataset", good, "freeze", "p_shared", "c_s2")
run("shared freeze extra new dataset", extra, "freeze", "p_shared", "c_s3")
run("shared freeze badtype new dataset", badtype, "freeze", "p_shared", "c_s4")

# 3. explicit stored schema with fresh pipeline per run
sch = dlt.Schema("shop")
p0 = dlt.pipeline(
    pipeline_name="p_boot",
    pipelines_dir=os.path.join(WS, ".dlt"),
    destination=dlt.destinations.duckdb(conn),
    dataset_name="c_boot",
)
p0.run(
    dlt.resource(good, name="orders", columns=COLS, write_disposition="replace"),
    schema=dlt.Schema("shop"),
)
stored = p0.default_schema.to_dict()
print("schema tables", [t for t in stored["tables"] if not t.startswith("_dlt")])


@dlt.source(schema=dlt.Schema.from_dict(stored))
def shop(data):
    yield dlt.resource(data, name="orders", columns=COLS, write_disposition="replace")


for label, data in [
    ("fresh+schema freeze good", good),
    ("fresh+schema freeze extra", extra),
    ("fresh+schema freeze badtype", badtype),
]:
    ds = "c_f" + str(abs(hash(label)) % 1000)
    p = dlt.pipeline(
        pipeline_name="pf_" + ds,
        pipelines_dir=os.path.join(WS, ".dlt"),
        destination=dlt.destinations.duckdb(conn),
        dataset_name=ds,
    )
    try:
        p.run(shop(data), schema_contract="freeze")
        print(
            label,
            "OK",
            conn.sql(
                f"select column_name from information_schema.columns where table_schema='{ds}' and table_name='orders'"
            ).fetchall(),
        )
    except Exception as e:
        print(label, "ERR", type(e).__name__, str(e).splitlines()[-1][:160])
