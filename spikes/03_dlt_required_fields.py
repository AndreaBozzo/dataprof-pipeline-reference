import os, shutil, dlt, duckdb

WS = os.path.join(os.path.dirname(__file__), "ws3")
shutil.rmtree(WS, ignore_errors=True)
os.makedirs(WS)
conn = duckdb.connect(os.path.join(WS, "r.duckdb"))
COLS = {
    "order_id": {"data_type": "text", "nullable": False},
    "amount": {"data_type": "decimal", "precision": 12, "scale": 2, "nullable": False},
}
for label, data in [
    ("missing required", [{"order_id": "O1"}]),
    ("null required", [{"order_id": "O1", "amount": None}]),
]:
    ds = "c_" + label.split()[0]
    p = dlt.pipeline(
        pipeline_name="p" + ds,
        pipelines_dir=os.path.join(WS, ".dlt"),
        destination=dlt.destinations.duckdb(conn),
        dataset_name=ds,
    )
    try:
        p.run(dlt.resource(data, name="orders", columns=COLS))
        print(label, "OK", conn.sql(f"select * from {ds}.orders").fetchall())
    except Exception as e:
        msg = [l for l in str(e).splitlines() if "null" in l.lower() or "constraint" in l.lower()]
        print(label, "ERR", type(e).__name__, (msg or [str(e)[:200]])[0][:200])
