"""
Loads the CSV files from data/enriched/ into the MySQL star schema
(mysql/init/02_star_schema.sql).

Usage (from the repo root, with the MySQL container running; it listens on 3307):
  pip install pymysql pandas
  MYSQL_USER=... MYSQL_PASSWORD=... python data_generation/load_star_schema.py --create

  --create    run 02_star_schema.sql first (needed when the MySQL volume already existed)
  --replace   empty the star-schema tables before loading
"""
import argparse
import os

import pandas as pd
import pymysql

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Parents first, so the foreign keys are satisfied
LOAD_ORDER = ["dim_customer", "dim_seller", "dim_product", "dim_date", "dim_device",
              "fact_transaction", "fact_delivery", "fact_refund", "device_ip_history",
              "complaints_enriched"]
BATCH = 2000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(ROOT, "data", "enriched"))
    ap.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3307")))
    ap.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "fraud"))
    ap.add_argument("--create", action="store_true")
    ap.add_argument("--replace", action="store_true")
    args = ap.parse_args()

    conn = pymysql.connect(host=args.host, port=args.port, user=os.environ["MYSQL_USER"],
                           password=os.environ["MYSQL_PASSWORD"], database=args.database,
                           autocommit=False, charset="utf8mb4")
    cur = conn.cursor()
    if args.create:
        sql = open(os.path.join(ROOT, "mysql", "init", "02_star_schema.sql")).read()
        for stmt in sql.split(";"):
            body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
            if body and not body.upper().startswith("USE "):
                cur.execute(body)
    if args.replace:
        for table in reversed(LOAD_ORDER):
            cur.execute(f"DELETE FROM {table}")

    for table in LOAD_ORDER:
        df = pd.read_csv(os.path.join(args.dir, f"{table}.csv"))
        df = df.astype(object).where(df.notna(), None)
        df = df.replace({"True": 1, "False": 0, True: 1, False: 0})
        cols = ", ".join(f"`{c}`" for c in df.columns)
        marks = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT IGNORE INTO {table} ({cols}) VALUES ({marks})"
        rows = df.values.tolist()
        for i in range(0, len(rows), BATCH):
            cur.executemany(sql, rows[i:i + BATCH])
        conn.commit()
        print(f"  {table:20s} {len(rows):>8,} rows loaded")
    conn.close()


if __name__ == "__main__":
    main()
