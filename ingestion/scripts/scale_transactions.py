"""
Grows the 30,000-row transactions file into a much bigger one, or streams new
transactions into Kafka without stopping, so Spark and HDFS have real volume.

How new rows are made: the original 183 days are replayed as "copies" that come
after each other in time. In each copy
  - TransactionID gets +1,000,000 per copy (no clashes),
  - TransactionDT is moved forward by one full period,
  - TransactionAmt changes by up to +/-5 %,
  - 30 % of customers (card1) are swapped for new customers, the rest come back,
    so the customer history features keep working.
All other columns (V, C, D, M, id_ ...) are copied unchanged, so the file has the
exact same columns as data/afrimarket_transactions.csv and works with
train_fraud_model.py, the producer and generate_star_schema.py.

Usage:
  # On your machine, from the repo root: 10 x bigger file (300,000 rows, about 370 MB)
  python ingestion/scripts/scale_transactions.py --copies 9

  # Inside the producer container: keep sending new transactions to Kafka,
  # 50 per second, until Ctrl+C
  docker compose exec producer python scale_transactions.py --stream --rate 50

  # Print a few JSON messages instead of using Kafka (quick look)
  python ingestion/scripts/scale_transactions.py --stream --stdout --max-rows 5
"""
import argparse
import itertools
import json
import os
import sys
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
import config  # noqa: E402

# Inside the producer container TRANSACTIONS_FILE is set; on your machine use the repo copy
DEFAULT_TRANSACTIONS = (os.environ.get("TRANSACTIONS_FILE")
                        or os.path.join(ROOT, "data", "afrimarket_transactions.csv"))

ID_STEP = 1_000_000
NEW_CUSTOMER_SHARE = 0.30
CARD1_OFFSET = 20_000          # card1 in the source data is below 20,000


def make_copy(base, k, period, rng):
    """Copy number k (1, 2, ...) of the base data, moved k periods forward."""
    df = base.copy()
    df["TransactionID"] = df["TransactionID"] + k * ID_STEP
    df["TransactionDT"] = df["TransactionDT"] + k * period
    df["TransactionAmt"] = (df["TransactionAmt"] * rng.uniform(0.95, 1.05, len(df))).round(2)
    cards = df["card1"].dropna().unique()
    new = set(cards[rng.random(len(cards)) < NEW_CUSTOMER_SHARE])
    swap = df["card1"].isin(new)
    df.loc[swap, "card1"] = df.loc[swap, "card1"] + k * CARD1_OFFSET
    return df


def write_file(args, base, period, rng):
    out = args.out or os.path.join(ROOT, "data", "generated",
                                   f"afrimarket_transactions_{len(base) * (args.copies + 1) // 1000}k.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    base.to_csv(out, index=False)
    for k in range(1, args.copies + 1):
        make_copy(base, k, period, rng).to_csv(out, mode="a", header=False, index=False)
        print(f"  copy {k}/{args.copies} written")
    print(f"Wrote {len(base) * (args.copies + 1):,} rows to {out}")


def stream(args, base, period, rng):
    base = base.sort_values("TransactionDT").reset_index(drop=True)
    cols = [c for c in config.TXN_COLUMNS if c in base.columns]
    ref = pd.Timestamp(config.REFERENCE_DATE)
    send = None
    if not args.stdout:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                                 key_serializer=lambda k: str(k).encode(),
                                 value_serializer=lambda v: json.dumps(v).encode(),
                                 acks="all", linger_ms=50, compression_type="gzip")

        def send(msg):
            producer.send("transactions", key=msg.get(config.TXN_KEY_COL), value=msg)
    sent = 0
    try:
        for k in itertools.count(args.start_copy):
            chunk = make_copy(base, k, period, rng)[cols]
            for row in chunk.itertuples(index=False):
                msg = {c: (None if isinstance(v, float) and np.isnan(v) else (v.item() if hasattr(v, "item") else v))
                       for c, v in zip(cols, row)}
                msg["transaction_time"] = (ref + pd.Timedelta(seconds=msg["TransactionDT"])).isoformat()
                msg["sent_at"] = pd.Timestamp.now(tz="UTC").isoformat()
                if send:
                    send(msg)
                else:
                    print(json.dumps(msg))
                sent += 1
                if args.max_rows and sent >= args.max_rows:
                    return
                if args.rate > 0:
                    time.sleep(1.0 / args.rate)
                if sent % 1000 == 0:
                    print(f"  {sent:,} sent (copy {k})", file=sys.stderr)
    finally:
        if send:
            producer.flush()
        print(f"Stopped after {sent:,} transactions", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--transactions", default=DEFAULT_TRANSACTIONS)
    ap.add_argument("--copies", type=int, default=9, help="extra copies to add (9 -> 10 x the rows)")
    ap.add_argument("--out", help="output CSV (default data/generated/afrimarket_transactions_<N>k.csv)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--stream", action="store_true", help="send rows continuously instead of writing a file")
    ap.add_argument("--rate", type=float, default=20.0, help="stream: transactions per second (0 = as fast as possible)")
    ap.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP, help="stream: Kafka address")
    ap.add_argument("--stdout", action="store_true", help="stream: print JSON lines instead of using Kafka")
    ap.add_argument("--start-copy", type=int, default=1, help="stream: first copy number (avoids reusing IDs)")
    ap.add_argument("--max-rows", type=int, default=0, help="stream: stop after this many rows (0 = never)")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    base = pd.read_csv(args.transactions)
    period = int(base["TransactionDT"].max() - base["TransactionDT"].min()) + 86_400
    print(f"Loaded {len(base):,} rows; one period = {period / 86400:.0f} days", file=sys.stderr)
    if args.stream:
        stream(args, base, period, rng)
    else:
        write_file(args, base, period, rng)


if __name__ == "__main__":
    main()
