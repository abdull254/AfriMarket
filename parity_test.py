"""
Checks that live features (online_features.py) equal training features
(features.py) for every purchase, replaying the CSV in time order.

  spark-submit --py-files features.py,online_features.py parity_test.py <transactions.csv> [--redis]
--redis uses an in-process fake Redis (pip install fakeredis) to test RedisStore.
"""
import sys
import pandas as pd
from pyspark.sql import SparkSession
from features import clean, add_uid, add_features
from online_features import ONLINE, MemoryStore, RedisStore, compute_online_features

path = sys.argv[1]
spark = SparkSession.builder.appName("feature-parity").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

batch = (add_features(add_uid(clean(spark.read.csv(path, header=True, inferSchema=True))))
         .select("TransactionID", *ONLINE).toPandas().set_index("TransactionID").sort_index())

raw = pd.read_csv(path, usecols=["TransactionID", "TransactionDT", "TransactionAmt", "card1", "addr1"])
rows = raw.astype(object).where(raw.notna(), None).to_dict("records")

if "--redis" in sys.argv:
    import fakeredis
    store, name = RedisStore(fakeredis.FakeRedis()), "RedisStore (fake Redis)"
else:
    store, name = MemoryStore(), "MemoryStore"

feats = compute_online_features(rows, store)
online = pd.DataFrame(feats, index=raw["TransactionID"]).sort_index()

# Replay everything again: duplicates must return identical features and not change state.
again = pd.DataFrame(compute_online_features(rows, store), index=raw["TransactionID"]).sort_index()

print(f"\n===== Feature parity: {name}, {len(raw):,} purchases =====")
ok = True
for c in ONLINE:
    diff = (online[c].astype(float) - batch[c].astype(float)).abs()
    bad = int((diff > 1e-6).sum())
    ok &= bad == 0
    print(f"{c:<22} mismatches: {bad}")
dup_bad = int((again[ONLINE].astype(float) - online[ONLINE].astype(float)).abs().gt(1e-9).sum().sum())
print(f"duplicate replay       mismatches: {dup_bad}")
print("PASS" if ok and dup_bad == 0 else "FAIL")
spark.stop()
