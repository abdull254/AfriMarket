"""
Checks that the customer features only look at the past.

Run: pytest tests/
"""
import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from features import ENGINEERED, add_features, add_uid, clean  # noqa: E402

COLS = ["TransactionID", "TransactionDT", "TransactionAmt", "card1", "addr1"]
ROWS = [
    (1, 1000, 10.0, 111, 200),
    (2, 1500, 20.0, 111, 200),     # same customer, 500s later
    (3, 2000, 99.5, 111, 200),     # same customer
    (4, 2000, 5.0, 222, 300),      # a different customer
]
FUTURE = [(5, 2100, 500.0, 111, 200), (6, 90000, 1.0, 111, 200)]


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("feature-tests").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def feats(spark, rows):
    df = spark.createDataFrame(rows, COLS)
    out = add_features(add_uid(clean(df))).orderBy("TransactionID").collect()
    return {r.TransactionID: r for r in out}


def test_future_rows_do_not_change_past_features(spark):
    before = feats(spark, ROWS)
    after = feats(spark, ROWS + FUTURE)
    for tid in before:
        for col in ENGINEERED:
            assert before[tid][col] == after[tid][col], f"{col} for txn {tid} saw the future"


def test_counts_and_gaps(spark):
    f = feats(spark, ROWS)
    assert f[1].uid_prev_txns == 0 and f[1].secs_since_last_txn == -1   # first purchase
    assert f[2].uid_prev_txns == 1 and f[2].secs_since_last_txn == 500
    assert f[3].txns_last_1h == 2
    assert f[3].amt_vs_uid_mean == pytest.approx(99.5 / 15.0)
    assert f[4].uid_prev_txns == 0                                      # other customer
