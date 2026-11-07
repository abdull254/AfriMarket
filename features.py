"""
Shared feature engineering for training and real-time scoring.

Every feature here looks only at the PAST of each customer (rows before the
current transaction), so it is safe to compute before the train/test split and
identical at scoring time.
"""
from pyspark.sql import Window, functions as F

LABEL = "isFraud"
TIME = "TransactionDT"          # seconds from a reference point (IEEE-CIS)

BASE_NUMERIC = ["TransactionAmt", "card1", "card2", "card3", "card5", "addr1", "dist1",
                "C1", "C2", "C5", "C6", "C9", "C11", "C13", "C14", "D1", "D4", "D10", "D15"]
CATEGORICAL = ["ProductCD", "card4", "card6", "P_emaildomain", "R_emaildomain", "DeviceType"]

ENGINEERED = ["hour_of_day", "amt_cents", "uid_prev_txns", "txns_last_1h", "txns_last_24h",
              "secs_since_last_txn", "amt_vs_uid_mean"]
NUMERIC = BASE_NUMERIC + ENGINEERED


def clean(df):
    """Make sure every expected column exists with the right type and no nulls."""
    for c in BASE_NUMERIC + [TIME]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
        df = df.withColumn(c, F.col(c).cast("double"))
    for c in CATEGORICAL:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df


def add_uid(df):
    """Rough customer ID: card number + billing address."""
    return df.withColumn("uid", F.concat_ws("_", F.col("card1").cast("long"),
                                            F.col("addr1").cast("long")))


def add_features(df):
    """Adds ENGINEERED columns, then fills missing values. Expects add_uid first."""
    by_uid = Window.partitionBy("uid").orderBy(TIME)
    prior = by_uid.rowsBetween(Window.unboundedPreceding, -1)
    t = F.col(TIME).cast("long")
    by_uid_time = Window.partitionBy("uid").orderBy(t)

    df = (df
          .withColumn("hour_of_day", F.floor(F.col(TIME) / 3600) % 24)
          # Cents part of the amount hints at currency conversion, a known IEEE-CIS signal.
          .withColumn("amt_cents", F.col("TransactionAmt") - F.floor("TransactionAmt"))
          .withColumn("uid_prev_txns", F.count("*").over(prior))
          .withColumn("txns_last_1h", F.count("*").over(by_uid_time.rangeBetween(-3600, -1)))
          .withColumn("txns_last_24h", F.count("*").over(by_uid_time.rangeBetween(-86400, -1)))
          .withColumn("secs_since_last_txn", F.col(TIME) - F.lag(TIME).over(by_uid))
          .withColumn("amt_vs_uid_mean",
                      F.col("TransactionAmt") / F.avg("TransactionAmt").over(prior)))
    df = df.fillna(-1, subset=NUMERIC).fillna("missing", subset=CATEGORICAL)
    return df
