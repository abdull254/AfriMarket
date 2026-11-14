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

# Raw card / address numbers are identifiers, not quantities. The no-IDs model
# drops these and relies on the per-customer features built from them instead.
ID_COLUMNS = ["card1", "card2", "card3", "card5", "addr1"]


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


def add_row_features(df):
    """Features that need only the purchase itself. Used by training and live scoring."""
    return (df
            # TransactionDT counts seconds from an unknown start point, so this is
            # "hour of a 24-hour cycle", shifted by an unknown offset, not clock time.
            .withColumn("hour_of_day", F.floor(F.col(TIME) / 3600) % 24)
            # Cents part of the amount hints at currency conversion, a known IEEE-CIS signal.
            .withColumn("amt_cents", F.col("TransactionAmt") - F.floor("TransactionAmt")))


def fill_missing(df):
    return df.fillna(-1, subset=NUMERIC).fillna("missing", subset=CATEGORICAL)


def add_features(df):
    """Adds ENGINEERED columns, then fills missing values. Expects add_uid first.

    Point-in-time rule: every customer feature uses only purchases from the same
    customer with a STRICTLY EARLIER TransactionDT. The current purchase, and any
    other purchase in the same second, is never counted. online_features.py
    implements exactly the same rule for live scoring (checked by parity_test.py).
    """
    t = F.col(TIME).cast("long")
    by_uid_time = Window.partitionBy("uid").orderBy(t)
    earlier = by_uid_time.rangeBetween(Window.unboundedPreceding, -1)

    df = (add_row_features(df)
          .withColumn("uid_prev_txns", F.count("*").over(earlier))
          .withColumn("txns_last_1h", F.count("*").over(by_uid_time.rangeBetween(-3600, -1)))
          .withColumn("txns_last_24h", F.count("*").over(by_uid_time.rangeBetween(-86400, -1)))
          .withColumn("secs_since_last_txn", F.col(TIME) - F.max(TIME).over(earlier))
          .withColumn("amt_vs_uid_mean",
                      F.col("TransactionAmt") / F.avg("TransactionAmt").over(earlier)))
    return fill_missing(df)
