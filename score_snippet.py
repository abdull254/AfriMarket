"""
Real-time scoring for the Spark Structured Streaming job.

Customer-history features come from the online feature store (Redis, see
online_features.py), NOT from HDFS. Each micro-batch only touches the running
state of the customers in that batch, so scoring time does not grow with
history, and HDFS being down does not stop scoring. HDFS is used only for
nightly retraining.

Run with:
  spark-submit --py-files features.py,online_features.py your_streaming_job.py

Settings (environment variables): MODEL_DIR, REDIS_HOST, REDIS_PORT.
"""
import os

import redis
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

from features import TIME, clean, add_row_features, fill_missing
from online_features import ONLINE, RedisStore, compute_online_features

MODEL_DIR = os.getenv("MODEL_DIR", "/fraud-data/model")        # --model-dir used in training
REDIS_HOST = os.getenv("REDIS_HOST", "redis")                  # redis container on the team network
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

spark = SparkSession.builder.getOrCreate()
model = PipelineModel.load(MODEL_DIR)
th = spark.read.json(MODEL_DIR.rstrip("/") + "_thresholds").first()
REVIEW_AT, BLOCK_AT = th["review_at"], th["block_at"]
store = RedisStore(redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=0.2))


def score_batch(batch_df):
    """batch_df: new purchases with the 27 model columns. Returns id, score, decision."""
    batch_df = batch_df.dropDuplicates(["TransactionID"])
    keys = ["TransactionID", TIME, "TransactionAmt", "card1", "addr1"]
    rows = [r.asDict() for r in batch_df.select(*keys).collect()]      # a micro-batch is small
    feats = compute_online_features(rows, store)
    feat_df = spark.createDataFrame(
        [(r["TransactionID"], *[float(f[c]) for c in ONLINE], f["features_degraded"])
         for r, f in zip(rows, feats)],
        f"TransactionID {batch_df.schema['TransactionID'].dataType.simpleString()}, "
        + ", ".join(f"{c} double" for c in ONLINE) + ", features_degraded boolean")

    # Same row features and missing-value rules as training (features.py).
    df = fill_missing(add_row_features(clean(batch_df)).join(feat_df, "TransactionID", "left"))

    decision = (F.when(F.col("risk_score") >= BLOCK_AT,
                       # Never auto-block on incomplete information: send to an analyst instead.
                       F.when(F.col("features_degraded"), "REVIEW").otherwise("BLOCK"))
                 .when(F.col("risk_score") >= REVIEW_AT, "REVIEW")
                 .otherwise("APPROVE"))
    return (model.transform(df)
            .withColumn("risk_score", vector_to_array("probability")[1])
            .withColumn("decision", decision)
            .withColumn("scored_at", F.current_timestamp())
            .select("TransactionID", TIME, "risk_score", "decision", "features_degraded", "scored_at"))


def handle_batch(batch_df, batch_id):
    scored = score_batch(batch_df)
    # Upsert into MySQL scores (TransactionID primary key) so duplicates are harmless.
    scored.show(truncate=False)

# parsed = <stream from Kafka "transactions" topic, parsed to the 27 columns>
# parsed.writeStream.foreachBatch(handle_batch).start().awaitTermination()
