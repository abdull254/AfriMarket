"""
Real-time scoring for the Spark Structured Streaming job.

The customer features (txns in last hour, amount vs usual, ...) need each
card's history, so every micro-batch is combined with that card's past
transactions from the HDFS raw data lake before computing features. Only the
new rows are scored and written out.

Run with: spark-submit --py-files features.py your_streaming_job.py
"""
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

from features import TIME, clean, add_uid, add_features

MODEL_DIR = "/fraud-data/model"                                 # --model-dir used in training
HISTORY_PATH = "hdfs://namenode:9000/data/transactions"        # raw data lake (parquet)

spark = SparkSession.builder.getOrCreate()
model = PipelineModel.load(MODEL_DIR)
th = spark.read.json(MODEL_DIR + "_thresholds").first()
REVIEW_AT, BLOCK_AT = th["review_at"], th["block_at"]


def score_batch(batch_df):
    """batch_df: new transactions with IEEE-CIS columns. Returns id, score, decision."""
    new = add_uid(clean(batch_df)).dropDuplicates(["TransactionID"]).withColumn("_new", F.lit(True))
    history = (add_uid(clean(spark.read.parquet(HISTORY_PATH)))
               .join(new.select("uid").distinct(), "uid")       # only these customers
               # If a new transaction already landed in the lake (retry, replay),
               # drop it from history so it is not counted twice in the features.
               .join(new.select("TransactionID"), "TransactionID", "left_anti")
               .filter(F.col(TIME) <= new.agg(F.max(TIME)).first()[0])  # no future rows
               .withColumn("_new", F.lit(False)))
    both = history.unionByName(new, allowMissingColumns=True)
    feats = add_features(both).filter("_new")
    return (model.transform(feats)
            .withColumn("risk_score", vector_to_array("probability")[1])
            .withColumn("decision",
                        F.when(F.col("risk_score") >= BLOCK_AT, "BLOCK")
                         .when(F.col("risk_score") >= REVIEW_AT, "REVIEW")
                         .otherwise("APPROVE"))
            .select("TransactionID", TIME, "risk_score", "decision"))


def handle_batch(batch_df, batch_id):
    scored = score_batch(batch_df)
    # scored.write.jdbc("jdbc:mysql://mysql:3306/fraud", "scores", mode="append",
    #                   properties={"user": "...", "password": "..."})
    scored.show(truncate=False)

# parsed = <stream from Kafka "transactions" topic, parsed to columns>
# parsed.writeStream.foreachBatch(handle_batch).start().awaitTermination()
