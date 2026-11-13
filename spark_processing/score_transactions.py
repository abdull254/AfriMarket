import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, when
)
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

sys.path.insert(0, "/shared_data")
import features

HDFS = "hdfs://6eea8ba9e649:8020"
MODEL_PATH = HDFS + "/shared_data/member2_fraud_model"
THRESHOLD_PATH = HDFS + "/shared_data/member2_fraud_model_thresholds"
HISTORY_PATH = HDFS + "/fraud/processed/member2_transaction_history"
OUTPUT_PATH = HDFS + "/fraud/processed/member2_scored_transactions"
SOURCE_PATH = "file:///shared_data/afrimarket_transactions.csv"

spark = (
    SparkSession.builder
    .appName("AfriMarketMember2BatchScoring")
    .config("spark.hadoop.fs.defaultFS", HDFS)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    print("\nLoading source transactions...")
    source = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(SOURCE_PATH)
    )

    # Apply the same past-only feature engineering used during training.
    prepared = features.add_features(
        features.add_uid(features.clean(source))
    )

    feature_columns = (
        ["TransactionID", "TransactionDT", "isFraud"] +
        features.BASE_NUMERIC +
        features.ENGINEERED +
        features.CATEGORICAL
    )

    prepared = prepared.select(*feature_columns)

    print("Loading historical transaction IDs...")
    history_ids = (
        spark.read.parquet(HISTORY_PATH)
        .select(col("TransactionID").cast("long").alias("history_id"))
        .distinct()
    )

    print("Reading transaction events from Kafka...")
    kafka_events = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka-recovery:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # The Kafka payload supplies event identity and the evaluation label.
    # Feature values are taken from the matching source transaction.
    event_schema = source.schema
    events = (
        kafka_events
        .select(from_json(col("value").cast("string"), event_schema).alias("event"))
        .select(
            col("event.TransactionID").cast("long").alias("TransactionID")
        )
        .dropDuplicates(["TransactionID"])
    )

    scoring_events = (
        events.join(
            history_ids,
            events.TransactionID == history_ids.history_id,
            "left_anti"
        )
    )

    print("Joining Kafka events to prepared features...")
    scoring_data = (
        scoring_events.join(prepared, "TransactionID", "inner")
    )

    event_count = scoring_data.count()
    print("Kafka events selected for scoring:", event_count)

    if event_count == 0:
        raise RuntimeError("No new Kafka transactions were found for scoring.")

    print("Loading trained GBT model...")
    model = PipelineModel.load(MODEL_PATH)

    print("Loading decision thresholds...")
    threshold_row = (
        spark.read.json(THRESHOLD_PATH)
        .first()
    )
    review_at = float(threshold_row["review_at"])
    block_at = float(threshold_row["block_at"])

    print(f"Review threshold: {review_at}")
    print(f"Block threshold: {block_at}")

    print("Scoring transactions...")
    scored = model.transform(scoring_data)

    scored = (
        scored
        .withColumn("risk_score", vector_to_array(col("probability"))[1])
        .withColumn(
            "decision",
            when(col("risk_score") >= block_at, "BLOCK")
            .when(col("risk_score") >= review_at, "REVIEW")
            .otherwise("APPROVE")
        )
        .withColumn("scored_at", current_timestamp())
        .select(
            "TransactionID",
            "TransactionDT",
            "isFraud",
            "risk_score",
            "prediction",
            "decision",
            "scored_at"
        )
    )

    print("Saving scored transactions to HDFS...")
    scored.write.mode("errorifexists").parquet(OUTPUT_PATH)

    print("\nDecision summary:")
    scored.groupBy("decision").count().orderBy("decision").show()

    print("Evaluation summary:")
    scored.groupBy("isFraud", "decision").count().orderBy(
        "isFraud", "decision"
    ).show()

    print("Scoring completed successfully.")
    print("Output:", OUTPUT_PATH)

finally:
    spark.stop()
