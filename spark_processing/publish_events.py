import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, struct, to_json

TRANSACTION_FIELDS = [
    "TransactionID", "isFraud", "TransactionDT", "TransactionAmt",
    "ProductCD", "card1", "card2", "card3", "card4", "card5",
    "card6", "addr1", "addr2", "dist1", "dist2",
    "P_emaildomain", "R_emaildomain",
    "C1", "C2", "C5", "C6", "C9", "C11", "C13", "C14",
    "D1", "D4", "D10", "D15", "DeviceType"
]

COMPLAINT_FIELDS = [
    "ComplaintID", "TransactionID", "ComplaintDate",
    "ComplaintCategory", "ComplaintText", "ComplaintStatus"
]

TEST_TRANSACTION_IDS = [
    "2987024", "2987007", "2987053", "2987052", "2987002"
]

TEST_COMPLAINT_IDS = [
    "CMP00004", "CMP00001", "CMP00005", "CMP00002", "CMP00003"
]

def publish(df, fields, topic, key_field, exclusion_field, excluded_ids):
    selected = df.select(*fields)

    selected = selected.filter(
        ~col(exclusion_field).cast("string").isin(excluded_ids)
    )

    events = selected.withColumn(
        "sent_at",
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    )

    messages = events.select(
        col(key_field).cast("string").alias("key"),
        to_json(struct(*[col(c) for c in events.columns])).alias("value")
    ).repartition(3)

    count = messages.count()

    messages.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-recovery:9092") \
        .option("topic", topic) \
        .save()

    print(f"Published {count} events to topic '{topic}'")

def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    spark = SparkSession.builder.appName("AfriMarketEventPublisher").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    transactions = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("/shared_data/afrimarket_transactions.csv")

    complaints = spark.read \
        .option("header", True) \
        .option("inferSchema", False) \
        .csv("/shared_data/afrimarket_complaints.csv")

    print("Publishing remaining transaction events...")
    publish(
        transactions, TRANSACTION_FIELDS, "transactions",
        "TransactionID", "TransactionID", TEST_TRANSACTION_IDS
    )

    print("Publishing remaining complaint events...")
    publish(
        complaints, COMPLAINT_FIELDS, "complaints",
        "TransactionID", "ComplaintID", TEST_COMPLAINT_IDS
    )

    spark.stop()

if __name__ == "__main__":
    main()
