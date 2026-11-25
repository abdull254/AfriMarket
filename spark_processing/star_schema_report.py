"""
Spark report over the star schema in data/enriched/ (or the same files on HDFS).

It joins the fact tables with the dimensions and prints, and writes as CSV:
  1. fraud rate by payment method and country
  2. fraud rate by local hour of day and by weekday
  3. fraud rate for new devices and for devices / IPs shared by several customers
  4. complaints linked to their outcome (delivery status, refund, chargeback)
  5. complaint resolution time by channel and category
  6. sellers with the most chargebacks

Usage:
  spark-submit spark_processing/star_schema_report.py --dir data/enriched --out results/star_schema
  spark-submit spark_processing/star_schema_report.py --dir hdfs://hadoop-container:9000/fraud/enriched
"""
import argparse
import os

from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--dir", default="data/enriched")
ap.add_argument("--out", default="results/star_schema")
args = ap.parse_args()

spark = SparkSession.builder.appName("afrimarket-star-schema-report").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def table(name):
    return spark.read.csv(f"{args.dir}/{name}.csv", header=True, inferSchema=True, escape='"', multiLine=True)


fact = table("fact_transaction")
seller = table("dim_seller")
date = table("dim_date")
deliv = table("fact_delivery")
refund = table("fact_refund")
comp = table("complaints_enriched")


def fraud_rate(df, *cols):
    return (df.groupBy(*cols).agg(F.count("*").alias("transactions"),
                                  F.sum("isFraud").alias("fraud"),
                                  F.round(100 * F.avg("isFraud"), 2).alias("fraud_pct"))
            .orderBy(F.desc("fraud_pct")))


reports = {
    "fraud_by_payment_method": fraud_rate(fact, "payment_method"),
    "fraud_by_country": fraud_rate(fact, "country"),
    "fraud_by_local_hour": fraud_rate(fact, "local_hour").orderBy("local_hour"),
    "fraud_by_weekday": fraud_rate(fact.join(date, "date_key"), "weekday", "weekday_name").orderBy("weekday"),
    "fraud_by_device_history": fraud_rate(
        fact.withColumn("shared_device", F.col("device_customers_so_far") > 1)
            .withColumn("shared_ip", F.col("ip_customers_so_far") > 1),
        "is_new_device_for_customer", "shared_device", "shared_ip"),
    "complaints_vs_outcome": (
        comp.join(refund.select("refund_id", "refund_type", "refund_status"), "refund_id", "left")
            .groupBy("ComplaintCategory", "delivery_status", "refund_type")
            .count().orderBy("ComplaintCategory", F.desc("count"))),
    "complaint_resolution_by_channel": (
        comp.groupBy("channel", "ComplaintCategory")
            .agg(F.count("*").alias("complaints"),
                 F.round(F.avg("resolution_hours"), 1).alias("avg_resolution_hours"),
                 F.round(100 * F.avg(F.when(F.col("ComplaintStatus") == "Resolved", 1).otherwise(0)), 1)
                  .alias("resolved_pct"))
            .orderBy("channel", "ComplaintCategory")),
    "sellers_by_chargebacks": (
        refund.filter(F.col("refund_type") == "CHARGEBACK")
              .join(fact.select("TransactionID", "seller_id"), "TransactionID")
              .groupBy("seller_id").agg(F.count("*").alias("chargebacks"),
                                        F.round(F.sum("amount_usd"), 2).alias("chargeback_usd"))
              .join(seller, "seller_id")
              .select("seller_id", "seller_name", "country", "rating", "is_verified", "joined_date",
                      "chargebacks", "chargeback_usd")
              .orderBy(F.desc("chargebacks")).limit(20)),
    "late_delivery_by_route": (
        deliv.filter(F.col("delivery_type") == "PHYSICAL")
             .groupBy("cross_border", "courier")
             .agg(F.count("*").alias("deliveries"),
                  F.round(F.avg("delivery_days"), 1).alias("avg_days"),
                  F.round(100 * F.avg(F.col("is_late").cast("int")), 1).alias("late_pct"))
             .orderBy("cross_border", "courier")),
}

for name, df in reports.items():
    print(f"\n=== {name} ===")
    df.show(25, truncate=False)
    if args.out:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(args.out, name))

spark.stop()
