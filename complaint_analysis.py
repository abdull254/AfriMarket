"""
AfriMarket complaint analysis (the delayed "Complaint NLP -> Link to original" path).

1. Read complaints (CSV here; Cassandra in the full pipeline).
2. Clean the text and tag each complaint with keyword rules:
   fraud_type  = UNAUTHORIZED_CHARGE / SUSPICIOUS_SELLER / NON_DELIVERY / SERVICE
   fraud_signal = HIGH / MEDIUM / NONE
3. Check the rules against the hand-labelled ComplaintCategory.
4. Link each complaint to its original transaction (and model score if given).
5. Write analyst cases (HIGH + MEDIUM) and candidate retraining labels.

Usage:
  spark-submit complaint_analysis.py --complaints afrimarket_complaints.csv \
      [--transactions train_transaction.csv] [--out complaint_results]
"""
import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover

ap = argparse.ArgumentParser()
ap.add_argument("--complaints", required=True)
ap.add_argument("--transactions", help="IEEE-CIS transactions CSV, to link complaints back")
ap.add_argument("--out", default="complaint_results")
args = ap.parse_args()

spark = SparkSession.builder.appName("afrimarket-complaints").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Load ----------------------------------------------------------------------
c = (spark.read.csv(args.complaints, header=True, inferSchema=True, escape='"')
     .withColumn("ComplaintDate", F.to_timestamp("ComplaintDate"))
     .withColumn("text", F.lower(F.regexp_replace("ComplaintText", r"[^A-Za-z' ]", " "))))

# 2. Keyword rules (checked in order; first match wins) ------------------------
RULES = [
    ("UNAUTHORIZED_CHARGE", "HIGH",
     r"(not authori[sz]e|never placed|do not recognize|don't recognize|unauthori[sz]ed|stolen card)"),
    ("SUSPICIOUS_SELLER", "HIGH",
     r"(suspicious|mislead|scam|fake seller|stopped responding|fraud)"),
    ("NON_DELIVERY", "MEDIUM",
     r"(never received|not arrived|nothing was delivered|still has not arrived|shows delivery|says delivered)"),
]
fraud_type = F.lit("SERVICE")
fraud_signal = F.lit("NONE")
matched_kw = F.lit(None).cast("string")
for name, level, pattern in reversed(RULES):
    hit = F.col("text").rlike(pattern)
    fraud_type = F.when(hit, name).otherwise(fraud_type)
    fraud_signal = F.when(hit, level).otherwise(fraud_signal)
    matched_kw = F.when(hit, F.regexp_extract("text", pattern, 1)).otherwise(matched_kw)
c = c.withColumn("fraud_type", fraud_type).withColumn("fraud_signal", fraud_signal) \
     .withColumn("matched_keyword", matched_kw)

# Top words per category (Spark ML tokenizer + stop words), useful for the report.
words = StopWordsRemover(inputCol="tokens", outputCol="words").transform(
    Tokenizer(inputCol="text", outputCol="tokens").transform(c))
top_words = (words.select("ComplaintCategory", F.explode("words").alias("word"))
             .filter(F.length("word") > 2)
             .groupBy("ComplaintCategory", "word").count())

# 3. How well do the rules match the labelled categories? ----------------------
expected = (F.when(F.col("ComplaintCategory") == "Unauthorized Charge", "UNAUTHORIZED_CHARGE")
             .when(F.col("ComplaintCategory") == "Suspicious Seller", "SUSPICIOUS_SELLER")
             .when(F.col("ComplaintCategory") == "Missing Package", "NON_DELIVERY")
             .otherwise("SERVICE"))
c = c.withColumn("expected_type", expected).cache()
total = c.count()
agree = c.filter(F.col("fraud_type") == F.col("expected_type")).count()

# 4. Link to the original transaction -------------------------------------------
cases = c.filter(F.col("fraud_signal") != "NONE")
linked_note = "no transactions file given"
if args.transactions:
    tx = (spark.read.csv(args.transactions, header=True, inferSchema=True)
          .select("TransactionID", "TransactionAmt", "isFraud", "card1", "addr1", "ProductCD")
          .dropDuplicates(["TransactionID"]))
    c = c.join(tx, "TransactionID", "left").cache()
    cases = c.filter(F.col("fraud_signal") != "NONE")
    found = c.filter(F.col("TransactionAmt").isNotNull()).count()
    linked_note = f"{found} of {total} complaints matched a transaction"

# 5. Outputs ------------------------------------------------------------------
case_cols = ["ComplaintID", "TransactionID", "ComplaintDate", "fraud_signal", "fraud_type",
             "matched_keyword", "ComplaintStatus", "ComplaintText"]
case_cols += [x for x in ["TransactionAmt", "isFraud"] if x in c.columns]
cases = (cases.select(*case_cols)
         .withColumn("priority", F.when(F.col("fraud_signal") == "HIGH", 1).otherwise(2))
         .orderBy("priority", "ComplaintDate"))
cases.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.out}/analyst_cases")

# HIGH-signal complaints become candidate labels; an analyst confirms before retraining.
(c.filter(F.col("fraud_signal") == "HIGH")
  .select("TransactionID", F.lit(1).alias("complaint_fraud_flag"), "fraud_type", "ComplaintDate")
  .coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.out}/candidate_labels"))

print("\n================ Complaint analysis ================")
print(f"Complaints: {total} | {linked_note}")
print(f"Rule tags agree with labelled category: {agree}/{total} ({agree / total:.1%})")
print("\nRule tag vs labelled category:")
c.groupBy("ComplaintCategory").pivot("fraud_type").count().na.fill(0) \
    .orderBy("ComplaintCategory").show(truncate=False)
print("Fraud signal by complaint status:")
c.groupBy("fraud_signal").pivot("ComplaintStatus").count().na.fill(0).show()
print("Complaints per month:")
c.groupBy(F.date_format("ComplaintDate", "yyyy-MM").alias("month"), "fraud_signal").count() \
    .groupBy("month").pivot("fraud_signal").sum("count").na.fill(0).orderBy("month").show()
if args.transactions:
    print("Do fraud-tagged complaints point at transactions labelled fraud (isFraud)?")
    c.filter(F.col("isFraud").isNotNull()).groupBy("fraud_signal") \
        .agg(F.count("*").alias("complaints"), F.sum("isFraud").alias("isFraud_1"),
             F.round(F.avg("isFraud"), 3).alias("fraud_rate"),
             F.round(F.sum("TransactionAmt"), 2).alias("amount_$")).show()
print("Top words per category:")
from pyspark.sql import Window
top_words.withColumn("r", F.row_number().over(
    Window.partitionBy("ComplaintCategory").orderBy(F.desc("count")))) \
    .filter("r <= 5").groupBy("ComplaintCategory") \
    .agg(F.concat_ws(", ", F.collect_list("word")).alias("top_words")).show(truncate=False)
print(f"Wrote {args.out}/analyst_cases and {args.out}/candidate_labels")
spark.stop()
