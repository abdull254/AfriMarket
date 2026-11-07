"""
AfriMarket fraud model (CIS641 Group 6), upgraded version.

  * Time-based split: train on earlier transactions, test on later ones.
  * Leak-free customer features (see features.py).
  * Compares logistic regression vs gradient boosted trees, keeps the best.
  * Picks REVIEW / BLOCK thresholds by dollars saved, not by default 0.5.

Usage:
  spark-submit --py-files features.py train_fraud_model.py \
      --transactions train_transaction.csv [--identity train_identity.csv] \
      --model-dir fraud_model [--results-dir results] [--fp-cost 15]
"""
import argparse
import json
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

from features import LABEL, TIME, NUMERIC, CATEGORICAL, clean, add_uid, add_features

ap = argparse.ArgumentParser()
ap.add_argument("--transactions", required=True)
ap.add_argument("--identity", help="optional train_identity.csv (adds DeviceType)")
ap.add_argument("--model-dir", default="fraud_model")
ap.add_argument("--results-dir", default="results")
ap.add_argument("--fp-cost", type=float, default=15.0,
                help="assumed $ cost of flagging a legit order (review time + lost goodwill)")
ap.add_argument("--test-fraction", type=float, default=0.2)
args = ap.parse_args()
os.makedirs(args.results_dir, exist_ok=True)

spark = SparkSession.builder.appName("afrimarket-fraud-train").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Load + features ---------------------------------------------------------
df = spark.read.csv(args.transactions, header=True, inferSchema=True)
if args.identity:
    ident = spark.read.csv(args.identity, header=True, inferSchema=True)
    df = df.join(ident.select("TransactionID", "DeviceType"), "TransactionID", "left")
df = add_features(add_uid(clean(df)))
df = df.withColumn(LABEL, F.col(LABEL).cast("double"))
df = df.select("TransactionID", TIME, LABEL, *NUMERIC, *CATEGORICAL).cache()

# 2. Time-based split --------------------------------------------------------
cut = df.approxQuantile(TIME, [1 - args.test_fraction], 0.0001)[0]
train = df.filter(F.col(TIME) <= cut)
test = df.filter(F.col(TIME) > cut).cache()

fraud_rate = train.filter(F.col(LABEL) == 1).count() / train.count()
train = train.withColumn(
    "weight", F.when(F.col(LABEL) == 1, 1 - fraud_rate).otherwise(fraud_rate)).cache()
test_days = (test.agg(F.max(TIME)).first()[0] - cut) / 86400

# 3. Two candidate models ----------------------------------------------------
indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
            for c in CATEGORICAL]

lr_pipe = Pipeline(stages=indexers + [
    OneHotEncoder(inputCols=[c + "_idx" for c in CATEGORICAL],
                  outputCols=[c + "_ohe" for c in CATEGORICAL]),
    VectorAssembler(inputCols=NUMERIC + [c + "_ohe" for c in CATEGORICAL],
                    outputCol="raw_features"),
    StandardScaler(inputCol="raw_features", outputCol="features"),
    LogisticRegression(labelCol=LABEL, weightCol="weight", maxIter=100, regParam=0.01),
])

gbt_pipe = Pipeline(stages=indexers + [
    VectorAssembler(inputCols=NUMERIC + [c + "_idx" for c in CATEGORICAL],
                    outputCol="features"),
    GBTClassifier(labelCol=LABEL, weightCol="weight", maxIter=60, maxDepth=5,
                  maxBins=128, stepSize=0.1, seed=42),
])

pr_eval = BinaryClassificationEvaluator(labelCol=LABEL, metricName="areaUnderPR")
roc_eval = BinaryClassificationEvaluator(labelCol=LABEL, metricName="areaUnderROC")


def evaluate(name, model):
    pred = model.transform(test).withColumn("risk_score", vector_to_array("probability")[1])
    flagged = F.col("risk_score") >= 0.5
    row = pred.agg(
        F.sum((flagged & (F.col(LABEL) == 1)).cast("int")).alias("tp"),
        F.sum((flagged & (F.col(LABEL) == 0)).cast("int")).alias("fp"),
        F.sum((~flagged & (F.col(LABEL) == 1)).cast("int")).alias("fn")).first()
    tp, fp, fn = row.tp or 0, row.fp or 0, row.fn or 0
    return pred, {
        "model": name,
        "auc_pr": round(pr_eval.evaluate(pred), 4),
        "auc_roc": round(roc_eval.evaluate(pred), 4),
        "precision@0.5": round(tp / (tp + fp), 4) if tp + fp else 0.0,
        "recall@0.5": round(tp / (tp + fn), 4) if tp + fn else 0.0,
        "false_positives@0.5": fp,
    }


results, fitted = [], {}
for name, pipe in [("LogisticRegression", lr_pipe), ("GradientBoostedTrees", gbt_pipe)]:
    print(f"Training {name} ...")
    m = pipe.fit(train)
    pred, metrics = evaluate(name, m)
    results.append(metrics)
    fitted[name] = (m, pred)

best = max(results, key=lambda r: r["auc_pr"])["model"]
best_model, best_pred = fitted[best]

# 4. Thresholds by dollars ---------------------------------------------------
# For each cut-off: fraud $ caught, legit orders wrongly flagged, net savings.
grid = [round(0.05 * i, 2) for i in range(1, 20)]
aggs = []
for t in grid:
    hit = F.col("risk_score") >= t
    is_fraud = F.col(LABEL) == 1
    aggs += [F.sum(F.when(hit & is_fraud, F.col("TransactionAmt")).otherwise(0)).alias(f"c{t}"),
             F.sum((hit & ~is_fraud).cast("int")).alias(f"f{t}"),
             F.sum((hit & is_fraud).cast("int")).alias(f"t{t}")]
row = best_pred.agg(F.sum(F.when(F.col(LABEL) == 1, F.col("TransactionAmt"))).alias("total"),
                    *aggs).first().asDict()
total_fraud = row["total"] or 0.0

sweep = []
for t in grid:
    caught, fp, tp = row[f"c{t}"] or 0.0, row[f"f{t}"] or 0, row[f"t{t}"] or 0
    sweep.append({"threshold": t, "fraud_dollars_caught": round(caught, 2),
                  "pct_fraud_dollars_caught": round(caught / total_fraud, 4) if total_fraud else 0,
                  "false_positives": fp,
                  "precision": round(tp / (tp + fp), 4) if tp + fp else 0.0,
                  "net_savings": round(caught - fp * args.fp_cost, 2)})

review_at = max(sweep, key=lambda s: s["net_savings"])["threshold"]
# BLOCK only when we're quite sure (precision >= 80%); everything between goes to analysts.
sure = [s["threshold"] for s in sweep if s["precision"] >= 0.8 and s["threshold"] > review_at]
block_at = min(sure) if sure else max(review_at, 0.9)
best_row = next(s for s in sweep if s["threshold"] == review_at)
monthly = best_row["net_savings"] / test_days * 30 if test_days > 0 else float("nan")

# 5. Explain: which features matter most --------------------------------------
attrs = best_pred.schema["raw_features" if best == "LogisticRegression" else "features"] \
    .metadata["ml_attr"]["attrs"]
names = {a["idx"]: a["name"] for group in attrs.values() for a in group}
last = best_model.stages[-1]
weights = (last.coefficients.toArray() if best == "LogisticRegression"
           else last.featureImportances.toArray())
top = sorted(((names.get(i, str(i)), float(w)) for i, w in enumerate(weights)),
             key=lambda x: -abs(x[1]))[:10]

# 6. Report + save -----------------------------------------------------------
print("\n================ Results (time-based test set) ================")
print(f"Train rows {train.count():,} | Test rows {test.count():,} "
      f"| Test period {test_days:.1f} days | Train fraud rate {fraud_rate:.2%}")
print(f"{'model':<22}{'AUC-PR':>8}{'AUC-ROC':>9}{'Prec@.5':>9}{'Rec@.5':>8}{'FP@.5':>8}")
for r in results:
    print(f"{r['model']:<22}{r['auc_pr']:>8}{r['auc_roc']:>9}{r['precision@0.5']:>9}"
          f"{r['recall@0.5']:>8}{r['false_positives@0.5']:>8}")
print(f"\nBest model: {best}")
print(f"REVIEW at risk >= {review_at}, BLOCK at risk >= {block_at} "
      f"(false positive cost assumed ${args.fp_cost:.0f})")
print(f"At REVIEW threshold: catches {best_row['pct_fraud_dollars_caught']:.1%} of fraud dollars, "
      f"{best_row['false_positives']} false positives, net savings ${best_row['net_savings']:,.0f} "
      f"in test period (about ${monthly:,.0f} per 30 days at this data volume)")
print("Top features:")
for n, w in top:
    print(f"  {n:<28}{w:+.4f}")

with open(os.path.join(args.results_dir, "model_comparison.json"), "w") as f:
    json.dump(results, f, indent=2)
with open(os.path.join(args.results_dir, "threshold_sweep.json"), "w") as f:
    json.dump(sweep, f, indent=2)
summary = {"best_model": best, "review_at": review_at, "block_at": block_at,
           "fp_cost": args.fp_cost, "net_savings_test_period": best_row["net_savings"],
           "net_savings_per_30_days": round(monthly, 2), "test_days": round(test_days, 2),
           "top_features": top}
with open(os.path.join(args.results_dir, "summary.json"), "w") as f:
    json.dump(summary, f, indent=2)

best_model.write().overwrite().save(args.model_dir)
spark.createDataFrame([(review_at, block_at)], ["review_at", "block_at"]) \
    .coalesce(1).write.mode("overwrite").json(args.model_dir.rstrip("/") + "_thresholds")
print(f"\nSaved model to {args.model_dir}, thresholds to {args.model_dir.rstrip('/')}_thresholds, "
      f"reports to {args.results_dir}/")
spark.stop()
