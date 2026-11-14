"""
AfriMarket fraud model (CIS641 Group 6).

  * Three-way time split: train on the oldest transactions, tune on the next
    slice (validation), and report on the newest slice (test). The test set is
    never used to choose anything, so the reported numbers are honest.
  * Leak-free customer features (see features.py).
  * Compares logistic regression, gradient boosted trees, and boosted trees
    without the raw card/address ID columns. Picks the best on validation AUC-PR.
  * Picks REVIEW / BLOCK thresholds by dollars saved on validation, then checks
    them on test. Also shows how the result changes with the false-positive cost.

Usage:
  spark-submit --py-files features.py train_fraud_model.py \
      --transactions data/afrimarket_transactions.csv [--identity train_identity.csv] \
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

from features import LABEL, TIME, NUMERIC, CATEGORICAL, ID_COLUMNS, clean, add_uid, add_features

ap = argparse.ArgumentParser()
ap.add_argument("--transactions", required=True)
ap.add_argument("--identity", help="optional train_identity.csv (adds DeviceType)")
ap.add_argument("--model-dir", default="fraud_model")
ap.add_argument("--results-dir", default="results")
ap.add_argument("--fp-cost", type=float, default=15.0,
                help="assumed $ cost of flagging a legit order (review time + lost goodwill)")
ap.add_argument("--fp-cost-grid", default="5,15,30",
                help="other false-positive costs to test, comma separated")
ap.add_argument("--val-fraction", type=float, default=0.15)
ap.add_argument("--test-fraction", type=float, default=0.15)
ap.add_argument("--block-precision", type=float, default=0.8,
                help="BLOCK only at cut-offs where at least this share of flagged orders is fraud")
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

# 2. Time-based split: train | validation | test ------------------------------
cut_val, cut_test = df.approxQuantile(
    TIME, [1 - args.val_fraction - args.test_fraction, 1 - args.test_fraction], 0.0001)
train = df.filter(F.col(TIME) <= cut_val)
val = df.filter((F.col(TIME) > cut_val) & (F.col(TIME) <= cut_test)).cache()
test = df.filter(F.col(TIME) > cut_test).cache()

fraud_rate = train.filter(F.col(LABEL) == 1).count() / train.count()
train = train.withColumn(
    "weight", F.when(F.col(LABEL) == 1, 1 - fraud_rate).otherwise(fraud_rate)).cache()
val_days = (cut_test - cut_val) / 86400
test_days = (test.agg(F.max(TIME)).first()[0] - cut_test) / 86400

# 3. Candidate models --------------------------------------------------------
indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
            for c in CATEGORICAL]
NUMERIC_NO_IDS = [c for c in NUMERIC if c not in ID_COLUMNS]


def lr_pipeline(numeric):
    return Pipeline(stages=indexers + [
        OneHotEncoder(inputCols=[c + "_idx" for c in CATEGORICAL],
                      outputCols=[c + "_ohe" for c in CATEGORICAL]),
        VectorAssembler(inputCols=numeric + [c + "_ohe" for c in CATEGORICAL],
                        outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features"),
        LogisticRegression(labelCol=LABEL, weightCol="weight", maxIter=100, regParam=0.01),
    ])


def gbt_pipeline(numeric):
    return Pipeline(stages=indexers + [
        VectorAssembler(inputCols=numeric + [c + "_idx" for c in CATEGORICAL],
                        outputCol="features"),
        GBTClassifier(labelCol=LABEL, weightCol="weight", maxIter=60, maxDepth=5,
                      maxBins=128, stepSize=0.1, seed=42),
    ])


CANDIDATES = [
    ("LogisticRegression", lr_pipeline(NUMERIC)),
    ("GradientBoostedTrees", gbt_pipeline(NUMERIC)),
    # Raw card/address numbers are IDs, not amounts. Trees can memorise them.
    # This version keeps the per-customer features but drops the raw IDs.
    ("GradientBoostedTrees_noIDs", gbt_pipeline(NUMERIC_NO_IDS)),
]

pr_eval = BinaryClassificationEvaluator(labelCol=LABEL, metricName="areaUnderPR")
roc_eval = BinaryClassificationEvaluator(labelCol=LABEL, metricName="areaUnderROC")


def score(model, data):
    return model.transform(data).withColumn("risk_score", vector_to_array("probability")[1])


def metrics(name, pred):
    flagged = F.col("risk_score") >= 0.5
    row = pred.agg(
        F.sum((flagged & (F.col(LABEL) == 1)).cast("int")).alias("tp"),
        F.sum((flagged & (F.col(LABEL) == 0)).cast("int")).alias("fp"),
        F.sum((~flagged & (F.col(LABEL) == 1)).cast("int")).alias("fn")).first()
    tp, fp, fn = row.tp or 0, row.fp or 0, row.fn or 0
    return {
        "model": name,
        "auc_pr": round(pr_eval.evaluate(pred), 4),
        "auc_roc": round(roc_eval.evaluate(pred), 4),
        "precision@0.5": round(tp / (tp + fp), 4) if tp + fp else 0.0,
        "recall@0.5": round(tp / (tp + fn), 4) if tp + fn else 0.0,
        "false_positives@0.5": fp,
    }


GRID = [round(0.05 * i, 2) for i in range(1, 20)]


def sweep(pred, fp_cost):
    """Fraud $ caught, legit orders wrongly flagged, and net savings at each cut-off."""
    aggs = []
    for t in GRID:
        hit = F.col("risk_score") >= t
        is_fraud = F.col(LABEL) == 1
        aggs += [F.sum(F.when(hit & is_fraud, F.col("TransactionAmt")).otherwise(0)).alias(f"c{t}"),
                 F.sum((hit & ~is_fraud).cast("int")).alias(f"f{t}"),
                 F.sum((hit & is_fraud).cast("int")).alias(f"t{t}")]
    row = pred.agg(F.sum(F.when(F.col(LABEL) == 1, F.col("TransactionAmt"))).alias("total"),
                   *aggs).first().asDict()
    total = row["total"] or 0.0
    out = []
    for t in GRID:
        caught, fp, tp = row[f"c{t}"] or 0.0, row[f"f{t}"] or 0, row[f"t{t}"] or 0
        out.append({"threshold": t, "fraud_dollars_caught": round(caught, 2),
                    "pct_fraud_dollars_caught": round(caught / total, 4) if total else 0,
                    "false_positives": fp,
                    "precision": round(tp / (tp + fp), 4) if tp + fp else 0.0,
                    "net_savings": round(caught - fp * fp_cost, 2)})
    return out


def pick_thresholds(rows):
    review_at = max(rows, key=lambda s: s["net_savings"])["threshold"]
    sure = [s["threshold"] for s in rows
            if s["precision"] >= args.block_precision and s["threshold"] > review_at]
    block_at = min(sure) if sure else max(review_at, 0.9)
    return review_at, block_at, bool(sure)


def at(rows, t):
    return next(s for s in rows if s["threshold"] == t)


# 4. Fit on train, choose on validation --------------------------------------
val_results, test_results, fitted = [], [], {}
for name, pipe in CANDIDATES:
    print(f"Training {name} ...")
    m = pipe.fit(train)
    val_pred, test_pred = score(m, val).cache(), score(m, test).cache()
    val_results.append(metrics(name, val_pred))
    test_results.append(metrics(name, test_pred))
    fitted[name] = (m, val_pred, test_pred)

best = max(val_results, key=lambda r: r["auc_pr"])["model"]
best_model, best_val, best_test = fitted[best]

# 5. Thresholds: choose on validation, check on test -------------------------
val_sweep = sweep(best_val, args.fp_cost)
test_sweep = sweep(best_test, args.fp_cost)
review_at, block_at, block_reached = pick_thresholds(val_sweep)
test_row = at(test_sweep, review_at)
monthly = test_row["net_savings"] / test_days * 30 if test_days > 0 else float("nan")
test_block = at(test_sweep, block_at)

# How much does the answer depend on the $ cost we assumed for a false alarm?
sensitivity = []
for cost in sorted({float(c) for c in args.fp_cost_grid.split(",")} | {args.fp_cost}):
    r_at, b_at, _ = pick_thresholds(sweep(best_val, cost))
    t_row = at(sweep(best_test, cost), r_at)
    sensitivity.append({"fp_cost": cost, "review_at": r_at, "block_at": b_at,
                        "test_pct_fraud_dollars_caught": t_row["pct_fraud_dollars_caught"],
                        "test_false_positives": t_row["false_positives"],
                        "test_net_savings": t_row["net_savings"]})

# 6. Explain: which features matter most --------------------------------------
vec_col = "raw_features" if best == "LogisticRegression" else "features"
attrs = best_val.schema[vec_col].metadata["ml_attr"]["attrs"]
names = {a["idx"]: a["name"] for group in attrs.values() for a in group}
last = best_model.stages[-1]
weights = (last.coefficients.toArray() if best == "LogisticRegression"
           else last.featureImportances.toArray())
top = sorted(((names.get(i, str(i)), round(float(w), 4)) for i, w in enumerate(weights)),
             key=lambda x: -abs(x[1]))[:10]

# 7. Report + save -----------------------------------------------------------
n_train, n_val, n_test = train.count(), val.count(), test.count()
print("\n================ Results ================")
print(f"Train {n_train:,} | Validation {n_val:,} ({val_days:.1f} days) "
      f"| Test {n_test:,} ({test_days:.1f} days) | Train fraud rate {fraud_rate:.2%}")
for label, res in [("Validation (used to choose)", val_results), ("Test (final report)", test_results)]:
    print(f"\n{label}")
    print(f"{'model':<28}{'AUC-PR':>8}{'AUC-ROC':>9}{'Prec@.5':>9}{'Rec@.5':>8}{'FP@.5':>8}")
    for r in res:
        print(f"{r['model']:<28}{r['auc_pr']:>8}{r['auc_roc']:>9}{r['precision@0.5']:>9}"
              f"{r['recall@0.5']:>8}{r['false_positives@0.5']:>8}")
print(f"\nBest model (by validation AUC-PR): {best}")
print(f"REVIEW at risk >= {review_at}, BLOCK at risk >= {block_at} "
      f"(chosen on validation, false positive cost ${args.fp_cost:.0f})")
print(f"On test at REVIEW: catches {test_row['pct_fraud_dollars_caught']:.1%} of fraud dollars, "
      f"{test_row['false_positives']} false positives, net savings ${test_row['net_savings']:,.0f} "
      f"(about ${monthly:,.0f} per 30 days at this data volume)")
print(f"On test at BLOCK: precision {test_block['precision']:.0%}, "
      f"{test_block['false_positives']} false positives")
print("False-positive cost sensitivity (test):")
for s in sensitivity:
    print(f"  ${s['fp_cost']:>4.0f}: REVIEW {s['review_at']}  caught {s['test_pct_fraud_dollars_caught']:.1%}"
          f"  FP {s['test_false_positives']}  net ${s['test_net_savings']:,.0f}")
print("Top features:")
for n, w in top:
    print(f"  {n:<28}{w:+.4f}")


def dump(name, obj):
    with open(os.path.join(args.results_dir, name), "w") as f:
        json.dump(obj, f, indent=2)


dump("model_comparison.json", {"validation": val_results, "test": test_results})
dump("threshold_sweep.json", {"fp_cost": args.fp_cost, "validation": val_sweep, "test": test_sweep})
dump("summary.json", {
    "best_model": best, "chosen_on": "validation",
    "review_at": review_at, "block_at": block_at,
    "block_reached_target_precision": block_reached,
    "fp_cost": args.fp_cost,
    "rows": {"train": n_train, "validation": n_val, "test": n_test},
    "days": {"validation": round(val_days, 2), "test": round(test_days, 2)},
    "train_fraud_rate": round(fraud_rate, 4),
    "test_at_review": test_row, "test_at_block": test_block,
    "test_net_savings_per_30_days": round(monthly, 2),
    "fp_cost_sensitivity": sensitivity,
    "top_features": top,
})

best_model.write().overwrite().save(args.model_dir)
spark.createDataFrame([(review_at, block_at)], ["review_at", "block_at"]) \
    .coalesce(1).write.mode("overwrite").json(args.model_dir.rstrip("/") + "_thresholds")
print(f"\nSaved model to {args.model_dir}, thresholds to {args.model_dir.rstrip('/')}_thresholds, "
      f"reports to {args.results_dir}/")
spark.stop()
