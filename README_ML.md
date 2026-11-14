# AfriMarket fraud model: details (CIS641 Group 6)

A Spark MLlib model that gives every transaction a 0-1 fraud risk score and an
APPROVE / REVIEW / BLOCK decision, built on the IEEE-CIS fraud dataset.

| Score | Decision | What happens |
|---|---|---|
| Below 0.55 | APPROVE | Order ships as normal |
| 0.55 to 0.9 | REVIEW | A fraud analyst checks it first |
| 0.9 and above | BLOCK | Order is declined (sent to REVIEW instead if live features were unavailable) |

This file covers the ML part of the project. For the full platform (Kafka, Spark
streaming, MySQL, Cassandra, dashboard), see `README.md`.

## Files
| File | What it is |
|---|---|
| `train_fraud_model.py` | Training job: split, compare models, pick thresholds, save model |
| `features.py` | Feature code shared by training and live scoring |
| `score_snippet.py` | `handle_batch()` for the Structured Streaming job (features from Redis) |
| `online_features.py` | Live customer features from per-card running totals in Redis, duplicate-safe |
| `parity_test.py` | Proves live features equal training features on every purchase |
| `complaint_analysis.py` | Complaint keyword tagging + link to transactions (delayed path) |
| `tests/test_features.py` | Proves the customer features never look at future rows |
| `data/` | `afrimarket_transactions.csv` (30K rows) and `afrimarket_complaints.csv` (1,200 rows) |
| `results/` | Outputs from one run of the training code |

The trained model is not stored in git. Running the training job creates it
(`fraud_model/` and `fraud_model_thresholds/`).

## How it works
1. **Three-way time split.** Transactions are sorted by `TransactionDT`:
   oldest 70% for training, next 15% for validation, newest 15% for test.
   The model and the thresholds are chosen on validation. The test set is only
   used at the end to report results, so nothing is tuned on the numbers we report.
2. **Customer features, with no leakage.** A rough customer ID (card1 + addr1), then
   transactions in the last hour and 24 hours, seconds since that card's last purchase,
   amount compared with that card's past average, hour of day, and the cents part of the
   amount. Each one counts only the same customer's purchases with a strictly earlier
   timestamp, so a purchase never counts itself or another purchase in the same second
   (`tests/` checks this).
3. **Model comparison.** Logistic regression (baseline), gradient boosted trees, and
   boosted trees without the raw card/address ID columns. All are weighted for the rare
   fraud class and judged on AUC-PR, since accuracy is meaningless at ~3.5% fraud.
4. **Thresholds picked by dollars.** Tries cut-offs from 0.05 to 0.95 on validation.
   For each one it adds up fraud dollars caught, minus a cost for every legit order
   flagged (`--fp-cost`, default $15). REVIEW = the cut-off with the highest net savings.
   BLOCK = the lowest higher cut-off where at least 80% of flagged orders are fraud
   (falls back to 0.9 if none reaches 80%).
5. **Cost check.** Repeats the threshold choice at $5, $15 and $30 per false alarm,
   because the $15 figure is an assumption.
6. **Explainability.** Prints the top 10 features driving the model.

## Results (from `results/`, one run)
Train 20,997 rows | validation 4,501 rows (30.9 days) | test 4,502 rows (31.4 days) |
fraud rate 3.4%, so a random guess scores AUC-PR about 0.035.

**Test set** (never used for choosing anything):

| Model | AUC-PR | AUC-ROC | Precision @0.5 | Recall @0.5 | False positives @0.5 |
|---|---|---|---|---|---|
| Logistic regression | 0.105 | 0.793 | 0.089 | 0.676 | 985 |
| Gradient boosted trees (picked) | 0.346 | 0.798 | 0.169 | 0.599 | 417 |
| Boosted trees, no ID columns | 0.399 | 0.808 | 0.164 | 0.585 | 422 |

- Boosted trees won on **validation** (AUC-PR 0.396 vs 0.386 without IDs), so they were picked.
  On test, the no-ID version did better (0.399 vs 0.346). That suggests the raw card numbers
  partly memorise customers and don't carry over to new periods. We kept the rule
  "pick on validation" rather than switch after seeing test, but the no-ID model is the safer
  choice for the next version.
- Thresholds chosen on validation: **REVIEW at 0.55, BLOCK at 0.9**.
- On test at REVIEW: catches **33% of fraud dollars**, 360 false positives, net savings
  **about $2,760** in 31 days.
- On test at BLOCK: 73% of blocked orders are fraud, 13 false positives.
- Validation showed $12,400 savings at the same cut-off. The drop on test is real: fraud
  patterns shift over time, and this is why the old version (tuned on test, about $10K)
  looked better than it was.

Cost check on test:

| Cost per false alarm | REVIEW at | Fraud $ caught | False positives | Net savings |
|---|---|---|---|---|
| $5 | 0.10 | 70.5% | 2,466 | $5,124 |
| $15 | 0.55 | 33.0% | 360 | $2,761 |
| $30 | 0.55 | 33.0% | 360 | -$2,639 |

So the business case depends on how cheap a manual review is. If a review costs
around $30, the model only pays off at a stricter cut-off than validation suggested.

Why the scores are modest: 30K rows randomly sampled from 590K means few repeat
customers, so the "last hour" features rarely fire. The full dataset should do better.

**Complaints:** 1,200 of 1,200 link to a transaction. Complaints the rules tag HIGH
(unauthorized charge, suspicious seller) point at actual fraud 19.2% of the time, vs
1.4% for ordinary service complaints and 3.5% overall. The rules agree 100% with the
labelled categories, but only because the complaint text is 18 repeated template sentences.

## Run
```
pip install -r requirements.txt      # or run inside the Spark container
spark-submit --driver-memory 4g --py-files features.py train_fraud_model.py \
    --transactions data/afrimarket_transactions.csv \
    --model-dir /fraud-data/model --results-dir results
```
Our transactions file already includes identity columns, so `--identity` is not needed.
For the full Kaggle data pass `--transactions train_transaction.csv --identity train_identity.csv`.
Other options: `--fp-cost 15`, `--fp-cost-grid 5,15,30`, `--val-fraction 0.15`,
`--test-fraction 0.15`, `--block-precision 0.8`.

On the 30K file this took about 20 minutes on 4 CPU cores, most of it the two boosted-tree models.

Complaints:
```
spark-submit complaint_analysis.py --complaints data/afrimarket_complaints.csv \
    --transactions data/afrimarket_transactions.csv --out complaint_results
```

Tests:
```
pytest tests/
spark-submit --py-files features.py,online_features.py parity_test.py data/afrimarket_transactions.csv
spark-submit --py-files features.py,online_features.py parity_test.py data/afrimarket_transactions.csv --redis
```
The last one tests the Redis version against a fake in-memory Redis (`fakeredis`).

Outputs:
- `results/model_comparison.json`: validation and test tables
- `results/threshold_sweep.json`: dollars caught vs false positives at each cut-off, for validation and test (good chart)
- `results/summary.json`: chosen model and thresholds, test results, cost check, top features
- `results/complaint_analyst_cases.csv`: HIGH and MEDIUM complaints for analysts
- `<model-dir>` and `<model-dir>_thresholds`: loaded by the streaming job

## Live scoring
Live scoring never reads HDFS. A small Redis container keeps a few running totals
per card (`online_features.py`): purchase count, amount count and total, last purchase
time, and that card's purchase times from the last 24 hours. So the work per purchase
stays the same however long the history gets. HDFS is only used for nightly retraining.

For each micro-batch, `score_snippet.py`:
1. Reads each card's totals from Redis, **then** adds the new purchase, so a purchase
   never counts itself. Purchases in the same second are all read before any is added.
2. Caches each purchase's features for 48 hours. A re-sent message (producer retry,
   job restart) gets the same features and is not added twice.
3. Adds hour of day and cents with the same code as training (`features.py`), scores,
   and decides. Writes TransactionID, risk_score, decision, features_degraded, scored_at
   (to MySQL in our architecture, as an upsert on TransactionID).

If Redis is down, the purchase is scored as a first-time customer and flagged
`features_degraded`, and it is never auto-blocked: BLOCK becomes REVIEW.

Checks run on this code:
- `parity_test.py` replayed all 30,000 purchases: **0 mismatches** against the training
  features, for both the memory and the Redis versions, and 0 changes on a duplicate replay.
- A 400-purchase batch through `score_batch()` with fake Redis, then the same batch again:
  identical scores. With Redis switched off: all 400 still scored, all flagged, 0 blocked.

Settings: `MODEL_DIR`, `REDIS_HOST` (default `redis`), `REDIS_PORT` (default 6379).
Seed Redis before going live by replaying the history once through
`compute_online_features()`, so returning customers aren't treated as new.

**Needed from the producer:** use the card number (`card1`) as the Kafka message key, so
each card's purchases land in one partition and reach Spark in time order. The Redis
version assumes this.

## Known limits
- `hour_of_day` comes from `TransactionDT`, which counts seconds from an unknown start
  point. It's a 24-hour cycle shifted by an unknown amount, not real clock time.
- Scoring latency with Redis has not been measured yet. Measure `scored_at - sent_at`
  at the producer's test rates.
- The Kafka read and the MySQL write in `score_snippet.py` are still placeholders. The
  integrated streaming job (`spark_processing/stream_transactions.py`) needs to switch to these features.
- The 36 MB transactions file is stored directly in git. For the full 590K rows, use
  Git LFS or a download script instead.
