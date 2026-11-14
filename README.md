# AfriMarket fraud model (CIS641 Group 6)

A Spark MLlib model that gives every transaction a 0-1 fraud risk score and an
APPROVE / REVIEW / BLOCK decision, built on the IEEE-CIS fraud dataset.

This branch is the **ML part** of the project. The rest of the platform lives on
the other branches: Kafka ingestion (`Marienne`), MySQL/Cassandra + dashboard
(`Bintou`), and the integrated streaming platform (`Muhammed`).

## Files
| File | What it is |
|---|---|
| `train_fraud_model.py` | Training job: split, compare models, pick thresholds, save model |
| `features.py` | Feature code shared by training and live scoring |
| `score_snippet.py` | `handle_batch()` for the Structured Streaming job |
| `complaint_analysis.py` | Complaint keyword tagging + link to transactions (delayed path) |
| `tests/test_features.py` | Proves the customer features never look at future rows |
| `data/` | `afrimarket_transactions.csv` (30K rows) and `afrimarket_complaints.csv` (1,200 rows) |
| `results/` | Outputs from one run of the code in this branch |

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
   amount. Each one uses only earlier transactions (`tests/` checks this).
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
Train 20,997 rows | validation 4,500 rows (30.9 days) | test 4,503 rows (31.4 days) |
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
| $5 | 0.10 | 70.5% | 2,467 | $5,119 |
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
```

Outputs:
- `results/model_comparison.json`: validation and test tables
- `results/threshold_sweep.json`: dollars caught vs false positives at each cut-off, for validation and test (good chart)
- `results/summary.json`: chosen model and thresholds, test results, cost check, top features
- `results/complaint_analyst_cases.csv`: HIGH and MEDIUM complaints for analysts
- `<model-dir>` and `<model-dir>_thresholds`: loaded by the streaming job

## Live scoring
`score_snippet.py`: each micro-batch from Kafka is joined with those cards' past
transactions from the HDFS data lake, so the "last hour" features match training.
Transactions already in the lake are skipped so they are not counted twice. It then
writes TransactionID, risk_score, decision (to MySQL in our architecture).

## Known limits
- `hour_of_day` comes from `TransactionDT`, which counts seconds from an unknown start
  point. It's a 24-hour cycle shifted by an unknown amount, not real clock time.
- Live scoring rereads card history from HDFS every batch. That's fine for the demo, but
  at scale, per-card running totals should be kept in Cassandra.
- The 36 MB transactions file is stored directly in git. For the full 590K rows, use
  Git LFS or a download script instead.
