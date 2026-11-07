# AfriMarket fraud model (CIS641 Group 6)

A Spark MLlib model that gives every transaction a 0-1 fraud risk score and an
APPROVE / REVIEW / BLOCK decision, built on the IEEE-CIS fraud dataset.

## Files
| File | What it is |
|---|---|
| `train_fraud_model.py` | Main training job (use this one) |
| `features.py` | Feature code shared by training and live scoring |
| `score_snippet.py` | `handle_batch()` for the Structured Streaming job |
| `complaint_analysis.py` | Complaint keyword tagging + link to transactions (delayed path) |
| `train_fraud_model_mvp.py` | Original one-model MVP, kept as the baseline |
| `results/` | Outputs from the real run on `afrimarket_transactions.csv` |
| `fraud_model/` | Trained model from that run |

## What makes it solid
1. **Time-based split.** Trains on the earliest 80% of transactions (by TransactionDT) and
   tests on the latest 20%, the way it would run in production. A random split lets the
   model peek at the future.
2. **Customer features, with no leakage.** A rough customer ID (card1 + addr1), then
   transactions in the last hour and 24 hours, seconds since that card's last purchase,
   amount compared with that card's past average, hour of day, and the cents part of the
   amount. Each one uses only earlier transactions.
3. **Model comparison.** Logistic regression (baseline) vs gradient boosted trees, both
   weighted for the rare fraud class. Judged on AUC-PR, since accuracy is meaningless
   at ~3.5% fraud. The best model is saved.
4. **Thresholds picked by dollars.** Tries cut-offs from 0.05 to 0.95. For each one it
   adds up fraud dollars caught, minus a cost for every legit order flagged
   (`--fp-cost`, default $15). REVIEW = the cut-off with the highest net savings.
   BLOCK = the lowest higher cut-off where at least 80% of flagged orders are fraud.
5. **Explainability.** Prints the top 10 features driving the model.

## Results on our data (afrimarket_transactions.csv, 30K rows, identity columns already merged)
Time-based test set: 6,003 transactions over the last 42.6 days. Fraud rate 3.5%, so a random guess scores AUC-PR 0.035.

| Model | AUC-PR | AUC-ROC | Precision @0.5 | Recall @0.5 | False positives @0.5 |
|---|---|---|---|---|---|
| Logistic regression | 0.121 | 0.792 | 0.098 | 0.678 | 1,276 |
| Gradient boosted trees | **0.370** | **0.846** | 0.183 | 0.634 | 580 |

Boosted trees are about 10x better than random on AUC-PR. At REVIEW 0.5 they catch 52% of fraud
dollars for net savings of about $10K in the test period. BLOCK fell back to 0.9 because no
cut-off reached 80% precision (0.9 gives 60% precision, 34 false positives).
Why not higher: 30K rows randomly sampled from 590K means few repeat customers, so the
"last hour" features rarely fire. The full dataset should do noticeably better.

Complaints: 1,200 of 1,200 link to a transaction. Complaints the rules tag HIGH (unauthorized
charge, suspicious seller) point at actual fraud 19.2% of the time, vs 1.4% for ordinary
service complaints and 3.5% overall. The rules agree 100% with the labelled categories, but
only because the complaint text is 18 repeated template sentences.

## Run
```
pip install pyspark==3.5.3 numpy pandas      # or run inside the Spark container
spark-submit --driver-memory 4g --py-files features.py train_fraud_model.py \
    --transactions afrimarket_transactions.csv \
    --model-dir /fraud-data/model --results-dir results
```
Our `afrimarket_transactions.csv` already includes identity columns, so `--identity` is not needed.
For the full Kaggle data pass `--transactions train_transaction.csv --identity train_identity.csv`.

Complaints:
```
spark-submit complaint_analysis.py --complaints afrimarket_complaints.csv \
    --transactions afrimarket_transactions.csv --out complaint_results
```
The full 590K rows is the "big data" version; the 30K sample also works.
On a 100K-row test it took about 7 minutes on a small machine, most of it boosted trees.

Outputs:
- `results/model_comparison.json`: the comparison table for your slides or report
- `results/threshold_sweep.json`: dollars caught vs false positives at each cut-off (good chart)
- `results/summary.json`: best model, thresholds, projected net savings per 30 days, top features
- `<model-dir>` and `<model-dir>_thresholds`: loaded by the streaming job

## Live scoring
`score_snippet.py`: each micro-batch from Kafka is joined with those cards' past
transactions from the HDFS data lake, so the "last hour" features match training.
Then it writes TransactionID, risk_score, decision (to MySQL in our architecture).
Nightly retraining is cron running `train_fraud_model.py` on the data lake.

## Ideas for later
- Measure scoring latency and throughput against the 50-250 tx/sec target.
- Tune GBT depth/iterations with `CrossValidator`.
