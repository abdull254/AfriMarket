# AfriMarket fraud model (CIS641 Group 6)

A Spark MLlib model that gives every transaction a 0-1 fraud risk score and an
APPROVE / REVIEW / BLOCK decision, built on the IEEE-CIS fraud dataset.

## Files
| File | What it is |
|---|---|
| `train_fraud_model.py` | Main training job (use this one) |
| `features.py` | Feature code shared by training and live scoring |
| `score_snippet.py` | `handle_batch()` for the Structured Streaming job |
| `train_fraud_model_mvp.py` | Original one-model MVP, kept as the baseline |

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

## Run
```
pip install pyspark==3.5.3        # or run inside the Spark container
spark-submit --driver-memory 4g --py-files features.py train_fraud_model.py \
    --transactions train_transaction.csv \
    --identity train_identity.csv \
    --model-dir /fraud-data/model --results-dir results
```
Use the original Kaggle files (`train_transaction.csv`, optional `train_identity.csv`).
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
