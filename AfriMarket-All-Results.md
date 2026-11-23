# AfriMarket — All training results

November 21, 2026 · Ubuntu on Windows WSL 2 · Spark 3.5.3

Three successful runs are recorded below. **A** is the initial WSL run (branch unconfirmed). **B** is the separate main worktree run. **C** is the latest rerun from the original AfriMarket directory (branch and commit unconfirmed).

| Run | Validation / test requested | Train rows | Validation rows (days) | Test rows (days) | Train fraud rate |
|---|---|---:|---:|---:|---:|
| A | 15% / 15% | 20,997 | 4,501 (30.9) | 4,502 (31.37) | 3.43% |
| B | 20% / 15% | 19,499 | 5,998 (40.6) | 4,503 (31.4) | 3.36% |
| C | 20% / 15% | 19,499 | 5,999 (40.6) | 4,502 (31.4) | 3.36% |

## Validation results

Precision, recall, and false positives (FP) in these tables use a common 0.50 threshold. Model selection uses validation AUC-PR.

| Run | Model | AUC-PR | AUC-ROC | Precision | Recall | FP |
|---|---|---:|---:|---:|---:|---:|
| A | Logistic Regression | 0.2292 | 0.7929 | 0.1169 | 0.6203 | 876 |
| A | GBT | 0.3957 | 0.8539 | 0.2415 | 0.5668 | 333 |
| A | GBT without IDs | 0.3860 | 0.8516 | 0.2459 | 0.5668 | 325 |
| B | Logistic Regression | 0.1894 | 0.7828 | 0.1132 | 0.6561 | 1,301 |
| B | GBT | 0.3846 | 0.8254 | 0.2336 | 0.5613 | 466 |
| B | GBT without IDs | 0.3933 | 0.8128 | 0.2204 | 0.5455 | 488 |
| C | Logistic Regression | 0.1894 | 0.7828 | 0.1132 | 0.6561 | 1,301 |
| C | GBT | 0.3846 | 0.8254 | 0.2336 | 0.5613 | 466 |
| C | GBT without IDs | 0.3932 | 0.8128 | 0.2204 | 0.5455 | 488 |

## Test results

| Run | Model | AUC-PR | AUC-ROC | Precision | Recall | FP |
|---|---|---:|---:|---:|---:|---:|
| A | Logistic Regression | 0.1054 | 0.7928 | 0.0888 | 0.6761 | 985 |
| A | GBT | 0.3458 | 0.7981 | 0.1693 | 0.5986 | 417 |
| A | GBT without IDs | 0.3985 | 0.8076 | 0.1644 | 0.5845 | 422 |
| B | Logistic Regression | 0.1038 | 0.7888 | 0.0841 | 0.6761 | 1,045 |
| B | GBT | 0.3172 | 0.7928 | 0.1552 | 0.5423 | 419 |
| B | GBT without IDs | 0.3593 | 0.7910 | 0.1615 | 0.5845 | 431 |
| C | Logistic Regression | 0.1038 | 0.7888 | 0.0841 | 0.6761 | 1,045 |
| C | GBT | 0.3172 | 0.7928 | 0.1552 | 0.5423 | 419 |
| C | GBT without IDs | 0.3595 | 0.7910 | 0.1615 | 0.5845 | 431 |

## Chosen models and operating thresholds

| Test metric | A | B | C |
|---|---:|---:|---:|
| Model chosen on validation | GBT | GBT without IDs | GBT without IDs |
| Review threshold | 0.55 | 0.60 | 0.60 |
| Fraud dollars flagged at review | $8,160.71 (32.97%) | 31.7% | 31.7% |
| Review precision | 17.62% | — | — |
| Legitimate orders flagged at review | 360 | 302 | 302 |
| Simulated net savings at $15 per FP | $2,760.71 | $3,317 | $3,317 |
| Simulated net savings per 30 days | $2,640.44 | $3,172 | $3,173 |
| Higher threshold (called BLOCK in output) | 0.90 | 0.95 | 0.95 |
| Test precision at higher threshold | 73.47% | 78% | 78% |
| Legitimate orders flagged at higher threshold | 13 | 4 | 4 |

At the higher threshold in A, the model flagged $2,615.44 in fraud (10.57%) and estimated $2,420.44 in net savings. A did not meet the validation precision target for automated blocking. The higher threshold flags a subset of review orders. Use it to prioritize manual reviews, and avoid adding the savings at the two thresholds. Dashes indicate figures not supplied in the shared results.

## Sensitivity to the assumed false positive cost

| Run | Cost per FP | Review cutoff | Fraud dollars flagged | FP | Simulated net savings |
|---|---:|---:|---:|---:|---:|
| A | $5 | 0.10 | 70.51% | 2,466 | $5,124.25 |
| A | $15 | 0.55 | 32.97% | 360 | $2,760.71 |
| A | $30 | 0.55 | 32.97% | 360 | −$2,639.29 |
| B | $5 | 0.20 | 64.5% | 1,482 | $8,563 |
| B | $15 | 0.60 | 31.7% | 302 | $3,317 |
| B | $30 | 0.85 | 20.8% | 33 | $4,164 |
| C | $5 | 0.20 | 64.5% | 1,481 | $8,568 |
| C | $15 | 0.60 | 31.7% | 302 | $3,317 |
| C | $30 | 0.85 | 20.8% | 33 | $4,164 |

Savings are simulated as flagged fraud dollars minus the assumed cost of false positives. The calculation assumes all flagged fraud is prevented.

## Top features for the selected models

| Rank | A feature and importance | B and C feature and importance |
|---|---|---|
| 1 | P_emaildomain_idx 0.1106 | P_emaildomain_idx 0.1203 |
| 2 | R_emaildomain_idx 0.0733 | TransactionAmt 0.0823 |
| 3 | card1 0.0697 | R_emaildomain_idx 0.0811 |
| 4 | TransactionAmt 0.0494 | D1 0.0548 |
| 5 | card2 0.0490 | D15 0.0488 |
| 6 | D1 0.0470 | uid_prev_txns 0.0482 |
| 7 | D15 0.0408 | C1 0.0471 |
| 8 | C1 0.0403 | hour_of_day 0.0468 |
| 9 | uid_prev_txns 0.0372 | D10 0.0446 |
| 10 | C14 0.0360 | C13 0.0418 |

## What the results support

The GBT without IDs had the highest test AUC-PR in all three runs. The standard GBT still won on validation in A. In B and C, the model without IDs won on validation and gave the same review outcome at 0.60. Use it as the demo candidate for manual review. The one-row difference between B and C and the unconfirmed commit for C prevent calling it an exact reproduction.

For the next comparison, use identical transaction rows and chronological cutoffs. Choose a threshold for each GBT variant on validation at roughly the same review workload, then carry those thresholds into the test period. Compare fraud dollars flagged, review volume, and legitimate orders flagged.

## Saved outputs

- A: model/, model_thresholds/, results-wsl/.
- B: experiment-main/{model,model_thresholds,results}/.
- C: experiment-main-rerun-20261121-131828/{model,model_thresholds,results}/.
