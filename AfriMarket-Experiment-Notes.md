# AfriMarket fraud model experiment notes

November 21, 2026 · Windows 10 with Ubuntu on WSL 2

## Setup and troubleshooting

- Ubuntu initially failed with error 0x80370102. On the HP EliteDesk 800 G1 SFF, we enabled Virtualization Technology (VTx) in BIOS (F10 → Security → System Security), saved, powered off and back on, and confirmed virtualization in Task Manager. Ubuntu then installed successfully.
- Cloned [AfriMarket](https://github.com/abdull254/AfriMarket) into Ubuntu at `~/projects/AfriMarket`. Run `explorer.exe .` from the repository to open it in Windows File Explorer.
- Installed OpenJDK 17 and uv. Used uv to create a project-specific Python 3.11.18 environment and install the repository requirements, including PySpark 3.5.3 and NumPy 1.26.4. Other projects can keep using Ubuntu’s default Python. See the [uv installer instructions](https://docs.astral.sh/uv/getting-started/installation/).
- The PySpark download took a while. Verbose installation showed the download, then reported 14 packages installed. The reflink messages were debug messages about a fallback.
- The first training attempt returned to Windows after reaching Gradient Boosted Trees. The results directory was empty and the WSL kernel log showed no matching out-of-memory messages. The cause was undetermined. The retry captured output with tee, exited with code 0, and saved the model and reports.

## Reproduce the local runs

In Ubuntu, from the original checkout, activate the environment:

```bash
cd ~/projects/AfriMarket
source .venv/bin/activate
```

The initial successful run used the command below with `--val-fraction 0.15`, `--model-dir ./model`, `--results-dir ./results-wsl`, and log name `training-wsl.log`. Its branch was not recorded in the output shared here.

For the second run we used a separate, detached checkout of GitHub’s main branch. If it already exists, just change into that folder.

```bash
cd ~/projects/AfriMarket
git fetch origin
git worktree add --detach ../AfriMarket-main origin/main
cd ../AfriMarket-main
source ../AfriMarket/.venv/bin/activate

JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" \
spark-submit --master 'local[2]' --driver-memory 4g \
  --py-files features.py train_fraud_model.py \
  --transactions data/afrimarket_transactions.csv \
  --val-fraction 0.20 --test-fraction 0.15 --fp-cost 15 \
  --model-dir ./experiment-main/model \
  --results-dir ./experiment-main/results 2>&1 | tee experiment-main.log
printf 'Spark exit code: %s\n' "${PIPESTATUS[0]}"
```

These commands run locally and do not push changes. The training run uses the transaction CSV directly and does not require the streaming stack. Check the output directories for the saved model, sibling model_thresholds, and reports such as summary.json, model_comparison.json, and threshold_sweep.json.

## Recorded results

| Metric | Initial WSL run | main worktree run |
| --- | ---: | ---: |
| Train / validation / test rows | 20,997 / 4,501 / 4,502 | 19,499 / 5,998 / 4,503 |
| Validation AUC-PR, GBT / GBT without IDs | 0.3957 / 0.3860 | 0.3846 / 0.3933 |
| Test AUC-PR, GBT / GBT without IDs | 0.3458 / 0.3985 | 0.3172 / 0.3593 |
| Test AUC-PR, logistic regression | 0.1054 | 0.1038 |
| Model selected on validation | GBT | GBT without IDs |
| Review cutoff | 0.55 | 0.60 |
| Test fraud dollars flagged at review cutoff | 33.0% | 31.7% |
| Legitimate orders flagged at review cutoff | 360 | 302 |
| Simulated net savings at $15 per false positive | $2,760.71 | $3,317 |
| Higher cutoff / test precision / false positives | 0.90 / 73.5% / 13 | 0.95 / 78% / 4 |

The main run flagged 58 fewer legitimate orders and a slightly smaller share of fraud dollars. The training and validation splits differed, the selected models differed, and the observed test row counts differed by one. These figures do not isolate the effect of branch or ID removal.

Estimated savings subtract the assumed cost of false alarms from flagged fraud dollars. This assumes the flagged fraud could all be prevented. The higher cutoff flags a subset of the orders at the review cutoff, so the two savings figures should not be added. Use the higher cutoff to prioritize manual review for the demo. The initial run did not reach its precision target for automated blocking on validation.

## Next experiment on Abdullahi

The [Abdullahi branch](https://github.com/abdull254/AfriMarket/tree/Abdullahi) already trains both GBT variants and includes prior transaction count, recent purchase frequency, time since last purchase, and amount compared with the customer’s previous average.

Run the same 20% validation and 15% test configuration on Abdullahi with the same CSV and $15 cost assumption. Record the commit and confirm that both runs used the same transaction rows and time cutoffs. Save outputs separately. This matched branch run was proposed and its results were not yet shared.

Then compare the two GBT versions at the same validation review workload. Pick a cutoff for each model that flags roughly the same number of validation orders. Carry both cutoffs forward unchanged and report test review volume, legitimate orders flagged, and fraud dollars flagged. Prefer the model that catches more fraud dollars at a similar workload across multiple chronological validation periods. Use fresh later data for final confirmation if available.
