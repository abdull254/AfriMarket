"""
Checks the generated star schema: keys are unique, every link points at a real
row, times are in a sensible order, the "_so_far" columns only look at the past,
and the same seed gives the same files as the ones committed in data/enriched/.

Run: pytest tests/test_star_schema.py
"""
import os
import subprocess
import sys

import pandas as pd
import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "ingestion", "scripts"))


@pytest.fixture(scope="module")
def out(tmp_path_factory):
    d = tmp_path_factory.mktemp("enriched")
    subprocess.run([sys.executable, os.path.join(ROOT, "data_generation", "generate_star_schema.py"),
                    "--transactions", os.path.join(ROOT, "data", "afrimarket_transactions.csv"),
                    "--complaints", os.path.join(ROOT, "data", "afrimarket_complaints.csv"),
                    "--out", str(d)], check=True, capture_output=True)
    return d


def read(out, name, **kw):
    return pd.read_csv(os.path.join(out, f"{name}.csv"), **kw)


def test_primary_keys_unique(out):
    for name, key in [("dim_customer", "customer_id"), ("dim_seller", "seller_id"),
                      ("dim_product", "product_id"), ("dim_date", "date_key"), ("dim_device", "device_id"),
                      ("fact_transaction", "TransactionID"), ("fact_delivery", "delivery_id"),
                      ("fact_refund", "refund_id"), ("complaints_enriched", "ComplaintID")]:
        assert read(out, name)[key].is_unique, name


def test_every_transaction_kept_and_linked(out):
    src = pd.read_csv(os.path.join(ROOT, "data", "afrimarket_transactions.csv"), usecols=["TransactionID"])
    fact = read(out, "fact_transaction")
    assert set(fact["TransactionID"]) == set(src["TransactionID"])
    links = [("customer_id", "dim_customer"), ("seller_id", "dim_seller"), ("product_id", "dim_product"),
             ("date_key", "dim_date"), ("device_id", "dim_device")]
    for col, dim in links:
        assert fact[col].isin(read(out, dim)[col]).all(), col
    for name in ["fact_delivery", "fact_refund", "complaints_enriched"]:
        assert read(out, name)["TransactionID"].isin(fact["TransactionID"]).all(), name
    comp = read(out, "complaints_enriched")
    assert comp["refund_id"].dropna().isin(read(out, "fact_refund")["refund_id"]).all()


def test_original_complaints_kept(out):
    orig = pd.read_csv(os.path.join(ROOT, "data", "afrimarket_complaints.csv"))
    comp = read(out, "complaints_enriched").set_index("ComplaintID")
    kept = comp.loc[orig["ComplaintID"]]
    assert (kept["TransactionID"].to_numpy() == orig["TransactionID"].to_numpy()).all()
    assert (kept["OriginalComplaintText"].to_numpy() == orig["ComplaintText"].to_numpy()).all()
    assert comp["ComplaintText"].nunique() > 0.9 * len(comp)        # text is varied now


def test_times_in_order(out):
    fact = read(out, "fact_transaction", parse_dates=["transaction_ts_utc"]).set_index("TransactionID")
    d = read(out, "fact_delivery", parse_dates=["shipped_at", "delivered_at"])
    ts = fact.loc[d["TransactionID"], "transaction_ts_utc"].to_numpy()
    assert (d["shipped_at"].isna() | (d["shipped_at"].to_numpy() >= ts)).all()
    assert (d["delivered_at"].isna() | (d["delivered_at"].to_numpy() >= ts)).all()
    r = read(out, "fact_refund", parse_dates=["requested_at", "resolved_at"])
    assert (r["requested_at"].to_numpy() >= fact.loc[r["TransactionID"], "transaction_ts_utc"].to_numpy()).all()
    assert (r["resolved_at"].isna() | (r["resolved_at"] >= r["requested_at"])).all()


def test_so_far_columns_only_use_the_past(out):
    fact = read(out, "fact_transaction").sort_values(["TransactionDT", "TransactionID"])
    # A device's customer count can only grow over time, and the first use is new
    for col, key in [("device_customers_so_far", "device_id"), ("ip_customers_so_far", "ip_address")]:
        assert (fact.groupby(key)[col].diff().fillna(0) >= 0).all()
    first = fact.drop_duplicates(["customer_id", "device_id"])
    assert first["is_new_device_for_customer"].all()
    assert fact["is_new_device_for_customer"].sum() == len(first)


def test_same_seed_matches_committed_files(out):
    committed = os.path.join(ROOT, "data", "enriched")
    for name in ["fact_transaction", "complaints_enriched", "fact_refund"]:
        pd.testing.assert_frame_equal(read(out, name), read(committed, name),
                                      obj=f"{name}.csv (re-run data_generation/generate_star_schema.py)")


def test_scaled_copy_keeps_columns_and_ids_unique():
    import numpy as np
    from scale_transactions import make_copy
    base = pd.read_csv(os.path.join(ROOT, "data", "afrimarket_transactions.csv"), nrows=2000)
    period = int(base["TransactionDT"].max() - base["TransactionDT"].min()) + 86_400
    copy = make_copy(base, 1, period, np.random.default_rng(0))
    assert list(copy.columns) == list(base.columns)
    both = pd.concat([base, copy])
    assert both["TransactionID"].is_unique
    assert copy["TransactionDT"].min() > base["TransactionDT"].max()
    assert (copy["isFraud"].to_numpy() == base["isFraud"].to_numpy()).all()
