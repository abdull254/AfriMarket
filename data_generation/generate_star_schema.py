"""
Builds a readable, connected AfriMarket database (star schema) around the
original transactions and complaints.

The original transactions keep every column. This script adds new tables that
link to them by TransactionID:

  dim_customer        one row per customer (card1 + addr1, same "uid" the model uses)
  dim_seller          invented marketplace sellers
  dim_product         invented product catalogue
  dim_date            calendar table for the 183 days of data
  dim_device          devices seen on the platform, with how many customers used each
  fact_transaction    one row per transaction: who, what, where, when, how they paid
  fact_delivery       shipping or digital delivery result per transaction
  fact_refund         returns, refunds and chargebacks
  device_ip_history   which customer used which device and IP, first/last seen
  complaints_enriched the 1,200 complaints with varied text, channel and resolution
                      time, linked to delivery and refund outcomes, plus new
                      complaints raised by those outcomes

Everything is synthetic and repeatable: the same --seed gives the same files.

Usage (from the repo root):
  python data_generation/generate_star_schema.py
  python data_generation/generate_star_schema.py --transactions data/generated/afrimarket_transactions_300k.csv \
      --out data/generated/enriched_300k

--no-fraud-signal makes the new columns independent of isFraud (see docs/DATA_DICTIONARY.md).
"""
import argparse
import hashlib
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import complaint_text  # noqa: E402
from reference_data import (COMPLAINT_CHANNELS, COUNTRIES, COURIERS, PAYMENT_MIX,  # noqa: E402
                            PHYSICAL_CATEGORIES, PRODUCT_GROUPS, PRODUCT_WORDS,
                            REFERENCE_DATE, RESOLUTION_HOURS, SELLER_WORDS_1,
                            SELLER_WORDS_2)

TXN_COLS = ["TransactionID", "TransactionDT", "TransactionAmt", "isFraud", "ProductCD",
            "card1", "addr1", "card4", "card6", "P_emaildomain", "DeviceType", "DeviceInfo"]
N_SELLERS = 400
PRODUCTS_PER_CATEGORY = 45
RISKY_SELLER_SHARE = 0.04
RING_DEVICES = 30


def pick(rng, weights_dict, size=None):
    keys = list(weights_dict)
    w = np.array([weights_dict[k] for k in keys], dtype=float)
    return rng.choice(keys, size=size, p=w / w.sum())


def short_hash(*parts, n=10):
    return hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()[:n].upper()


def fmt_ts(s):
    return pd.to_datetime(s).dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 1. Customers
# ---------------------------------------------------------------------------
def build_customers(tx, rng):
    tx["uid"] = tx["card1"].fillna(-1).astype(int).astype(str) + "_" + tx["addr1"].fillna(-1).astype(int).astype(str)
    first = tx.groupby("uid", sort=False).agg(
        first_ts=("ts_utc", "min"), card1=("card1", "first"), addr1=("addr1", "first"),
        email_domain=("P_emaildomain", lambda s: s.mode().iat[0] if s.notna().any() else None),
        n_txn=("TransactionID", "size")).reset_index().sort_values(["first_ts", "uid"])
    first["customer_id"] = [f"CUS{i:06d}" for i in range(1, len(first) + 1)]

    # The same addr1 (billing region code) always maps to the same country
    weights = {c: v["weight"] for c, v in COUNTRIES.items()}
    regions = sorted(tx["addr1"].dropna().unique())
    region_country = dict(zip(regions, pick(rng, weights, len(regions))))
    first["country"] = [region_country.get(a) if pd.notna(a) else None for a in first["addr1"]]
    missing = first["country"].isna()
    first.loc[missing, "country"] = pick(rng, weights, int(missing.sum()))

    first["city"] = [pick(rng, COUNTRIES[c]["cities"]) for c in first["country"]]
    first["currency"] = first["country"].map(lambda c: COUNTRIES[c]["currency"])
    days_before = np.minimum(rng.exponential(200, len(first)), 900).astype(int)
    first["signup_date"] = (first["first_ts"] - pd.to_timedelta(days_before, unit="D")).dt.strftime("%Y-%m-%d")
    first["phone_masked"] = [COUNTRIES[c]["phone"] + " *** *** " + f"{rng.integers(1000):03d}" for c in first["country"]]
    first["preferred_payment"] = [pick(rng, PAYMENT_MIX[c]) for c in first["country"]]
    first["segment"] = pd.cut(first["n_txn"], [0, 1, 5, 20, 10**9],
                              labels=["one_time", "occasional", "regular", "power_user"]).astype(str)
    first["addr1"] = first["addr1"].astype("Int64")
    cols = ["customer_id", "card1", "addr1", "country", "city", "currency", "signup_date",
            "email_domain", "phone_masked", "preferred_payment", "segment"]
    return first[["uid"] + cols], cols


# ---------------------------------------------------------------------------
# 2. Sellers and products
# ---------------------------------------------------------------------------
def build_sellers(rng):
    names = [f"{a} {b}{s}" for a in SELLER_WORDS_1 for b in SELLER_WORDS_2 for s in ["", " Ltd"]]
    names = list(rng.choice(names, N_SELLERS, replace=False))
    weights = {c: v["weight"] for c, v in COUNTRIES.items()}
    countries = pick(rng, weights, N_SELLERS)
    categories = sorted(PRODUCT_WORDS)
    risky = rng.random(N_SELLERS) < RISKY_SELLER_SHARE
    joined_days = np.where(risky, rng.integers(10, 120, N_SELLERS), rng.integers(60, 1100, N_SELLERS))
    sellers = pd.DataFrame({
        "seller_id": [f"SEL{i:04d}" for i in range(1, N_SELLERS + 1)],
        "seller_name": names,
        "country": countries,
        "city": [pick(rng, COUNTRIES[c]["cities"]) for c in countries],
        "main_category": rng.choice(categories, N_SELLERS),
        "joined_date": (pd.Timestamp(REFERENCE_DATE) - pd.to_timedelta(joined_days, unit="D")).strftime("%Y-%m-%d"),
        "rating": np.round(np.where(risky, rng.uniform(2.5, 4.3, N_SELLERS), rng.uniform(3.6, 5.0, N_SELLERS)), 1),
        "is_verified": np.where(risky, rng.random(N_SELLERS) < 0.3, rng.random(N_SELLERS) < 0.85),
    })
    return sellers, risky


def build_products(sellers, rng):
    rows, pid = [], 1
    for cat in sorted(PRODUCT_WORDS):
        adjs, nouns, (lo, hi) = PRODUCT_WORDS[cat]
        own = sellers.index[sellers["main_category"] == cat].to_numpy()
        for _ in range(PRODUCTS_PER_CATEGORY):
            name = f"{rng.choice(adjs)} {rng.choice(nouns)}"
            if rng.random() < 0.5:
                name += f" {rng.choice(list('ABCDEFGHKMXZ'))}{rng.integers(1, 99)}"
            price = float(np.exp(rng.uniform(np.log(lo), np.log(hi))))
            seller = rng.choice(own) if len(own) and rng.random() < 0.8 else rng.integers(len(sellers))
            rows.append({"product_id": f"PRD{pid:05d}", "product_name": name, "category": cat,
                         "is_physical": cat in PHYSICAL_CATEGORIES, "unit_price_usd": round(price, 2),
                         "default_seller_id": sellers["seller_id"].iat[seller]})
            pid += 1
    return pd.DataFrame(rows)


def assign_products(tx, products, sellers, risky, complaints, fraud_signal, rng):
    """Pick a category (from ProductCD) and a product close in price to TransactionAmt."""
    physical_complaint = complaints.loc[complaints["ComplaintCategory"].isin(
        ["Missing Package", "Wrong Item", "Damaged Product"]), "TransactionID"]
    needs_physical = tx["TransactionID"].isin(physical_complaint).to_numpy()

    cats = np.empty(len(tx), dtype=object)
    for cd, mix in PRODUCT_GROUPS.items():
        idx = np.where(tx["ProductCD"].to_numpy() == cd)[0]
        cats[idx] = pick(rng, mix, len(idx))
        phys_mix = {k: v for k, v in mix.items() if k in PHYSICAL_CATEGORIES}
        fix = idx[needs_physical[idx] & ~np.isin(cats[idx], list(PHYSICAL_CATEGORIES))]
        if len(fix):
            cats[fix] = pick(rng, phys_mix or {"Electronics": 1}, len(fix))
    tx["category"] = cats

    amt = np.log(tx["TransactionAmt"].clip(lower=0.5).to_numpy())
    product_id = np.empty(len(tx), dtype=object)
    qty = np.ones(len(tx), dtype=int)
    for cat, grp in products.groupby("category"):
        idx = np.where(cats == cat)[0]
        if not len(idx):
            continue
        logp = np.log(grp["unit_price_usd"].to_numpy())
        w = np.exp(-np.abs(amt[idx, None] - logp[None, :]) * 2.0)
        w /= w.sum(axis=1, keepdims=True)
        choice = (w.cumsum(axis=1) > rng.random(len(idx))[:, None]).argmax(axis=1)
        product_id[idx] = grp["product_id"].to_numpy()[choice]
        price = grp["unit_price_usd"].to_numpy()[choice]
        qty[idx] = np.clip(np.round(tx["TransactionAmt"].to_numpy()[idx] / price), 1, 10)
    tx["product_id"] = product_id
    tx["quantity"] = qty

    # Seller: usually the product's own seller, sometimes another seller of that category
    prod = products.set_index("product_id")
    seller_ids = prod.loc[tx["product_id"], "default_seller_id"].to_numpy().astype(object)
    other = rng.random(len(tx)) < 0.3
    by_cat = {c: g["seller_id"].to_numpy() for c, g in sellers.groupby("main_category")}
    for i in np.where(other)[0]:
        pool = by_cat.get(cats[i], sellers["seller_id"].to_numpy())
        seller_ids[i] = pool[rng.integers(len(pool))]
    if fraud_signal:
        # Some fraud goes through a few risky (new, unverified) sellers
        risky_ids = sellers.loc[risky, "seller_id"].to_numpy()
        hit = (tx["isFraud"].to_numpy() == 1) & (rng.random(len(tx)) < 0.30)
        seller_ids[hit] = rng.choice(risky_ids, hit.sum())
    tx["seller_id"] = seller_ids


# ---------------------------------------------------------------------------
# 3. Payment, place and local time
# ---------------------------------------------------------------------------
def assign_payment(tx, customers, fraud_signal, rng):
    cust = customers.set_index("uid")
    for col in ["customer_id", "country", "city", "currency", "preferred_payment"]:
        tx[col] = cust.loc[tx["uid"], col].to_numpy()

    physical = tx["category"].isin(PHYSICAL_CATEGORIES).to_numpy()
    fraud = tx["isFraud"].to_numpy() == 1
    methods = np.empty(len(tx), dtype=object)
    keep_pref = rng.random(len(tx)) < 0.75
    for i in range(len(tx)):
        mix = dict(PAYMENT_MIX[tx["country"].iat[i]])
        if not physical[i]:
            mix.pop("CASH_ON_DELIVERY", None)          # nothing to pay for at the door
        if fraud_signal and fraud[i]:
            # stolen-card fraud: mostly card, almost never cash on delivery
            mix = {k: v * (3.0 if k == "CARD" else 0.2 if k == "CASH_ON_DELIVERY" else 1.0) for k, v in mix.items()}
        pref = tx["preferred_payment"].iat[i]
        methods[i] = pref if keep_pref[i] and pref in mix and not (fraud_signal and fraud[i]) else pick(rng, mix)
    tx["payment_method"] = methods
    card = methods == "CARD"
    tx["card_network"] = np.where(card, tx["card4"].fillna("visa"), None)
    tx["card_type"] = np.where(card, tx["card6"].fillna("debit"), None)

    tx["fx_rate_usd"] = tx["country"].map(lambda c: COUNTRIES[c]["fx"])
    tx["amount_local"] = (tx["TransactionAmt"] * tx["fx_rate_usd"]).round(2)
    offset = tx["country"].map(lambda c: COUNTRIES[c]["utc"])
    tx["ts_local"] = tx["ts_utc"] + pd.to_timedelta(offset, unit="h")
    tx["utc_offset"] = offset
    tx["date_key"] = tx["ts_local"].dt.strftime("%Y%m%d").astype(int)
    tx["local_hour"] = tx["ts_local"].dt.hour
    tx["local_weekday"] = tx["ts_local"].dt.day_name()
    tx["is_weekend"] = tx["ts_local"].dt.day_of_week >= 5


def build_dates(tx):
    days = pd.date_range(tx["ts_local"].min().normalize(), tx["ts_local"].max().normalize(), freq="D")
    return pd.DataFrame({
        "date_key": days.strftime("%Y%m%d").astype(int), "date": days.strftime("%Y-%m-%d"),
        "year": days.year, "quarter": days.quarter, "month": days.month, "month_name": days.month_name(),
        "week_of_year": days.isocalendar().week.to_numpy(), "day_of_month": days.day,
        "weekday": days.day_of_week + 1, "weekday_name": days.day_name(), "is_weekend": days.day_of_week >= 5,
        "is_month_end": days.is_month_end,
    })


# ---------------------------------------------------------------------------
# 4. Devices and IP addresses
# ---------------------------------------------------------------------------
DEFAULT_DEVICES = {("mobile", "Android"): 0.55, ("mobile", "iOS Device"): 0.15,
                   ("desktop", "Windows"): 0.22, ("desktop", "MacOS"): 0.08}


def random_ip(rng, country):
    first = COUNTRIES[country]["ip"]
    return f"{first[rng.integers(len(first))]}.{rng.integers(1, 255)}.{rng.integers(0, 255)}.{rng.integers(1, 255)}"


def assign_devices(tx, fraud_signal, rng):
    """Give every customer 1-3 devices; fraud sometimes uses a new or a shared 'ring' device.

    Columns ending in _so_far only count earlier transactions, so they are safe model inputs.
    """
    keys = list(DEFAULT_DEVICES)
    kw = np.array(list(DEFAULT_DEVICES.values()))

    devices = {}                       # device_id -> (type, info)
    cust_devices = {}                  # customer -> list of (device_id, home_ip)
    ring = []
    for r in range(RING_DEVICES):
        did = "DEV" + short_hash("ring", r)
        devices[did] = ("mobile", "Android") if r % 3 else ("desktop", "Windows")
        ring.append((did, random_ip(rng, list(COUNTRIES)[r % len(COUNTRIES)])))

    dev_col, ip_col = [], []
    new_dev, new_ip, dev_users_so_far, ip_users_so_far = [], [], [], []
    seen_cd, seen_ci = set(), set()
    city_devices = {}                  # city -> primary devices of earlier customers (family/shared phones)
    public_ips = {}                    # city -> a few shared IPs (cyber cafes, mobile carrier NAT)
    dev_users, ip_users = {}, {}
    fraud = tx["isFraud"].to_numpy() == 1
    u = rng.random((len(tx), 4))
    for i, row in enumerate(tx[["customer_id", "country", "city", "DeviceType", "DeviceInfo"]].itertuples(index=False)):
        cid, country, city, dtype, dinfo = row
        if cid not in cust_devices:
            n = 1 + min(rng.poisson(0.4), 2)
            lst = []
            pool = city_devices.setdefault(city, [])
            if pool and rng.random() < 0.03:                         # shares a device with family
                lst.append(pool[rng.integers(len(pool))])
            for k in range(n - len(lst)):
                did = "DEV" + short_hash(cid, k)
                if k == 0 and isinstance(dinfo, str):
                    devices[did] = (dtype if isinstance(dtype, str) else "desktop", dinfo)
                else:
                    devices[did] = keys[rng.choice(len(keys), p=kw)]
                lst.append((did, random_ip(rng, country)))
            cust_devices[cid] = lst
            pool.append(lst[0])
        lst = cust_devices[cid]
        if fraud_signal and fraud[i] and u[i, 0] < 0.25:
            did, ip = ring[rng.integers(len(ring))]                   # shared fraud-ring device
        elif (fraud_signal and fraud[i] and u[i, 0] < 0.50) or u[i, 0] < 0.005:
            did = "DEV" + short_hash(cid, "new", i)                   # brand-new device
            devices[did] = keys[rng.choice(len(keys), p=kw)]
            other = list(COUNTRIES)[rng.integers(len(COUNTRIES))] if fraud[i] and fraud_signal else country
            ip = random_ip(rng, other)
        else:
            did, ip = lst[0] if u[i, 1] < 0.8 or len(lst) == 1 else lst[1 + rng.integers(len(lst) - 1)]
            if u[i, 2] < 0.05:                                        # cyber cafe / carrier NAT
                shared = public_ips.setdefault(city, [random_ip(rng, country) for _ in range(5)])
                ip = shared[rng.integers(len(shared))]
            elif u[i, 2] < 0.20:                                      # mobile networks change IPs
                ip = random_ip(rng, country)
        new_dev.append((cid, did) not in seen_cd)
        new_ip.append((cid, ip) not in seen_ci)
        seen_cd.add((cid, did))
        seen_ci.add((cid, ip))
        dev_users.setdefault(did, set()).add(cid)
        ip_users.setdefault(ip, set()).add(cid)
        dev_users_so_far.append(len(dev_users[did]))
        ip_users_so_far.append(len(ip_users[ip]))
        dev_col.append(did)
        ip_col.append(ip)

    tx["device_id"] = dev_col
    tx["ip_address"] = ip_col
    tx["is_new_device_for_customer"] = new_dev
    tx["is_new_ip_for_customer"] = new_ip
    tx["device_customers_so_far"] = dev_users_so_far
    tx["ip_customers_so_far"] = ip_users_so_far

    used = tx.groupby("device_id").agg(first_seen=("ts_utc", "min"), last_seen=("ts_utc", "max"),
                                       txn_count=("TransactionID", "size"),
                                       distinct_customers=("customer_id", "nunique")).reset_index()
    used["device_type"] = used["device_id"].map(lambda d: devices[d][0])
    used["device_info"] = used["device_id"].map(lambda d: devices[d][1])
    used["is_shared"] = used["distinct_customers"] > 1
    used["first_seen"] = fmt_ts(used["first_seen"])
    used["last_seen"] = fmt_ts(used["last_seen"])
    dim_device = used[["device_id", "device_type", "device_info", "first_seen", "last_seen",
                       "txn_count", "distinct_customers", "is_shared"]]

    hist = tx.groupby(["customer_id", "device_id", "ip_address"]).agg(
        first_seen=("ts_utc", "min"), last_seen=("ts_utc", "max"), txn_count=("TransactionID", "size")).reset_index()
    hist["first_seen"] = fmt_ts(hist["first_seen"])
    hist["last_seen"] = fmt_ts(hist["last_seen"])
    return dim_device, hist


# ---------------------------------------------------------------------------
# 5. Deliveries
# ---------------------------------------------------------------------------
def build_deliveries(tx, sellers, complaints, fraud_signal, rng):
    data_end = tx["ts_utc"].max()
    seller = sellers.set_index("seller_id")
    s_country = seller.loc[tx["seller_id"], "country"].to_numpy()
    s_city = seller.loc[tx["seller_id"], "city"].to_numpy()
    physical = tx["category"].isin(PHYSICAL_CATEGORIES).to_numpy()
    fraud = tx["isFraud"].to_numpy() == 1
    missing = set(complaints.loc[complaints["ComplaintCategory"] == "Missing Package", "TransactionID"])
    # Damaged or wrong items must have arrived before the customer complained
    received = complaints[complaints["ComplaintCategory"].isin(["Damaged Product", "Wrong Item"])]
    received = received.groupby("TransactionID")["ComplaintDate"].min()
    n = len(tx)

    same_city = (s_city == tx["city"].to_numpy()) & (s_country == tx["country"].to_numpy())
    cross = s_country != tx["country"].to_numpy()
    promised = np.where(same_city, 2, np.where(cross, 10, 5))
    ship_h = rng.gamma(2.0, 6.0, n)
    transit_d = np.where(same_city, rng.uniform(0.2, 1.9, n),
                         np.where(cross, rng.uniform(3, 11.5, n), rng.uniform(1, 5, n)))

    p_cancel = np.where(fraud & fraud_signal, 0.10, 0.03)
    p_lost = np.where(fraud & fraud_signal, 0.04, 0.012)
    p_return = 0.04
    u = rng.random(n)
    status = np.select([u < p_cancel, u < p_cancel + p_lost, u < p_cancel + p_lost + p_return],
                       ["CANCELLED", "LOST", "RETURNED"], "DELIVERED").astype(object)
    # A missing-package complaint means the parcel was lost or "delivered" but not received
    tids = tx["TransactionID"].to_numpy()
    m = np.isin(tids, list(missing)) & physical
    status[m] = np.where(rng.random(m.sum()) < 0.45, "LOST", "DELIVERED")

    ts = tx["ts_utc"]
    got = np.isin(tids, received.index) & physical
    if got.any():
        complained = pd.to_datetime(received.reindex(tids[got]).to_numpy())
        hours_free = (complained - ts[got].to_numpy()).total_seconds().to_numpy() / 3600
        ship_h[got] = np.minimum(ship_h[got], hours_free * 0.3)
        transit_d[got] = np.clip(np.minimum(transit_d[got], (hours_free - ship_h[got] - 2) / 24), 0.05, None)
        status[got] = np.where(rng.random(got.sum()) < 0.25, "RETURNED", "DELIVERED")
    shipped = ts + pd.to_timedelta(ship_h, unit="h")
    delivered = shipped + pd.to_timedelta(transit_d, unit="D")
    in_transit = physical & (status == "DELIVERED") & (delivered > data_end).to_numpy() & ~got
    status[in_transit] = "IN_TRANSIT"

    # Digital products are delivered in seconds; a few fail
    dig_fail = ~physical & (rng.random(n) < 0.01)
    status = np.where(physical, status, np.where(dig_fail, "FAILED", "DELIVERED")).astype(object)
    delivered = delivered.where(physical, ts + pd.to_timedelta(rng.uniform(2, 120, n), unit="s"))
    shipped = shipped.where(physical, pd.NaT)

    done = np.isin(status, ["DELIVERED", "RETURNED"])
    deliv = pd.DataFrame({
        "delivery_id": [f"DLV{i:07d}" for i in range(1, n + 1)],
        "TransactionID": tids,
        "delivery_type": np.where(physical, "PHYSICAL", "DIGITAL"),
        "courier": np.where(physical, np.array(COURIERS)[rng.integers(len(COURIERS), size=n)], None),
        "origin_city": np.where(physical, s_city, None),
        "destination_city": tx["city"].to_numpy(),
        "cross_border": np.where(physical, cross, False),
        "shipped_at": fmt_ts(shipped.where(~np.isin(status, ["CANCELLED"]), pd.NaT)),
        "promised_at": fmt_ts((ts + pd.to_timedelta(promised, unit="D")).where(physical, pd.NaT)),
        "delivered_at": fmt_ts(delivered.where(done, pd.NaT)),
        "delivery_days": np.where(done & physical, np.round((ship_h / 24 + transit_d), 1), np.nan),
        "delivery_status": status,
    })
    deliv["is_late"] = physical & done & (deliv["delivery_days"] > promised)
    for c in ["shipped_at", "promised_at", "delivered_at"]:
        deliv.loc[deliv[c] == "NaT", c] = None
    return deliv


# ---------------------------------------------------------------------------
# 6. Refunds, returns and chargebacks
# ---------------------------------------------------------------------------
def build_refunds(tx, deliv, complaints, rng):
    data_end = tx["ts_utc"].max()
    d = deliv.set_index("TransactionID")
    t = tx.set_index("TransactionID")
    comp = complaints.set_index("TransactionID")
    rows = []

    def add(tid, kind, reason, start, share=1.0):
        amt = round(float(t.at[tid, "TransactionAmt"]) * share, 2)
        requested = max(start, t.at[tid, "ts_utc"]) + pd.Timedelta(hours=float(rng.uniform(1, 24 * 5)))
        took = pd.Timedelta(days=float(rng.gamma(2.0, 3.0 if kind != "CHARGEBACK" else 12.0)))
        resolved = requested + took
        if resolved > data_end:
            status, resolved = "PENDING", None
        elif kind == "CHARGEBACK":
            status = "CUSTOMER_WON" if (t.at[tid, "isFraud"] == 1 or rng.random() < 0.4) else "MERCHANT_WON"
        else:
            status = "COMPLETED" if rng.random() < 0.92 else "REJECTED"
        rows.append({"TransactionID": tid, "refund_type": kind, "reason": reason, "amount_usd": amt,
                     "requested_at": requested, "resolved_at": resolved, "refund_status": status})

    for tid, status, delivered_at in zip(deliv["TransactionID"], deliv["delivery_status"], deliv["delivered_at"]):
        ts = t.at[tid, "ts_utc"]
        cod = t.at[tid, "payment_method"] == "CASH_ON_DELIVERY"
        if status == "RETURNED":
            add(tid, "RETURN", "Customer returned item", pd.Timestamp(delivered_at) + pd.Timedelta(days=1))
        elif status == "LOST" and not cod:
            add(tid, "REFUND", "Lost in transit", ts + pd.Timedelta(days=7))
        elif status == "CANCELLED" and not cod:
            add(tid, "REFUND", "Order cancelled", ts)
        elif status == "FAILED":
            add(tid, "REFUND", "Digital delivery failed", ts)
        elif d.at[tid, "is_late"] and rng.random() < 0.10:
            add(tid, "REFUND", "Late delivery compensation", pd.Timestamp(delivered_at), share=0.1)

    # Chargebacks: the bank reverses the payment. This is where fraud labels come from
    # in real life, so it is an outcome, never a model input.
    fraud = tx["isFraud"].to_numpy() == 1
    cod = tx["payment_method"].to_numpy() == "CASH_ON_DELIVERY"
    u = rng.random(len(tx))
    cb = ~cod & ((fraud & (u < 0.65)) | (~fraud & (u < 0.004)))
    for tid, ts in zip(tx.loc[cb, "TransactionID"], tx.loc[cb, "ts_utc"]):
        add(tid, "CHARGEBACK", "Unauthorized transaction" if t.at[tid, "isFraud"] == 1 else "Item not received",
            ts + pd.Timedelta(days=float(rng.uniform(5, 45))))

    # Complaints about refunds, damage or wrong items get a refund record too
    have = {(r["TransactionID"], r["refund_type"]) for r in rows}
    want = {"Refund Issue": ("REFUND", "Refund requested by customer"),
            "Damaged Product": ("RETURN", "Item damaged"), "Wrong Item": ("RETURN", "Wrong item sent")}
    for tid, row in comp.iterrows():
        if row["ComplaintCategory"] in want:
            kind, reason = want[row["ComplaintCategory"]]
            if (tid, kind) not in have:
                add(tid, kind, reason, pd.Timestamp(row["ComplaintDate"]) - pd.Timedelta(days=float(rng.uniform(0.5, 3))))

    ref = pd.DataFrame(rows).sort_values("requested_at").reset_index(drop=True)
    ref.insert(0, "refund_id", [f"RFD{i:06d}" for i in range(1, len(ref) + 1)])
    ref["requested_at"] = fmt_ts(ref["requested_at"])
    ref["resolved_at"] = fmt_ts(ref["resolved_at"])
    ref.loc[ref["resolved_at"] == "NaT", "resolved_at"] = None
    return ref


# ---------------------------------------------------------------------------
# 7. Complaints
# ---------------------------------------------------------------------------
METHOD_WORDS = {"CARD": "card", "M-PESA": "M-PESA account", "AIRTEL_MONEY": "Airtel Money wallet",
                "MOBILE_MONEY": "mobile money wallet", "BANK_TRANSFER": "bank account",
                "INSTANT_EFT": "bank account", "CASH_ON_DELIVERY": "cash on delivery order"}


def build_complaints(complaints, tx, deliv, refunds, products, sellers, rng):
    data_end = tx["ts_utc"].max() + pd.Timedelta(days=30)
    t = tx.set_index("TransactionID")
    d = deliv.set_index("TransactionID")
    pname = products.set_index("product_id")["product_name"]
    sname = sellers.set_index("seller_id")["seller_name"]
    first_refund = refunds.drop_duplicates("TransactionID").set_index("TransactionID")

    # New complaints raised by outcomes (lost parcels, late parcels, chargebacks)
    have = set(complaints["TransactionID"])
    extra = []
    for tid, status, late in zip(deliv["TransactionID"], deliv["delivery_status"], deliv["is_late"]):
        if tid in have:
            continue
        cat = None
        if status == "LOST" and rng.random() < 0.5:
            cat = "Missing Package"
        elif late and rng.random() < 0.04:
            cat = "Late Delivery"
        if cat:
            extra.append((tid, cat, t.at[tid, "ts_utc"] + pd.Timedelta(days=float(rng.uniform(3, 20)))))
            have.add(tid)
    for tid, req in zip(refunds.loc[refunds["refund_type"] == "CHARGEBACK", "TransactionID"],
                        refunds.loc[refunds["refund_type"] == "CHARGEBACK", "requested_at"]):
        if tid not in have and rng.random() < 0.3:
            cat = "Unauthorized Charge" if t.at[tid, "isFraud"] == 1 else "Chargeback Dispute"
            extra.append((tid, cat, pd.Timestamp(req) + pd.Timedelta(days=float(rng.uniform(0, 5)))))
            have.add(tid)

    base = complaints.copy()
    base["ComplaintDate"] = pd.to_datetime(base["ComplaintDate"])
    base["source"] = "original"
    new = pd.DataFrame(extra, columns=["TransactionID", "ComplaintCategory", "ComplaintDate"])
    new["source"] = "generated_from_outcome"
    new["ComplaintText"] = None
    new["ComplaintStatus"] = None
    start = int(base["ComplaintID"].str[3:].astype(int).max()) + 1
    new["ComplaintID"] = [f"CMP{i:05d}" for i in range(start, start + len(new))]
    c = pd.concat([base, new], ignore_index=True)
    c = c[c["TransactionID"].isin(t.index)].reset_index(drop=True)

    texts, chan, prio, first_resp, resolved, statuses = [], [], [], [], [], []
    for row in c.itertuples(index=False):
        tx_row = t.loc[row.TransactionID]
        method = METHOD_WORDS.get(tx_row["payment_method"], "account")
        days = max(1, int((row.ComplaintDate - tx_row["ts_utc"]).days))
        slots = dict(product=pname[tx_row["product_id"]], courier=d.at[row.TransactionID, "courier"] or "the courier",
                     seller=sname[tx_row["seller_id"]], method=method, days=days, promised=5,
                     amount=f"{tx_row['currency']} {tx_row['amount_local']:,.0f}", tid=row.TransactionID)
        texts.append(complaint_text.build(row.ComplaintCategory, rng, **slots))
        chan.append(pick(rng, COMPLAINT_CHANNELS))
        prio.append("HIGH" if row.ComplaintCategory in ("Unauthorized Charge", "Suspicious Seller")
                    else "MEDIUM" if row.ComplaintCategory in ("Missing Package", "Refund Issue", "Chargeback Dispute")
                    else "LOW")
        fr = row.ComplaintDate + pd.Timedelta(hours=float(rng.gamma(1.5, 4.0)))
        res = row.ComplaintDate + pd.Timedelta(hours=float(rng.gamma(2.0, RESOLUTION_HOURS[row.ComplaintCategory] / 2)))
        status = row.ComplaintStatus
        if not isinstance(status, str):                       # generated complaint: status from its age
            status = "Resolved" if res < data_end else ("Under Review" if fr < data_end else "Open")
        first_resp.append(fr if status != "Open" else None)
        resolved.append(res if status == "Resolved" else None)
        statuses.append(status)

    c["OriginalComplaintText"] = c["ComplaintText"]
    c["ComplaintText"] = texts
    c["ComplaintStatus"] = statuses
    c["channel"] = chan
    c["priority"] = prio
    c["first_response_at"] = fmt_ts(pd.Series(first_resp, dtype="datetime64[ns]"))
    c["resolved_at"] = fmt_ts(pd.Series(resolved, dtype="datetime64[ns]"))
    for col in ["first_response_at", "resolved_at"]:
        c.loc[c[col] == "NaT", col] = None
    c["resolution_hours"] = ((pd.to_datetime(c["resolved_at"]) - c["ComplaintDate"]).dt.total_seconds() / 3600).round(1)
    c["ComplaintDate"] = fmt_ts(c["ComplaintDate"])
    c["customer_id"] = t.loc[c["TransactionID"], "customer_id"].to_numpy()
    c["seller_id"] = t.loc[c["TransactionID"], "seller_id"].to_numpy()
    c["product_id"] = t.loc[c["TransactionID"], "product_id"].to_numpy()
    c["delivery_status"] = d.loc[c["TransactionID"], "delivery_status"].to_numpy()
    c["refund_id"] = c["TransactionID"].map(first_refund["refund_id"])
    c = c.sort_values("ComplaintID").reset_index(drop=True)
    return c[["ComplaintID", "TransactionID", "ComplaintDate", "ComplaintCategory", "ComplaintText",
              "ComplaintStatus", "channel", "priority", "first_response_at", "resolved_at",
              "resolution_hours", "customer_id", "seller_id", "product_id", "delivery_status",
              "refund_id", "source", "OriginalComplaintText"]]


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--transactions", default="data/afrimarket_transactions.csv")
    ap.add_argument("--complaints", default="data/afrimarket_complaints.csv")
    ap.add_argument("--out", default="data/enriched")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--no-fraud-signal", action="store_true",
                    help="do not let isFraud change sellers, payment, devices or deliveries")
    args = ap.parse_args()
    rng = np.random.default_rng(args.seed)
    fraud_signal = not args.no_fraud_signal

    tx = pd.read_csv(args.transactions, usecols=TXN_COLS)
    complaints = pd.read_csv(args.complaints)
    tx["ts_utc"] = pd.Timestamp(REFERENCE_DATE) + pd.to_timedelta(tx["TransactionDT"], unit="s")
    print(f"Loaded {len(tx):,} transactions and {len(complaints):,} complaints")

    customers, cust_cols = build_customers(tx, rng)
    sellers, risky = build_sellers(rng)
    products = build_products(sellers, rng)
    assign_products(tx, products, sellers, risky, complaints, fraud_signal, rng)
    assign_payment(tx, customers, fraud_signal, rng)
    dim_date = build_dates(tx)
    tx = tx.sort_values(["ts_utc", "TransactionID"]).reset_index(drop=True)   # time order for devices
    dim_device, history = assign_devices(tx, fraud_signal, rng)
    deliv = build_deliveries(tx, sellers, complaints, fraud_signal, rng)
    refunds = build_refunds(tx, deliv, complaints, rng)
    comp = build_complaints(complaints, tx, deliv, refunds, products, sellers, rng)

    fact = tx.assign(transaction_ts_utc=fmt_ts(tx["ts_utc"]), transaction_ts_local=fmt_ts(tx["ts_local"]))
    fact = fact[["TransactionID", "TransactionDT", "transaction_ts_utc", "transaction_ts_local", "utc_offset",
                 "date_key", "local_hour", "local_weekday", "is_weekend", "customer_id", "seller_id",
                 "product_id", "category", "quantity", "TransactionAmt", "currency", "fx_rate_usd",
                 "amount_local", "payment_method", "card_network", "card_type", "country", "city",
                 "device_id", "ip_address", "is_new_device_for_customer", "is_new_ip_for_customer",
                 "device_customers_so_far", "ip_customers_so_far", "isFraud"]]

    os.makedirs(args.out, exist_ok=True)
    tables = {
        "dim_customer": customers[cust_cols], "dim_seller": sellers, "dim_product": products,
        "dim_date": dim_date, "dim_device": dim_device, "fact_transaction": fact,
        "fact_delivery": deliv, "fact_refund": refunds, "device_ip_history": history,
        "complaints_enriched": comp,
    }
    for name, df in tables.items():
        df.to_csv(os.path.join(args.out, f"{name}.csv"), index=False)
        print(f"  {name:20s} {len(df):>8,} rows")

    summary = {
        "seed": args.seed, "fraud_signal": fraud_signal, "source_transactions": args.transactions,
        "rows": {k: int(len(v)) for k, v in tables.items()},
        "fraud_rate_by_payment_method": fact.groupby("payment_method")["isFraud"].mean().round(4).to_dict(),
        "fraud_rate_by_country": fact.groupby("country")["isFraud"].mean().round(4).to_dict(),
        "delivery_status": deliv["delivery_status"].value_counts().to_dict(),
        "refund_type": refunds["refund_type"].value_counts().to_dict(),
        "complaint_source": comp["source"].value_counts().to_dict(),
        "distinct_complaint_texts": int(comp["ComplaintText"].nunique()),
        "shared_devices": int(dim_device["is_shared"].sum()),
    }
    with open(os.path.join(args.out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"Wrote {len(tables)} tables to {args.out}/")


if __name__ == "__main__":
    main()
