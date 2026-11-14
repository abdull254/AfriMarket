"""
Live (online) customer features for real-time scoring, without reading HDFS.

Keeps a small running state per customer (uid = card1 + addr1) and computes the
same five features as features.py, with the same point-in-time rule:
only purchases with a STRICTLY EARLIER TransactionDT count.

  uid_prev_txns        purchases before this one
  txns_last_1h         purchases in [t-3600, t-1]
  txns_last_24h        purchases in [t-86400, t-1]
  secs_since_last_txn  t minus the latest earlier purchase   (-1 if none)
  amt_vs_uid_mean      amount / mean of earlier amounts       (-1 if none)

Order of work for each purchase: READ features from state, THEN add the purchase
to state. So the current purchase is never counted in its own features.

Backends:
  MemoryStore  - a Python dict (tests, demos)
  RedisStore   - Redis (production): per customer a few running totals plus
                 the last 24 h of purchase times, so state stays small
Both are duplicate-safe: features for a TransactionID are cached for 48 h, so a
re-sent message (producer retry, job restart) gets the same features and is not
added to the state twice.
"""
import bisect
import json
import math

DAY = 86400
HOUR = 3600
FEATURE_TTL = 2 * DAY
ONLINE = ["uid_prev_txns", "txns_last_1h", "txns_last_24h", "secs_since_last_txn", "amt_vs_uid_mean"]
# What a brand-new customer looks like in training. Used if the store is down.
NEW_CUSTOMER = {"uid_prev_txns": 0, "txns_last_1h": 0, "txns_last_24h": 0,
                "secs_since_last_txn": -1.0, "amt_vs_uid_mean": -1.0}


def _num(x):
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(x) else x


def make_uid(card1, addr1):
    """Same as Spark's concat_ws('_', long(card1), long(addr1)): nulls are skipped."""
    parts = [str(int(v)) for v in (_num(card1), _num(addr1)) if v is not None]
    return "_".join(parts)


def _features(t, amt, n, amt_n, amt_sum, last, c1h, c24h):
    return {
        "uid_prev_txns": n,
        "txns_last_1h": c1h,
        "txns_last_24h": c24h,
        "secs_since_last_txn": float(t - last) if last is not None else -1.0,
        "amt_vs_uid_mean": (amt / (amt_sum / amt_n)) if (amt is not None and amt_n and amt_sum) else -1.0,
    }


class MemoryStore:
    def __init__(self):
        self.state, self.cache = {}, {}

    def read(self, uid, t, amt):
        s = self.state.get(uid)
        if not s:
            return dict(NEW_CUSTOMER)
        times = s["times"]
        hi = bisect.bisect_left(times, t)                      # strictly earlier than t
        c1h = hi - bisect.bisect_left(times, t - HOUR)
        c24h = hi - bisect.bisect_left(times, t - DAY)
        last = times[hi - 1] if hi else None
        return _features(t, amt, hi, s["amt_n"], s["amt_sum"], last, c1h, c24h)

    def add(self, uid, t, amt):
        s = self.state.setdefault(uid, {"times": [], "amt_n": 0, "amt_sum": 0.0})
        bisect.insort(s["times"], t)
        if amt is not None:
            s["amt_n"] += 1
            s["amt_sum"] += amt

    def get_cached(self, tx_id):
        return self.cache.get(tx_id)

    def set_cached(self, tx_id, feats):
        self.cache[tx_id] = feats


class RedisStore:
    """Per customer: hash  u:<uid>  {n, amt_n, amt_sum, last}   (a few numbers)
                     zset  t:<uid>  purchase times in the last 24 h only
       Per purchase: f:<TransactionID> = cached features (48 h TTL).
    State per customer stays small no matter how long the history gets.
    Assumes purchases for a customer arrive in time order (Kafka keeps order
    within a partition, so the producer should key messages by card)."""

    def __init__(self, client):
        self.r = client

    def read(self, uid, t, amt):
        h = {k.decode() if isinstance(k, bytes) else k: v for k, v in self.r.hgetall(f"u:{uid}").items()}
        if not h:
            return dict(NEW_CUSTOMER)
        key = f"t:{uid}"
        c1h = self.r.zcount(key, t - HOUR, f"({t}")
        c24h = self.r.zcount(key, t - DAY, f"({t}")
        return _features(t, amt, int(h["n"]), int(h.get("amt_n", 0)), float(h.get("amt_sum", 0.0)),
                         float(h["last"]), c1h, c24h)

    def add(self, uid, t, amt):
        key = f"u:{uid}"
        seq = self.r.hincrby(key, "n", 1)
        p = self.r.pipeline()
        p.hset(key, "last", t)
        if amt is not None:
            p.hincrby(key, "amt_n", 1)
            p.hincrbyfloat(key, "amt_sum", amt)
        p.zadd(f"t:{uid}", {f"{t}:{seq}": t})
        p.zremrangebyscore(f"t:{uid}", "-inf", f"({t - DAY}")   # keep only the last 24 h
        p.execute()

    def get_cached(self, tx_id):
        v = self.r.get(f"f:{tx_id}")
        return json.loads(v) if v else None

    def set_cached(self, tx_id, feats):
        self.r.set(f"f:{tx_id}", json.dumps(feats), ex=FEATURE_TTL)


def compute_online_features(rows, store):
    """rows: list of dicts with TransactionID, TransactionDT, TransactionAmt, card1, addr1.
    Returns one dict per row (same order) with the five ONLINE features plus
    'features_degraded' (True if the store failed and new-customer values were used).

    Purchases in the same second are all read before any of them is added, so
    same-second purchases never count each other (matches features.py)."""
    out = [None] * len(rows)
    order = sorted(range(len(rows)), key=lambda i: float(rows[i]["TransactionDT"]))
    i = 0
    while i < len(order):
        t = float(rows[order[i]]["TransactionDT"])
        group = []
        while i < len(order) and float(rows[order[i]]["TransactionDT"]) == t:
            group.append(order[i]); i += 1
        to_add = []
        for k in group:
            r = rows[k]
            uid, amt, tx = make_uid(r.get("card1"), r.get("addr1")), _num(r.get("TransactionAmt")), r["TransactionID"]
            try:
                cached = store.get_cached(tx)
                if cached is not None:                       # duplicate message: same answer, no re-add
                    out[k] = dict(cached, features_degraded=False)
                    continue
                f = store.read(uid, t, amt)
                store.set_cached(tx, f)
                to_add.append((uid, amt))
                out[k] = dict(f, features_degraded=False)
            except Exception:                                # store down: score as a new customer
                out[k] = dict(NEW_CUSTOMER, features_degraded=True)
        for uid, amt in to_add:
            try:
                store.add(uid, t, amt)
            except Exception:
                pass
    return out
