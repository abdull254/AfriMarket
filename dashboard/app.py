import os
from pathlib import Path

import mysql.connector
import pandas as pd
import streamlit as st
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType, dict_factory


st.set_page_config(
    page_title="AfriMarket Fraud Intelligence",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      .block-container {padding-top: 2rem; padding-bottom: 2rem;}
      /* AfriMarket metric cards */
      [data-testid="stMetric"] {
          background: #FFFFFF;
          border: 1px solid #E2E8F0;
          border-top: 4px solid #008E92;
          border-radius: 14px;
          padding: 22px 20px;
          box-shadow: 0 5px 18px rgba(0, 33, 56, 0.07);
      }

      [data-testid="stMetricLabel"] {
          color: #526477;
          font-size: 15px;
          font-weight: 600;
      }

      [data-testid="stMetricValue"] {
          color: #002138;
          font-size: 32px;
          font-weight: 700;
      }

      [data-testid="stMetricLabel"] {
          color: #64748B;
      }
      [data-testid="stMetricValue"] {
          color: #172338;
      }

      h1, h2, h3 {letter-spacing: -0.5px;}
      [data-testid="stSidebar"] {
          background: linear-gradient(
              155deg,
              #002138 0%,
              #00182e 58%,
              #00374c 100%
          ) !important;
          border-right: 1px solid #07536b;
      }

      [data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
      [data-testid="stSidebar"] label,
      [data-testid="stSidebar"] p {
        color: #E8F2F5;
      }
      [data-testid="stSidebar"] [role="radiogroup"] {
        gap: 0.5rem;
      }
      [data-testid="stSidebar"] [role="radiogroup"] label {
        display: flex;
        align-items: center;
        min-height: 58px;
        padding: 12px 16px;
        border-radius: 12px;
        border: 1px solid transparent;
        background: transparent;
      }
      [data-testid="stSidebar"] [role="radiogroup"] label:hover {
        background: rgba(30, 190, 181, .14);
      }
      [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
        background: linear-gradient(105deg, #008e92, #007481);
        border: 1px solid #24d3c3;
        border-radius: 12px;
        box-shadow: 0 4px 14px rgba(0, 0, 0, 0.15);
      }
      [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p {
        color: #FFFFFF; font-weight: 700;
      }
      [data-testid="stSidebar"] [data-testid="stImage"] img {
        width: 100%; border-radius: 0.45rem;
      }

      /* Hide navigation radio circles */
      [data-testid="stSidebar"] [role="radiogroup"] label [data-testid="stWidgetLabel"] {
          display: none;
      }

      [data-testid="stSidebar"] [role="radiogroup"] label > div:first-child {
          display: none;
      }

      /* Professional sidebar navigation buttons */
      [data-testid="stSidebar"] div.stButton > button {
          width: 100%;
          min-height: 60px;
          font-size: 18px;
          font-weight: 600;
          text-align: left;
          justify-content: flex-start;
          padding: 14px 22px;
          margin-bottom: 8px;
          border-radius: 10px;
      }

      /* Inactive navigation buttons */
      [data-testid="stSidebar"] div.stButton > button[kind="secondary"] {
          background: transparent;
          color: #E8F2F5;
          border: 1px solid transparent;
      }

      /* Selected navigation button */
      [data-testid="stSidebar"] div.stButton > button[kind="primary"] {
          background: linear-gradient(105deg, #008E92, #007481);
          color: white;
          border: 1px solid #24D3C3;
      }

      /* AfriMarket branded dashboard header */
      .afri-page-header {
          background: linear-gradient(110deg, #002138 0%, #003B4D 100%);
          border-radius: 16px;
          border-left: 5px solid #E9B44C;
          padding: 26px 32px;
          margin-bottom: 22px;
          box-shadow: 0 6px 20px rgba(0, 33, 56, 0.12);
      }

      .afri-page-header h1 {
          color: #FFFFFF !important;
          font-size: 32px;
          font-weight: 700;
          margin: 0 0 10px 0;
          padding: 0;
      }

      .afri-page-header p {
          color: #C9E0E5 !important;
          font-size: 15px;
          margin: 0;
      }

      /* Improve readability on the light dashboard */
      [data-testid="stMain"] h1,
      [data-testid="stMain"] h2,
      [data-testid="stMain"] h3 {
          color: #002138;
      }

      [data-testid="stMain"] [data-testid="stCaptionContainer"] {
          color: #475569 !important;
      }

      [data-testid="stMain"] label,
      [data-testid="stMain"] p {
          color: #334155;
      }

      /* Keep branded header text light */
      [data-testid="stMain"] .afri-page-header h1 {
          color: #FFFFFF !important;
      }

      [data-testid="stMain"] .afri-page-header p {
          color: #C9E0E5 !important;
      }

    </style>
    """,
    unsafe_allow_html=True,
)

COMPLAINT_STATUSES = ["Open", "Under Review", "Resolved"]
CASE_STATUSES = ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"]
ANALYST_DECISIONS = ["PENDING", "APPROVE", "DECLINE"]


def get_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "fraud"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
    )


def mysql_rows(query, params=None):
    connection = get_mysql_connection()
    try:
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params or ())
            return cursor.fetchall()
        finally:
            cursor.close()
    finally:
        connection.close()



# Shared reporting and analytics
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import altair as alt

REPORT_TZ = ZoneInfo("America/Chicago")
REPORT_PERIODS = ["Hour", "Day", "Week", "Month", "Year", "All Time"]

DECISION_COLORS = {
    "APPROVE": "#16A34A",
    "REVIEW": "#D97706",
    "BLOCK": "#DC2626",
}


def page_period_filter(page_name, default="All Time"):
    state_key = f"saved_period_{page_name}"
    widget_key = f"period_widget_{page_name}"

    if state_key not in st.session_state:
        st.session_state[state_key] = default

    st.session_state[widget_key] = st.session_state[state_key]

    def save_period():
        st.session_state[state_key] = st.session_state[widget_key]

    return st.radio(
        "Reporting period",
        REPORT_PERIODS,
        horizontal=True,
        key=widget_key,
        on_change=save_period,
    )


def monitoring_json(url, timeout=5):
    import json
    import urllib.request

    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def monitor_spark():
    return monitoring_json(
        "http://afrimarket-spark-master:8080/json/"
    )


def monitor_hdfs():
    result = monitoring_json(
        "http://hadoop:9870/jmx?"
        "qry=Hadoop:service=NameNode,name=FSNamesystemState"
    )

    return result["beans"][0]


def monitor_kafka():
    import socket
    import time

    started = time.perf_counter()

    with socket.create_connection(("kafka", 9092), timeout=5):
        latency = (time.perf_counter() - started) * 1000

    return {
        "reachable": True,
        "connection_ms": round(latency, 2),
    }


def monitor_mysql():
    import time

    started = time.perf_counter()

    scores = mysql_rows("""
        SELECT COUNT(*) AS total_scores,
               MAX(stored_at) AS latest_score
        FROM scores
    """)[0]

    cases = mysql_rows("""
        SELECT COUNT(*) AS total_cases
        FROM investigation_cases
    """)[0]

    return {
        "total_scores": int(scores["total_scores"]),
        "latest_score": scores["latest_score"],
        "total_cases": int(cases["total_cases"]),
        "query_ms": round((time.perf_counter() - started) * 1000, 2),
    }


def monitor_cassandra():
    import time

    started = time.perf_counter()
    cluster = cassandra_cluster()

    try:
        session = cluster.connect("afrimarket")

        row = session.execute(
            "SELECT release_version FROM system.local",
            timeout=5,
        ).one()

        return {
            "version": row.release_version,
            "query_ms": round(
                (time.perf_counter() - started) * 1000, 2
            ),
        }

    finally:
        cluster.shutdown()


def monitor_checkpoints():
    base = (
        "http://hadoop:9870/webhdfs/v1/fraud/checkpoints/"
        "member2_transaction_scoring"
    )

    def read_checkpoints(directory):
        result = monitoring_json(
            f"{base}/{directory}?op=LISTSTATUS",
            timeout=20,
        )

        return {
            int(item["pathSuffix"]): item["modificationTime"]
            for item in result["FileStatuses"]["FileStatus"]
            if item["type"] == "FILE"
            and item["pathSuffix"].isdigit()
        }

    commits = read_checkpoints("commits")
    offsets = read_checkpoints("offsets")

    latest_commit = max(commits, default=None)
    latest_offset = max(offsets, default=None)

    duration = None
    completed_at = None

    if latest_commit is not None:
        completed_at = pd.to_datetime(
            commits[latest_commit], unit="ms", utc=True
        ).tz_convert(REPORT_TZ)

        if latest_commit in offsets:
            duration = (
                commits[latest_commit] - offsets[latest_commit]
            ) / 1000

    return {
        "latest_commit": latest_commit,
        "latest_offset": latest_offset,
        "completed_at": completed_at,
        "duration_seconds": duration,
        "pending_batches": len(set(offsets) - set(commits)),
    }


def collect_system_health():
    checks = {
        "Spark": monitor_spark,
        "HDFS": monitor_hdfs,
        "Kafka": monitor_kafka,
        "MySQL": monitor_mysql,
        "Cassandra": monitor_cassandra,
        "Checkpoints": monitor_checkpoints,
    }

    results = {}

    for service, check in checks.items():
        try:
            results[service] = {
                "available": True,
                "data": check(),
                "error": None,
            }
        except Exception as error:
            results[service] = {
                "available": False,
                "data": {},
                "error": str(error),
            }

    return results


def reporting_bounds(period, now=None):
    now = now or datetime.now(REPORT_TZ)

    if period == "Hour":
        start = now - timedelta(hours=1)
    elif period == "Day":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "Week":
        start = now - timedelta(days=7)
    elif period == "Month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif period == "Year":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = None

    def utc_value(value):
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    return utc_value(start) if start else None, utc_value(now)


def reporting_where(bounds, column="stored_at"):
    start, end = bounds
    if start is None:
        return f" WHERE {column} < %s", (end,)
    return f" WHERE {column} >= %s AND {column} < %s", (start, end)


def transaction_summary(bounds):
    where, params = reporting_where(bounds)
    return mysql_rows("""
        SELECT COUNT(*) AS total,
               COALESCE(SUM(decision = 'APPROVE'), 0) AS approved,
               COALESCE(SUM(decision = 'REVIEW'), 0) AS review,
               COALESCE(SUM(decision = 'BLOCK'), 0) AS blocked,
               COALESCE(AVG(risk_score), 0) AS avg_risk,
               MAX(stored_at) AS latest_record
        FROM scores
    """ + where, params)[0]


def previous_period_summary(bounds):
    start, end = bounds
    if start is None:
        return None
    return transaction_summary((start - (end - start), start))


def percentage_change(current, previous):
    if previous is None or previous == 0:
        return None
    return f"{(current - previous) / previous:+.1%}"


def localize_timestamp(value):
    if value is None:
        return None
    return pd.to_datetime(value, utc=True).tz_convert(REPORT_TZ)


def transaction_risk_distribution(bounds):
    where, params = reporting_where(bounds)
    return pd.DataFrame(mysql_rows("""
        SELECT LEAST(FLOOR(risk_score * 5), 4) AS risk_band,
               COUNT(*) AS total
        FROM scores
    """ + where + " GROUP BY risk_band ORDER BY risk_band", params))


def transaction_trend(bounds, period):
    where, params = reporting_where(bounds)

    if period == "Hour":
        bucket = "%Y-%m-%d %H:%i:00"
    else:
        bucket = "%Y-%m-%d %H:00:00"

    rows = mysql_rows("""
        SELECT DATE_FORMAT(stored_at, '""" + bucket + """') AS bucket,
               COUNT(*) AS total,
               COALESCE(SUM(decision = 'APPROVE'), 0) AS approved,
               COALESCE(SUM(decision = 'REVIEW'), 0) AS review,
               COALESCE(SUM(decision = 'BLOCK'), 0) AS blocked
        FROM scores
    """ + where + " GROUP BY bucket ORDER BY bucket", params)

    trend = pd.DataFrame(rows)

    if trend.empty:
        return trend

    trend["bucket"] = pd.to_datetime(trend["bucket"], utc=True).dt.tz_convert(REPORT_TZ)

    frequencies = {
        "Hour": "min",
        "Day": "h",
        "Week": "D",
        "Month": "D",
        "Year": "MS",
        "All Time": "D",
    }

    trend = trend.set_index("bucket").resample(frequencies[period]).sum().reset_index()

    for column in ["total", "approved", "review", "blocked"]:
        trend[column] = trend[column].astype(int)

    trend["flagged"] = trend["review"] + trend["blocked"]
    trend["flagged_rate"] = trend["flagged"] / trend["total"].replace(0, float("nan"))

    return trend



def decision_donut(summary):
    data = pd.DataFrame({
        "Decision": ["APPROVE", "REVIEW", "BLOCK"],
        "Transactions": [
            int(summary["approved"]),
            int(summary["review"]),
            int(summary["blocked"]),
        ],
    })

    total = data["Transactions"].sum()
    data["Rate"] = data["Transactions"] / total if total else 0.0

    return alt.Chart(data).mark_arc(innerRadius=75, outerRadius=115).encode(
        theta=alt.Theta("Transactions:Q"),
        color=alt.Color(
            "Decision:N",
            scale=alt.Scale(
                domain=list(DECISION_COLORS),
                range=list(DECISION_COLORS.values()),
            ),
            legend=alt.Legend(orient="bottom", title=None),
        ),
        tooltip=[
            "Decision:N",
            alt.Tooltip("Transactions:Q", format=","),
            alt.Tooltip("Rate:Q", format=".1%"),
        ],
    ).properties(height=280)


def transaction_line_chart(trend):
    data = trend.copy()
    data["bucket"] = data["bucket"].dt.tz_localize(None)

    data = data.melt(
        id_vars=["bucket"],
        value_vars=["total", "approved", "review", "blocked"],
        var_name="Series",
        value_name="Transactions",
    )

    return alt.Chart(data).mark_line(point=True, strokeWidth=2.5).encode(
        x=alt.X("bucket:T", title="Time (Central)"),
        y=alt.Y("Transactions:Q", title="Transactions"),
        color=alt.Color(
            "Series:N",
            scale=alt.Scale(
                domain=["total", "approved", "review", "blocked"],
                range=["#008E92", "#16A34A", "#D97706", "#DC2626"],
            ),
            legend=alt.Legend(orient="bottom", title=None),
        ),
        tooltip=[
            alt.Tooltip("bucket:T", title="Time", format="%b %d, %H:%M"),
            "Series:N",
            alt.Tooltip("Transactions:Q", format=","),
        ],
    ).properties(height=300).interactive()


def risk_distribution_chart(data):
    bands = ["0.0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0"]

    counts = {
        int(row["risk_band"]): int(row["total"])
        for _, row in data.iterrows()
    }

    chart_data = pd.DataFrame({
        "Risk Band": bands,
        "Transactions": [counts.get(i, 0) for i in range(5)],
    })

    return alt.Chart(chart_data).mark_bar(
        cornerRadiusTopLeft=5,
        cornerRadiusTopRight=5,
    ).encode(
        x=alt.X("Risk Band:N", sort=bands, title="Risk score"),
        y=alt.Y("Transactions:Q"),
        color=alt.Color(
            "Risk Band:N",
            scale=alt.Scale(
                domain=bands,
                range=["#16A34A", "#65A30D", "#EAB308", "#D97706", "#DC2626"],
            ),
            legend=None,
        ),
        tooltip=["Risk Band:N", "Transactions:Q"],
    ).properties(height=280)


def flagged_rate_chart(trend):
    data = trend.copy()
    data["bucket"] = data["bucket"].dt.tz_localize(None)

    return alt.Chart(data).mark_line(
        color="#D97706",
        point=True,
        strokeWidth=3,
    ).encode(
        x=alt.X("bucket:T", title="Time (Central)"),
        y=alt.Y(
            "flagged_rate:Q",
            title="Flagged rate",
            axis=alt.Axis(format="%"),
            scale=alt.Scale(domain=[0, 1]),
        ),
        tooltip=[
            alt.Tooltip("bucket:T", title="Time", format="%b %d, %H:%M"),
            alt.Tooltip("flagged_rate:Q", title="Flagged rate", format=".1%"),
            alt.Tooltip("total:Q", title="Transactions"),
        ],
    ).properties(height=280).interactive()


def get_scores(bounds=None, decisions=None, search=""):
    bounds = bounds or reporting_bounds("All Time")
    where, params = reporting_where(bounds)
    params = list(params)

    if decisions is not None:
        if not decisions:
            return pd.DataFrame(columns=["TransactionID", "TransactionDT", "risk_score", "decision", "stored_at"])

        placeholders = ", ".join(["%s"] * len(decisions))
        where += f" AND decision IN ({placeholders})"
        params.extend(decisions)

    if search.strip():
        where += " AND LOCATE(%s, CAST(TransactionID AS CHAR)) > 0"
        params.append(search.strip())

    rows = mysql_rows("""
        SELECT TransactionID, TransactionDT, risk_score, decision, stored_at
        FROM scores
    """ + where + " ORDER BY stored_at DESC, TransactionID DESC LIMIT 1000", tuple(params))

    scores = pd.DataFrame(rows, columns=["TransactionID", "TransactionDT", "risk_score", "decision", "stored_at"])

    if not scores.empty:
        scores["stored_at"] = pd.to_datetime(scores["stored_at"], utc=True).dt.tz_convert(REPORT_TZ)

    return scores


def get_flagged_scores():
    return mysql_rows("""
        SELECT s.TransactionID, s.risk_score, s.decision
        FROM scores s
        WHERE s.decision IN ('REVIEW', 'BLOCK')
          AND NOT EXISTS (
              SELECT 1 FROM investigation_cases c
              WHERE c.TransactionID = s.TransactionID
                AND c.case_status IN ('OPEN', 'IN_PROGRESS')
          )
        ORDER BY s.stored_at DESC LIMIT 1000
    """)


def investigation_summary(bounds):
    where, params = reporting_where(bounds, "created_at")

    return mysql_rows("""
        SELECT COUNT(*) AS total,
               COALESCE(SUM(case_status = 'OPEN'), 0) AS open_cases,
               COALESCE(SUM(case_status = 'IN_PROGRESS'), 0) AS in_progress,
               COALESCE(SUM(case_status = 'RESOLVED'), 0) AS resolved,
               COALESCE(SUM(case_status = 'CLOSED'), 0) AS closed,
               COALESCE(SUM(analyst_decision = 'APPROVE'), 0) AS approved,
               COALESCE(SUM(analyst_decision = 'DECLINE'), 0) AS declined,
               COALESCE(SUM(analyst_decision = 'PENDING'), 0) AS pending,
               COALESCE(SUM(assigned_to IS NULL OR TRIM(assigned_to) = ''), 0) AS unassigned,
               AVG(CASE
                   WHEN case_status IN ('RESOLVED', 'CLOSED')
                        AND resolved_at IS NOT NULL
                   THEN TIMESTAMPDIFF(SECOND, created_at, resolved_at) / 3600.0
               END) AS avg_resolution_hours,
               COUNT(CASE
                   WHEN case_status IN ('RESOLVED', 'CLOSED')
                        AND resolved_at IS NOT NULL
                   THEN 1
               END) AS timed_cases
        FROM investigation_cases
    """ + where, params)[0]


def get_assigned_analysts():
    rows = mysql_rows("""
        SELECT DISTINCT assigned_to
        FROM investigation_cases
        WHERE assigned_to IS NOT NULL
          AND TRIM(assigned_to) <> ''
        ORDER BY assigned_to
    """)

    return [row["assigned_to"] for row in rows]


def get_investigations(bounds=None, statuses=None, analyst=None,
                       search="", priority=None, sort_by="Highest Risk"):

    bounds = bounds or reporting_bounds("All Time")
    where, params = reporting_where(bounds, "c.created_at")
    params = list(params)

    if statuses is not None:
        if not statuses:
            return pd.DataFrame()

        placeholders = ", ".join(["%s"] * len(statuses))
        where += f" AND c.case_status IN ({placeholders})"
        params.extend(statuses)

    if analyst == "Unassigned":
        where += " AND (c.assigned_to IS NULL OR TRIM(c.assigned_to) = '')"
    elif analyst and analyst != "All Analysts":
        where += " AND c.assigned_to = %s"
        params.append(analyst)

    if search.strip():
        where += """
            AND (LOCATE(%s, CAST(c.case_id AS CHAR)) > 0
                 OR LOCATE(%s, CAST(c.TransactionID AS CHAR)) > 0)
        """
        params.extend([search.strip(), search.strip()])

    if priority == "Critical":
        where += " AND s.decision = 'BLOCK'"
    elif priority == "High":
        where += " AND s.decision = 'REVIEW'"
    elif priority == "Standard":
        where += " AND s.decision NOT IN ('BLOCK', 'REVIEW')"

    sorting = {
        "Highest Risk": "s.risk_score DESC",
        "Oldest First": "c.created_at ASC",
        "Newest First": "c.created_at DESC",
        "Recently Updated": "c.updated_at DESC",
    }

    order = sorting.get(sort_by, sorting["Highest Risk"])

    rows = mysql_rows("""
        SELECT c.case_id, c.TransactionID, s.risk_score,
               s.decision, c.case_status, c.assigned_to, c.notes,
               c.analyst_decision, c.created_at, c.updated_at,
               c.resolved_at,
               CASE
                   WHEN s.decision = 'BLOCK' THEN 'Critical'
                   WHEN s.decision = 'REVIEW' THEN 'High'
                   ELSE 'Standard'
               END AS priority,
               ROUND(
                   TIMESTAMPDIFF(SECOND, c.created_at, UTC_TIMESTAMP()) / 3600.0,
                   1
               ) AS case_age_hours,
               CASE
                   WHEN c.resolved_at IS NOT NULL
                   THEN ROUND(
                       TIMESTAMPDIFF(SECOND, c.created_at, c.resolved_at) / 3600.0,
                       2
                   )
                   ELSE NULL
               END AS resolution_hours
        FROM investigation_cases c
        JOIN scores s ON c.TransactionID = s.TransactionID
    """ + where + f" ORDER BY {order}, c.case_id DESC LIMIT 500", tuple(params))

    cases = pd.DataFrame(rows)

    if not cases.empty:
        for column in ["created_at", "updated_at", "resolved_at"]:
            cases[column] = pd.to_datetime(
                cases[column], utc=True
            ).dt.tz_convert(REPORT_TZ)

    return cases


def get_analyst_summary():
    rows = mysql_rows("""
        SELECT COALESCE(analyst_decision, 'PENDING') AS analyst_decision,
               COUNT(*) AS total
        FROM investigation_cases GROUP BY analyst_decision
    """)
    return {row["analyst_decision"]: int(row["total"]) for row in rows}


def update_investigation(case_id, status, analyst, notes, analyst_decision):
    if status not in CASE_STATUSES or analyst_decision not in ANALYST_DECISIONS:
        raise ValueError("Invalid investigation status or analyst decision.")
    connection = get_mysql_connection()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("""
                UPDATE investigation_cases
                SET resolved_at = CASE
                        WHEN %s IN ('OPEN', 'IN_PROGRESS') THEN NULL
                        WHEN %s IN ('RESOLVED', 'CLOSED')
                             AND case_status IN ('OPEN', 'IN_PROGRESS')
                        THEN CURRENT_TIMESTAMP(3)
                        ELSE resolved_at
                    END,
                    case_status = %s, assigned_to = %s, notes = %s,
                    analyst_decision = %s
                WHERE case_id = %s
            """, (status, status, status, analyst or None, notes or None, analyst_decision, case_id))
            if cursor.rowcount == 0:
                # MySQL can return zero if the values are unchanged.
                cursor.execute("SELECT case_id FROM investigation_cases WHERE case_id = %s", (case_id,))
                if cursor.fetchone() is None:
                    raise ValueError("Investigation case was not found.")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
    finally:
        connection.close()


def create_investigation(transaction_id):
    connection = get_mysql_connection()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT decision FROM scores WHERE TransactionID = %s", (transaction_id,))
            transaction = cursor.fetchone()
            if transaction is None:
                raise ValueError("Transaction not found.")
            if transaction[0] not in ("REVIEW", "BLOCK"):
                raise ValueError("Only flagged transactions can be investigated.")
            cursor.execute("""
                SELECT case_id FROM investigation_cases
                WHERE TransactionID = %s AND case_status IN ('OPEN', 'IN_PROGRESS')
                LIMIT 1
            """, (transaction_id,))
            if cursor.fetchone():
                raise ValueError("An active investigation already exists.")
            cursor.execute("INSERT INTO investigation_cases (TransactionID) VALUES (%s)", (transaction_id,))
            connection.commit()
            return cursor.lastrowid
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
    finally:
        connection.close()


def cassandra_cluster():
    return Cluster(
        [os.getenv("CASSANDRA_HOST", "cassandra")],
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        connect_timeout=10,
    )


def normalize_complaint_status(value):
    normalized = str(value or "").strip().lower()
    return {"open": "Open", "under review": "Under Review", "resolved": "Resolved"}.get(normalized, "Open")


def get_complaints(search_type, search_value):
    cluster = cassandra_cluster()
    try:
        session = cluster.connect("afrimarket")
        session.row_factory = dict_factory
        if search_type == "Complaint ID":
            query = """
                SELECT complaint_id, transaction_id, complaint_date,
                       complaint_category, complaint_text, complaint_status
                FROM complaints_by_id WHERE complaint_id = ?
            """
            value = search_value.strip()
        else:
            if not search_value.strip().isdigit():
                raise ValueError("Transaction ID must contain only numbers.")
            query = """
                SELECT complaint_id, transaction_id, complaint_date,
                       complaint_category, complaint_text, complaint_status
                FROM complaints_by_transaction WHERE transaction_id = ?
            """
            value = int(search_value.strip())
        statement = session.prepare(query)
        return list(session.execute(statement, (value,)))
    finally:
        cluster.shutdown()


def complaint_analytics_data(complaints):
    columns = [
        "complaint_id", "transaction_id", "complaint_date",
        "complaint_category", "complaint_text", "complaint_status",
    ]

    data = pd.DataFrame(complaints, columns=columns)

    data["complaint_timestamp"] = pd.to_datetime(
        data["complaint_date"],
        format="mixed",
        errors="coerce",
        utc=True,
    ).dt.tz_convert(REPORT_TZ)

    data["status"] = data["complaint_status"].map(
        normalize_complaint_status
    )

    data["category"] = data["complaint_category"].fillna("Unknown")

    transaction_ids = sorted({
        int(value)
        for value in data["transaction_id"].dropna()
    })

    score_rows = []

    for start in range(0, len(transaction_ids), 500):
        batch = transaction_ids[start:start + 500]
        placeholders = ", ".join(["%s"] * len(batch))

        score_rows.extend(mysql_rows(
            "SELECT TransactionID, risk_score, decision "
            "FROM scores WHERE TransactionID IN (" + placeholders + ")",
            tuple(batch),
        ))

    score_map = {
        int(row["TransactionID"]): row
        for row in score_rows
    }

    data["model_decision"] = data["transaction_id"].map(
        lambda value: score_map.get(int(value), {}).get("decision", "UNSCORED")
        if pd.notna(value) else "UNSCORED"
    )

    data["risk_score"] = data["transaction_id"].map(
        lambda value: score_map.get(int(value), {}).get("risk_score")
        if pd.notna(value) else None
    )

    return data


def filter_complaint_period(data, bounds):
    start, end = bounds

    if start is None:
        return data[
            data["complaint_timestamp"].isna()
            | (data["complaint_timestamp"] < pd.Timestamp(end, tz="UTC"))
        ].copy()

    return data[
        (data["complaint_timestamp"] >= pd.Timestamp(start, tz="UTC"))
        & (data["complaint_timestamp"] < pd.Timestamp(end, tz="UTC"))
    ].copy()


def complaint_volume_chart(data, period):
    dated = data.dropna(subset=["complaint_timestamp"]).copy()

    if dated.empty:
        return None

    frequencies = {
        "Hour": "min",
        "Day": "h",
        "Week": "D",
        "Month": "D",
        "Year": "MS",
        "All Time": "D",
    }

    trend = (
        dated.set_index("complaint_timestamp")
        .resample(frequencies[period])
        .size()
        .reset_index(name="Complaints")
    )

    trend["Date"] = trend["complaint_timestamp"].dt.tz_localize(None)

    return alt.Chart(trend).mark_line(
        color="#008E92",
        point=True,
        strokeWidth=3,
    ).encode(
        x=alt.X("Date:T", title="Reporting Time"),
        y=alt.Y("Complaints:Q", title="Complaint Volume"),
        tooltip=[
            alt.Tooltip("Date:T", title="Date"),
            alt.Tooltip("Complaints:Q"),
        ],
    ).properties(height=280).interactive()


def complaint_category_chart(data):
    categories = (
        data["category"]
        .value_counts()
        .rename_axis("Category")
        .reset_index(name="Complaints")
    )

    return alt.Chart(categories).mark_bar(
        cornerRadiusEnd=5,
    ).encode(
        y=alt.Y("Category:N", sort="-x", title=None),
        x=alt.X("Complaints:Q"),
        color=alt.Color(
            "Category:N",
            scale=alt.Scale(scheme="tableau10"),
            legend=None,
        ),
        tooltip=["Category:N", "Complaints:Q"],
    ).properties(height=max(250, len(categories) * 35))


def list_complaints():
    """Small-project list; a production Cassandra deployment needs indexed/status-specific tables."""
    cluster = cassandra_cluster()
    try:
        session = cluster.connect("afrimarket")
        session.row_factory = dict_factory
        statement = session.prepare("""
            SELECT complaint_id, transaction_id, complaint_date,
                   complaint_category, complaint_text, complaint_status
            FROM complaints_by_id
        """)
        statement.fetch_size = 500
        # The sample dataset has ~1,200 complaints; don't silently truncate the list.
        return list(session.execute(statement))
    finally:
        cluster.shutdown()


def update_complaint_status(complaint_id, transaction_id, new_status):
    if new_status not in COMPLAINT_STATUSES:
        raise ValueError("Invalid complaint status.")
    cluster = cassandra_cluster()
    try:
        session = cluster.connect("afrimarket")
        # Verify the source row exists before updating: Cassandra UPDATE is an upsert.
        lookup = session.prepare("SELECT transaction_id FROM complaints_by_id WHERE complaint_id = ?")
        existing = session.execute(lookup, (complaint_id,)).one()
        if existing is None or existing.transaction_id != int(transaction_id):
            raise ValueError("Complaint not found or transaction ID mismatch.")
        by_id = session.prepare("""
            UPDATE complaints_by_id SET complaint_status = ? WHERE complaint_id = ?
        """)
        by_transaction = session.prepare("""
            UPDATE complaints_by_transaction SET complaint_status = ?
            WHERE transaction_id = ? AND complaint_id = ?
        """)
        # A logged batch keeps the two denormalized copies together on retry.
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(by_id, (new_status, complaint_id))
        batch.add(by_transaction, (new_status, int(transaction_id), complaint_id))
        session.execute(batch)
    finally:
        cluster.shutdown()


def get_management_summary():
    return mysql_rows("""
        SELECT COUNT(*) AS total,
               COALESCE(SUM(decision = 'APPROVE'), 0) AS approved,
               COALESCE(SUM(decision = 'REVIEW'), 0) AS review,
               COALESCE(SUM(decision = 'BLOCK'), 0) AS blocked,
               COALESCE(AVG(risk_score), 0) AS avg_risk
        FROM scores
    """)[0]


def get_case_summary():
    return mysql_rows("""
        SELECT case_status, COUNT(*) AS total
        FROM investigation_cases GROUP BY case_status
    """)


def show_complaint(complaint, editable=False, prefix="complaint"):
    complaint_id = complaint["complaint_id"]
    current = normalize_complaint_status(complaint.get("complaint_status"))
    with st.container(border=True):
        st.subheader(f"Complaint #{complaint_id}")
        left, right = st.columns(2)
        left.write(f"**Transaction ID:** {complaint['transaction_id']}")
        left.write(f"**Category:** {complaint['complaint_category']}")
        right.write(f"**Date:** {complaint['complaint_date']}")
        right.write(f"**Status:** {current}")
        st.markdown("**Customer Complaint**")
        st.write(complaint["complaint_text"])
        if editable:
            with st.form(f"{prefix}_form_{complaint_id}"):
                new_status = st.selectbox(
                    "Update complaint status", COMPLAINT_STATUSES,
                    index=COMPLAINT_STATUSES.index(current),
                    key=f"{prefix}_status_{complaint_id}_{current}",
                )
                saved = st.form_submit_button("Save Changes", type="primary")
            if saved:
                try:
                    update_complaint_status(complaint_id, complaint["transaction_id"], new_status)
                    # Clear search selection so the page returns to the appropriate queue.
                    st.session_state.pop("complaint_search", None)
                    st.session_state["complaint_flash"] = f"Complaint #{complaint_id} updated to {new_status}."
                    st.rerun()
                except Exception as error:
                    st.error(f"Unable to update complaint: {error}")


logo_path = Path(__file__).with_name("afrimarket_logo.png")
with st.sidebar:
    if logo_path.is_file():
        st.image(str(logo_path), width=280)
    else:
        st.markdown("## 🛡️ AfriMarket USA")
    st.markdown(
        '<p style="color:#A8CFD5;font-size:0.82rem;letter-spacing:0.04em;'
        'margin-top:-0.4rem;">FRAUD INTELLIGENCE PLATFORM</p>',
        unsafe_allow_html=True,
    )
    st.divider()
    navigation = {
        "Overview": "Overview",
        "Investigations": "Investigations",
        "Complaints": "Complaints",
        "Management": "Management",
        "System Monitoring": "System Monitoring",
    }

    if "active_page" not in st.session_state:
        st.session_state["active_page"] = "Overview"

    for menu_label, menu_page in navigation.items():
        if st.button(
            menu_label,
            key=f"nav_{menu_page}",
            use_container_width=True,
            type="primary" if st.session_state["active_page"] == menu_page else "secondary",
        ):
            st.session_state["active_page"] = menu_page
            st.rerun()

    page = st.session_state["active_page"]

    st.divider()
    st.markdown(
        '<div style="display:inline-block; padding:12px 20px; '
        'background:#063B4D; border:1px solid #168D91; '
        'border-radius:30px; color:white; font-size:15px;">'
        '<span style="color:#2DD4A7;">●</span> '
        'Fraud Monitoring</div>'
        '<p style="color:#B8D1DA; font-size:13px; margin-top:14px;">'
        'Real-Time Fraud Intelligence</p>',
        unsafe_allow_html=True,
    )


if page == "Overview":
    st.markdown(
        """
        <div class="afri-page-header">
            <h1>Fraud Monitoring Overview</h1>
            <p>Real-time transaction intelligence and risk monitoring</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    controls = st.columns([3, 1, 1])

    with controls[0]:
        period = page_period_filter("overview", "Day")

    with controls[1]:
        auto_refresh = st.toggle("Auto refresh", value=False)

    with controls[2]:
        if st.button("↻ Refresh transactions"):
            st.rerun()

    @st.fragment(run_every=30 if auto_refresh else None)
    def render_overview():
        try:
            now = datetime.now(REPORT_TZ)
            bounds = reporting_bounds(period, now)
            summary = transaction_summary(bounds)
            all_time = transaction_summary(reporting_bounds("All Time", now))
            previous = previous_period_summary(bounds)

            total = int(summary["total"])
            approved = int(summary["approved"])
            review = int(summary["review"])
            blocked = int(summary["blocked"])
            flagged = review + blocked
            avg_risk = float(summary["avg_risk"])
            flagged_rate = flagged / total if total else 0

            st.caption(
                f"Dashboard updated: {now:%b %d, %Y %I:%M:%S %p %Z}"
                " | Reporting time: Central Time"
            )

            latest = localize_timestamp(all_time["latest_record"])

            if latest is not None:
                st.caption(f"Latest scored transaction: {latest:%b %d, %Y %I:%M:%S %p %Z}")

            period_labels = {
                "Hour": "Transactions — Last Hour",
                "Day": "Transactions Today",
                "Week": "Transactions — Last 7 Days",
                "Month": "Transactions This Month",
                "Year": "Transactions This Year",
                "All Time": "Transactions — All Time",
            }

            c1, c2, c3 = st.columns(3)

            c1.metric(
                ":material/database: Total Transactions",
                f"{int(all_time['total']):,}",
                help="All scored transactions, regardless of reporting period.",
            )

            c2.metric(
                period_labels[period],
                f"{total:,}",
                delta=percentage_change(total, int(previous["total"])) if previous else None,
                delta_color="off",
                help="Compared with the immediately preceding interval of equal duration.",
            )

            c3.metric(
                ":material/percent: Flagged Rate",
                f"{flagged_rate:.1%}" if total else "N/A",
                help="Percentage of transactions classified as REVIEW or BLOCK.",
            )

            c4, c5, c6 = st.columns(3)

            c4.metric(":material/check_circle: Model Approved", f"{approved:,}")
            c5.metric(":material/search: Needs Review", f"{review:,}")
            c6.metric(":material/shield: Model Blocked", f"{blocked:,}")

            st.divider()

            if total:
                st.subheader("Transaction Activity")
                trend = transaction_trend(bounds, period)

                if not trend.empty:
                    st.altair_chart(transaction_line_chart(trend), use_container_width=True)

                left, right = st.columns(2)

                with left:
                    st.subheader("Model Decision Distribution")
                    st.altair_chart(decision_donut(summary), use_container_width=True)

                with right:
                    st.subheader("Risk Score Distribution")
                    risk_data = transaction_risk_distribution(bounds)
                    st.altair_chart(risk_distribution_chart(risk_data), use_container_width=True)

                st.subheader("Risk Summary")

                r1, r2, r3 = st.columns(3)
                r1.metric("Average Risk Score", f"{avg_risk:.1%}")
                r2.metric("Flagged Transactions", f"{flagged:,}")
                r3.metric("Flagged Rate", f"{flagged_rate:.1%}")

                if not trend.empty:
                    st.subheader("Flagged Rate Trend")
                    st.altair_chart(flagged_rate_chart(trend), use_container_width=True)

            else:
                st.info("No transactions were recorded during the selected period.")

            st.divider()
            st.subheader("Transaction Records")

            left, right = st.columns(2)

            with left:
                search = st.text_input(
                    "Search Transaction ID",
                    placeholder="Enter a transaction ID...",
                    key="overview_search",
                )

            with right:
                decisions = st.multiselect(
                    "Filter by model decision",
                    ["APPROVE", "REVIEW", "BLOCK"],
                    default=["APPROVE", "REVIEW", "BLOCK"],
                    key="overview_decisions",
                )

            scores = get_scores(bounds, decisions, search)

            if scores.empty:
                st.info("No transactions match the selected filters.")

            else:
                st.dataframe(
                    scores,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "TransactionID": st.column_config.NumberColumn(
                            "Transaction ID", format="%d"
                        ),
                        "TransactionDT": st.column_config.NumberColumn(
                            "Transaction Time (relative seconds)"
                        ),
                        "risk_score": st.column_config.ProgressColumn(
                            "Risk Score", min_value=0, max_value=1, format="%.2f"
                        ),
                        "decision": "Model Decision",
                        "stored_at": st.column_config.DatetimeColumn(
                            "Recorded At (Central)",
                            format="MMM DD, YYYY hh:mm:ss a",
                            timezone="America/Chicago",
                        ),
                    },
                )

                st.download_button(
                    "Download displayed transactions (CSV)",
                    data=scores.to_csv(index=False).encode("utf-8"),
                    file_name=f"afrimarket_transactions_{now:%Y%m%d_%H%M%S}.csv",
                    mime="text/csv",
                )

            st.caption(
                f"Displaying {len(scores):,} matching records, up to 1,000. "
                "Summary metrics and charts include all transactions in the selected period. "
                "Search and decision filters apply to the transaction table."
            )

            st.caption(
                "Model decisions and risk scores are predictions, not confirmed fraud outcomes."
            )

        except mysql.connector.Error as error:
            st.error(f"Unable to retrieve transactions from MySQL: {error}")

    render_overview()


elif page == "Investigations":
    st.title("Investigation Center")
    st.caption("Review flagged transactions and record analyst decisions")
    try:
        # This is outside the cases.empty branch, so the first case can be created.
        with st.expander("➕ Create New Investigation"):
            flagged_scores = get_flagged_scores()
            if not flagged_scores:
                st.info("No flagged transactions are available for a new active investigation.")
            else:
                transaction_ids = [int(row["TransactionID"]) for row in flagged_scores]
                with st.form("create_case_form"):
                    transaction_id = st.selectbox(
                        "Select a flagged transaction", transaction_ids,
                        format_func=lambda x: f"Transaction #{x}",
                    )
                    create_clicked = st.form_submit_button("Create Investigation", type="primary")
                if create_clicked:
                    try:
                        new_case_id = create_investigation(transaction_id)
                        st.session_state["investigation_flash"] = f"Investigation #{new_case_id} created."
                        st.rerun()
                    except (mysql.connector.Error, ValueError) as error:
                        st.error(str(error))

        if st.session_state.pop("investigation_flash", None):
            st.success("Investigation created successfully.")

        controls = st.columns([4, 1])

        with controls[0]:
            period = page_period_filter("investigation", "All Time")

        with controls[1]:
            if st.button("Refresh Investigations"):
                st.rerun()

        now = datetime.now(REPORT_TZ)
        bounds = reporting_bounds(period, now)
        summary = investigation_summary(bounds)

        st.caption(f"Dashboard updated: {now:%b %d, %Y %I:%M:%S %p %Z}")

        st.subheader("Investigation Performance")

        total = int(summary["total"])
        active = int(summary["open_cases"]) + int(summary["in_progress"])
        completed = int(summary["resolved"]) + int(summary["closed"])

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Investigations", f"{total:,}")
        c2.metric("Active Investigations", f"{active:,}")
        c3.metric("Completed Investigations", f"{completed:,}")

        c4, c5, c6 = st.columns(3)
        c4.metric("Unassigned Cases", int(summary["unassigned"]))
        c5.metric("Completion Rate", f"{completed / total:.1%}" if total else "N/A")

        avg_hours = summary["avg_resolution_hours"]
        c6.metric(
            "Average Completion Time",
            f"{float(avg_hours):.1f} hrs" if avg_hours is not None else "N/A",
        )

        st.caption(
            f"Completion time is based on {int(summary['timed_cases'])} cases "
            "with recorded completion timestamps."
        )

        st.subheader("Investigation Workload")

        w1, w2, w3, w4 = st.columns(4)
        w1.metric("Open", int(summary["open_cases"]))
        w2.metric("In Progress", int(summary["in_progress"]))
        w3.metric("Resolved", int(summary["resolved"]))
        w4.metric("Closed", int(summary["closed"]))

        workload = pd.DataFrame({
            "Status": ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"],
            "Cases": [
                int(summary["open_cases"]),
                int(summary["in_progress"]),
                int(summary["resolved"]),
                int(summary["closed"]),
            ],
        })

        workload_chart = alt.Chart(workload).mark_bar(
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5,
        ).encode(
            x=alt.X("Status:N", sort=CASE_STATUSES),
            y=alt.Y("Cases:Q"),
            color=alt.Color(
                "Status:N",
                scale=alt.Scale(
                    domain=["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"],
                    range=["#D97706", "#008E92", "#16A34A", "#64748B"],
                ),
                legend=None,
            ),
            tooltip=["Status:N", "Cases:Q"],
        ).properties(height=250)

        st.altair_chart(workload_chart, use_container_width=True)

        st.subheader("Analyst Review Summary")

        r1, r2, r3 = st.columns(3)
        r1.metric("Approved", int(summary["approved"]))
        r2.metric("Declined", int(summary["declined"]))
        r3.metric("Pending Reviews", int(summary["pending"]))

        st.caption(
            "Statistics reflect the current status of cases created "
            "during the selected reporting period."
        )

        st.divider()
        st.subheader("Investigation Queue")

        f1, f2 = st.columns(2)

        with f1:
            search = st.text_input(
                "Search case or transaction ID",
                key="investigation_search",
            )

            status_filter = st.multiselect(
                "Case Status",
                CASE_STATUSES,
                default=CASE_STATUSES,
            )

        with f2:
            analyst_filter = st.selectbox(
                "Assigned Analyst",
                ["All Analysts", "Unassigned"] + get_assigned_analysts(),
            )

            priority_filter = st.selectbox(
                "Case Priority",
                ["All Priorities", "Critical", "High", "Standard"],
            )

        sort_by = st.selectbox(
            "Sort investigations by",
            ["Highest Risk", "Oldest First", "Newest First", "Recently Updated"],
        )

        filtered = get_investigations(
            bounds=bounds,
            statuses=status_filter,
            analyst=analyst_filter,
            search=search,
            priority=priority_filter,
            sort_by=sort_by,
        )

        if filtered.empty:
            st.info("No investigation cases match the selected filters.")
        else:
            queue = filtered[[
                "case_id", "TransactionID", "priority",
                "risk_score", "decision", "analyst_decision",
                "case_status", "assigned_to", "case_age_hours",
                "resolution_hours", "updated_at",
            ]].copy()

            def priority_style(value):
                colors = {
                    "Critical": "#DC2626",
                    "High": "#D97706",
                    "Standard": "#64748B",
                }
                return f"color: {colors.get(value, '#64748B')}; font-weight: bold"

            def status_style(value):
                colors = {
                    "OPEN": "#D97706",
                    "IN_PROGRESS": "#008E92",
                    "RESOLVED": "#16A34A",
                    "CLOSED": "#64748B",
                }
                return f"color: {colors.get(value, '#64748B')}; font-weight: bold"

            styled_queue = queue.style.map(
                priority_style, subset=["priority"]
            ).map(
                status_style, subset=["case_status"]
            )

            st.dataframe(
                styled_queue,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "case_id": st.column_config.NumberColumn("Case ID", format="%d"),
                    "TransactionID": st.column_config.NumberColumn("Transaction ID", format="%d"),
                    "priority": "Priority",
                    "risk_score": st.column_config.ProgressColumn(
                        "Risk Score", min_value=0, max_value=1, format="percent"
                    ),
                    "decision": "Model Decision",
                    "analyst_decision": "Analyst Decision",
                    "case_status": "Status",
                    "assigned_to": "Assigned Analyst",
                    "case_age_hours": st.column_config.NumberColumn(
                        "Case Age (hrs)", format="%.1f"
                    ),
                    "resolution_hours": st.column_config.NumberColumn(
                        "Completion Time (hrs)", format="%.2f"
                    ),
                    "updated_at": st.column_config.DatetimeColumn(
                        "Last Updated (Central)",
                        format="MMM DD, YYYY hh:mm a",
                        timezone="America/Chicago",
                    ),
                },
            )

            st.caption(f"Displaying {len(filtered):,} matching cases, up to 500.")

            st.download_button(
                "Download Investigation Queue (CSV)",
                data=queue.to_csv(index=False).encode("utf-8"),
                file_name=f"afrimarket_investigations_{now:%Y%m%d_%H%M%S}.csv",
                mime="text/csv",
            )

            st.divider()
            st.subheader("Case Details")
            if filtered.empty:
                st.info("No cases match the selected filter.")
            else:
                case_ids = filtered["case_id"].tolist()
                selected_id = st.selectbox(
                    "Select an investigation case", case_ids,
                    format_func=lambda x: f"Case #{x}",
                )
                selected_case = filtered[filtered["case_id"] == selected_id].iloc[0]
                left, right = st.columns(2)
                left.write(f"**Transaction ID:** {selected_case['TransactionID']}")
                left.write(f"**Risk Score:** {selected_case['risk_score']:.1%}")
                left.write(f"**Model Decision:** {selected_case['decision']}")
                right.write(f"**Created:** {selected_case['created_at']}")
                right.write(f"**Last Updated:** {selected_case['updated_at']}")

                st.subheader("Related Customer Complaints")
                try:
                    related_complaints = get_complaints(
                        "Transaction ID", str(int(selected_case["TransactionID"]))
                    )
                    if related_complaints:
                        for complaint in related_complaints:
                            with st.expander(
                                f"Complaint #{complaint['complaint_id']} — {complaint['complaint_category']}"
                            ):
                                show_complaint(complaint, editable=normalize_complaint_status(complaint["complaint_status"]) != "Resolved", prefix=f"case_{selected_id}")
                        st.caption("Complaint statuses are managed here, separately from the analyst fraud decision.")
                    else:
                        st.info("No customer complaints are linked to this transaction.")
                except Exception:
                    st.warning("Related complaints are temporarily unavailable. You can still manage this case.")

                with st.form(f"investigation_form_{selected_id}"):
                    status = st.selectbox(
                        "Case Status", CASE_STATUSES,
                        index=CASE_STATUSES.index(selected_case["case_status"]),
                    )
                    analyst = st.text_input(
                        "Assigned Analyst", value=selected_case["assigned_to"] or ""
                    )
                    notes = st.text_area(
                        "Investigation Notes", value=selected_case["notes"] or "", height=140
                    )
                    current_decision = selected_case["analyst_decision"] or "PENDING"
                    analyst_decision = st.selectbox(
                        "Analyst Decision", ANALYST_DECISIONS,
                        index=ANALYST_DECISIONS.index(current_decision),
                    )
                    submitted = st.form_submit_button("Save Investigation", type="primary")
                if submitted:
                    try:
                        update_investigation(
                            int(selected_id), status, analyst.strip(), notes.strip(), analyst_decision
                        )
                        st.success("Investigation updated successfully!")
                        st.rerun()
                    except (mysql.connector.Error, ValueError) as error:
                        st.error(f"Unable to save investigation: {error}")
            st.caption("Case creation, analyst decisions and complaint management remain independent of model predictions.")
    except (mysql.connector.Error, ValueError) as error:
        st.error(f"Unable to manage investigations: {error}")


elif page == "Complaints":
    st.title("Customer Complaints")
    st.caption("View active complaints, resolved history, and complaints linked to transactions")
    if "complaint_flash" in st.session_state:
        st.success(st.session_state.pop("complaint_flash"))

    controls = st.columns([4, 1])

    with controls[0]:
        period = page_period_filter("complaints", "All Time")

    with controls[1]:
        if st.button("Refresh Complaints"):
            st.rerun()

    try:
        complaints = list_complaints()
        data = complaint_analytics_data(complaints)

        now = datetime.now(REPORT_TZ)
        bounds = reporting_bounds(period, now)
        selected = filter_complaint_period(data, bounds)

        total = len(selected)
        resolved_count = int((selected["status"] == "Resolved").sum())
        active_count = total - resolved_count

        flagged_count = int(
            selected["model_decision"].isin(["REVIEW", "BLOCK"]).sum()
        )

        unscored_count = int(
            (selected["model_decision"] == "UNSCORED").sum()
        )

        st.caption(f"Dashboard updated: {now:%b %d, %Y %I:%M:%S %p %Z}")

        st.subheader("Complaint Performance")

        c1, c2, c3 = st.columns(3)

        c1.metric("Total Complaints", f"{len(data):,}")
        c2.metric("Complaints in Selected Period", f"{total:,}")
        c3.metric("Active Complaints", f"{active_count:,}")

        c4, c5, c6 = st.columns(3)

        c4.metric("Resolved Complaints", f"{resolved_count:,}")
        c5.metric(
            "Resolution Rate",
            f"{resolved_count / total:.1%}" if total else "N/A",
        )
        c6.metric("Complaints Linked to Flagged Transactions", flagged_count)

        st.caption(
            "Resolution rate reflects the current status of complaints "
            "dated within the selected reporting period."
        )

        st.divider()

        if total:
            st.subheader("Complaint Volume Trend")

            volume_chart = complaint_volume_chart(selected, period)

            if volume_chart is not None:
                st.altair_chart(volume_chart, use_container_width=True)
            else:
                st.info("No valid complaint dates are available for this chart.")

            left, right = st.columns(2)

            with left:
                st.subheader("Complaint Categories")
                st.altair_chart(
                    complaint_category_chart(selected),
                    use_container_width=True,
                )

            with right:
                st.subheader("Complaint Status Distribution")

                status_data = (
                    selected["status"]
                    .value_counts()
                    .rename_axis("Status")
                    .reset_index(name="Complaints")
                )

                status_chart = alt.Chart(status_data).mark_arc(
                    innerRadius=65,
                    outerRadius=105,
                ).encode(
                    theta=alt.Theta("Complaints:Q"),
                    color=alt.Color(
                        "Status:N",
                        scale=alt.Scale(
                            domain=["Open", "Under Review", "Resolved"],
                            range=["#D97706", "#008E92", "#16A34A"],
                        ),
                    ),
                    tooltip=["Status:N", "Complaints:Q"],
                ).properties(height=280)

                st.altair_chart(status_chart, use_container_width=True)

        else:
            st.info("No complaints match the selected reporting period.")

        st.divider()
        st.subheader("Linked Transaction Risk")

        scored_count = total - unscored_count

        r1, r2, r3 = st.columns(3)

        r1.metric("Complaints with Scored Transactions", scored_count)
        r2.metric("Complaints with Unscored Transactions", unscored_count)
        r3.metric(
            "Flagged Rate Among Scored Complaints",
            f"{flagged_count / scored_count:.1%}" if scored_count else "N/A",
        )

        if total:
            risk_data = pd.DataFrame({
                "Decision": ["APPROVE", "REVIEW", "BLOCK", "UNSCORED"],
                "Complaints": [
                    int((selected["model_decision"] == decision).sum())
                    for decision in ["APPROVE", "REVIEW", "BLOCK", "UNSCORED"]
                ],
            })

            risk_chart = alt.Chart(risk_data).mark_bar(
                cornerRadiusTopLeft=5,
                cornerRadiusTopRight=5,
            ).encode(
                x=alt.X(
                    "Decision:N",
                    sort=["APPROVE", "REVIEW", "BLOCK", "UNSCORED"],
                ),
                y=alt.Y("Complaints:Q"),
                color=alt.Color(
                    "Decision:N",
                    scale=alt.Scale(
                        domain=["APPROVE", "REVIEW", "BLOCK", "UNSCORED"],
                        range=["#16A34A", "#D97706", "#DC2626", "#64748B"],
                    ),
                    legend=None,
                ),
                tooltip=["Decision:N", "Complaints:Q"],
            ).properties(height=260)

            st.altair_chart(risk_chart, use_container_width=True)

        st.caption(
            "Complaint records are retrieved from Cassandra and matched "
            "to Spark-generated transaction scores stored in MySQL. "
            "Unscored transactions are not classified as safe."
        )

        st.divider()
        st.subheader("Complaint Records")

        f1, f2, f3 = st.columns(3)

        with f1:
            category_filter = st.selectbox(
                "Complaint Category",
                ["All Categories"] + sorted(selected["category"].unique().tolist()),
            )

        with f2:
            risk_filter = st.selectbox(
                "Model Decision",
                ["All Decisions", "APPROVE", "REVIEW", "BLOCK", "UNSCORED"],
            )

        with f3:
            status_filter = st.selectbox(
                "Complaint Status",
                ["All Statuses"] + sorted(selected["status"].dropna().unique().tolist()),
            )

        filtered = selected.copy()

        if category_filter != "All Categories":
            filtered = filtered[filtered["category"] == category_filter]

        if risk_filter != "All Decisions":
            filtered = filtered[filtered["model_decision"] == risk_filter]

        if status_filter != "All Statuses":
            filtered = filtered[filtered["status"] == status_filter]

        filtered = filtered.sort_values(
            "complaint_timestamp",
            ascending=False,
            na_position="last",
        )

        st.caption(f"{len(filtered):,} complaints match the selected filters.")

        if not filtered.empty:
            report = filtered[[
                "complaint_id", "transaction_id", "complaint_date",
                "category", "status", "model_decision", "risk_score",
            ]].copy()

            report["risk_score"] = pd.to_numeric(
                report["risk_score"], errors="coerce"
            )

            st.dataframe(
                report,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "complaint_id": "Complaint ID",
                    "transaction_id": st.column_config.NumberColumn(
                        "Transaction ID", format="%d"
                    ),
                    "complaint_date": "Complaint Date",
                    "category": "Category",
                    "status": "Status",
                    "model_decision": "Model Decision",
                    "risk_score": st.column_config.ProgressColumn(
                        "Transaction Risk",
                        min_value=0,
                        max_value=1,
                        format="percent",
                    ),
                },
            )

            st.download_button(
                "Download Complaint Report (CSV)",
                data=report.to_csv(index=False).encode("utf-8"),
                file_name=f"afrimarket_complaints_{now:%Y%m%d_%H%M%S}.csv",
                mime="text/csv",
            )

        matching_ids = set(filtered["complaint_id"])

        active = [
            c for c in complaints
            if c["complaint_id"] in matching_ids
            and normalize_complaint_status(c["complaint_status"]) != "Resolved"
        ]

        resolved = [
            c for c in complaints
            if c["complaint_id"] in matching_ids
            and normalize_complaint_status(c["complaint_status"]) == "Resolved"
        ]

        active.sort(
            key=lambda c: (str(c.get("complaint_date") or ""), c["complaint_id"]),
            reverse=True,
        )

        resolved.sort(
            key=lambda c: (str(c.get("complaint_date") or ""), c["complaint_id"]),
            reverse=True,
        )

        active_tab, resolved_tab, search_tab = st.tabs(
            ["Active Complaints", "Resolved History", "Search Complaints"]
        )

        with active_tab:
            st.metric("Active Complaints", len(active))
            st.caption("Showing up to 50 matching active complaints. Use Search Complaints to find older records.")
            if not active:
                st.info("No open or under-review complaints found.")
            else:
                for complaint in active[:50]:
                    show_complaint(complaint, editable=False, prefix="active")

        with resolved_tab:
            st.metric("Resolved Complaints", len(resolved))
            st.caption("Showing up to 50 matching resolved complaints. Use Search Complaints to find older records.")
            if not resolved:
                st.info("No resolved complaints found.")
            else:
                for complaint in resolved[:50]:
                    show_complaint(complaint, editable=False, prefix="resolved")

        with search_tab:
            st.caption("Direct search covers all complaint records, independently of reporting filters.")
            with st.form("complaint_search_form"):
                search_type = st.radio(
                    "Search complaints by", ["Transaction ID", "Complaint ID"], horizontal=True
                )
                search_value = st.text_input(
                    "Enter search value",
                    placeholder="100002" if search_type == "Transaction ID" else "TEST001",
                )
                search_clicked = st.form_submit_button("Search Complaints", type="primary")
            if search_clicked:
                if not search_value.strip():
                    st.warning("Please enter a search value.")
                    st.session_state.pop("complaint_search", None)
                else:
                    st.session_state["complaint_search"] = (search_type, search_value.strip())
            saved_search = st.session_state.get("complaint_search")
            if saved_search:
                try:
                    results = get_complaints(*saved_search)
                    if not results:
                        st.info("No complaints found for this search.")
                    else:
                        st.success(f"Found {len(results)} complaint(s).")
                        for complaint in results:
                            show_complaint(complaint, editable=False, prefix="search")
                except (ValueError, Exception) as error:
                    st.error(f"Unable to retrieve complaints: {error}")
    except Exception as error:
        st.error(f"Unable to load complaint analytics: {error}")


elif page == "Management":
    st.title("Management Overview")
    st.caption("Fraud monitoring and investigation performance")
    controls = st.columns([4, 1])

    with controls[0]:
        period = page_period_filter("management", "All Time")

    with controls[1]:
        if st.button("↻ Refresh Management Data"):
            st.rerun()

    try:
        now = datetime.now(REPORT_TZ)
        bounds = reporting_bounds(period, now)
        summary = transaction_summary(bounds)
        all_time = transaction_summary(reporting_bounds("All Time", now))
        previous = previous_period_summary(bounds)

        total = int(summary["total"])
        approved = int(summary["approved"])
        review = int(summary["review"])
        blocked = int(summary["blocked"])
        flagged = review + blocked
        avg_risk = float(summary["avg_risk"])

        approval_rate = approved / total if total else 0
        review_rate = review / total if total else 0
        block_rate = blocked / total if total else 0
        flagged_rate = flagged / total if total else 0

        def rate_change(count, previous_count):
            if not previous or not total or not previous["total"]:
                return None
            difference = count / total - previous_count / int(previous["total"])
            return f"{difference * 100:+.1f} pp"

        st.caption(f"Dashboard updated: {now:%b %d, %Y %I:%M:%S %p %Z}")

        st.subheader("Business Performance")

        c1, c2, c3 = st.columns(3)

        c1.metric(
            ":material/database: Total Transactions",
            f"{int(all_time['total']):,}",
            help="All scored transactions across the entire database.",
        )

        c2.metric(
            "Transactions in Selected Period",
            f"{total:,}",
            delta=percentage_change(total, int(previous["total"])) if previous else None,
            delta_color="off",
        )

        c3.metric(
            "Average Risk Score",
            f"{avg_risk:.1%}" if total else "N/A",
        )

        c4, c5, c6, c7 = st.columns(4)

        c4.metric(
            "Approval Rate",
            f"{approval_rate:.1%}" if total else "N/A",
            delta=rate_change(approved, int(previous["approved"])) if previous else None,
            delta_color="off",
        )

        c5.metric(
            "Review Rate",
            f"{review_rate:.1%}" if total else "N/A",
            delta=rate_change(review, int(previous["review"])) if previous else None,
            delta_color="off",
        )

        c6.metric(
            "Block Rate",
            f"{block_rate:.1%}" if total else "N/A",
            delta=rate_change(blocked, int(previous["blocked"])) if previous else None,
            delta_color="off",
        )

        c7.metric(
            "Flagged Rate",
            f"{flagged_rate:.1%}" if total else "N/A",
        )

        st.caption("Changes are compared with the preceding interval of equal duration. pp = percentage points.")

        st.divider()

        if total:
            st.subheader("Transaction Volume Trend")

            trend = transaction_trend(bounds, period)

            if not trend.empty:
                st.altair_chart(transaction_line_chart(trend), use_container_width=True)

            st.subheader("Model Decision Analysis")

            decisions = pd.DataFrame({
                "Decision": ["APPROVE", "REVIEW", "BLOCK"],
                "Count": [approved, review, blocked],
                "Rate": [approval_rate, review_rate, block_rate],
            })

            left, right = st.columns(2)

            with left:
                st.altair_chart(decision_donut(summary), use_container_width=True)

            with right:
                st.dataframe(
                    decisions,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Rate": st.column_config.ProgressColumn(
                            "Rate", min_value=0, max_value=1, format="percent"
                        )
                    },
                )

            if not trend.empty:
                st.subheader("Fraud Risk Trend")
                st.altair_chart(flagged_rate_chart(trend), use_container_width=True)

        else:
            st.info("No transactions were recorded during the selected period.")

        st.divider()
        st.subheader("Investigation Performance")

        where, params = reporting_where(bounds, "created_at")

        cases = mysql_rows("""
            SELECT case_status, COUNT(*) AS total
            FROM investigation_cases
        """ + where + " GROUP BY case_status", params)

        analyst_rows = mysql_rows("""
            SELECT analyst_decision, COUNT(*) AS total
            FROM investigation_cases
        """ + where + " GROUP BY analyst_decision", params)

        case_counts = {row["case_status"]: int(row["total"]) for row in cases}
        analyst_counts = {row["analyst_decision"]: int(row["total"]) for row in analyst_rows}

        total_cases = sum(case_counts.values())
        completed_cases = case_counts.get("RESOLVED", 0) + case_counts.get("CLOSED", 0)
        resolution_rate = completed_cases / total_cases if total_cases else 0

        i1, i2, i3 = st.columns(3)
        i1.metric("Total Investigations", f"{total_cases:,}")
        i2.metric("Completed Investigations", f"{completed_cases:,}")
        i3.metric("Case Completion Rate", f"{resolution_rate:.1%}" if total_cases else "N/A")

        st.caption("Current status of investigations created during the selected reporting period.")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Open", case_counts.get("OPEN", 0))
        c2.metric("In Progress", case_counts.get("IN_PROGRESS", 0))
        c3.metric("Resolved", case_counts.get("RESOLVED", 0))
        c4.metric("Closed", case_counts.get("CLOSED", 0))

        workload = pd.DataFrame({
            "Status": CASE_STATUSES,
            "Cases": [case_counts.get(status, 0) for status in CASE_STATUSES],
        })

        workload_chart = alt.Chart(workload).mark_bar(
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5,
        ).encode(
            x=alt.X("Status:N", sort=CASE_STATUSES),
            y=alt.Y("Cases:Q"),
            color=alt.Color(
                "Status:N",
                scale=alt.Scale(
                    domain=["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"],
                    range=["#D97706", "#008E92", "#16A34A", "#64748B"],
                ),
                legend=None,
            ),
            tooltip=["Status:N", "Cases:Q"],
        ).properties(height=250)

        st.altair_chart(workload_chart, use_container_width=True)

        st.divider()
        st.subheader("Analyst Review Outcomes")

        a1, a2, a3 = st.columns(3)
        a1.metric("Analyst Approved", analyst_counts.get("APPROVE", 0))
        a2.metric("Analyst Declined", analyst_counts.get("DECLINE", 0))
        a3.metric("Pending Decision", analyst_counts.get("PENDING", 0))

        decided = analyst_counts.get("APPROVE", 0) + analyst_counts.get("DECLINE", 0)

        if decided:
            st.write(f"**Analyst approval rate among decided cases:** {analyst_counts.get('APPROVE', 0) / decided:.1%}")

        st.divider()
        st.subheader("Export Management Report")

        report = pd.DataFrame([{
            "Reporting Period": period,
            "Period Start UTC": bounds[0],
            "Period End UTC": bounds[1],
            "All-Time Transactions": int(all_time["total"]),
            "Period Transactions": total,
            "Model Approved": approved,
            "Model Review": review,
            "Model Blocked": blocked,
            "Approval Rate": approval_rate if total else None,
            "Review Rate": review_rate if total else None,
            "Block Rate": block_rate if total else None,
            "Flagged Rate": flagged_rate if total else None,
            "Average Risk Score": avg_risk if total else None,
            "Total Investigations": total_cases,
            "Completed Investigations": completed_cases,
            "Case Completion Rate": resolution_rate if total_cases else None,
            "Analyst Approved": analyst_counts.get("APPROVE", 0),
            "Analyst Declined": analyst_counts.get("DECLINE", 0),
            "Analyst Pending": analyst_counts.get("PENDING", 0),
        }])

        st.download_button(
            "Download Management Report (CSV)",
            data=report.to_csv(index=False).encode("utf-8"),
            file_name=f"afrimarket_management_{now:%Y%m%d_%H%M%S}.csv",
            mime="text/csv",
        )

        st.caption("Model decisions and risk scores are predictions, not confirmed fraud outcomes.")

    except mysql.connector.Error as error:
        st.error(f"Unable to retrieve management data from MySQL: {error}")


elif page == "System Monitoring":
    st.title("System Monitoring")
    st.caption("Big Data Infrastructure, Streaming Performance and Pipeline Health")

    controls = st.columns([3, 1, 1])

    with controls[0]:
        st.caption("Live infrastructure monitoring")

    with controls[1]:
        auto_refresh = st.toggle("Auto-refresh", value=False, key="monitor_auto_refresh")

    with controls[2]:
        st.button("Refresh", key="monitor_refresh", use_container_width=True)

    @st.fragment(run_every=60 if auto_refresh else None)
    def render_system_monitoring():
        health = collect_system_health()

        st.caption(
            "Last checked: " +
            datetime.now(REPORT_TZ).strftime("%B %d, %Y | %I:%M:%S %p %Z")
        )

        spark = health["Spark"]["data"]
        hdfs = health["HDFS"]["data"]
        kafka = health["Kafka"]["data"]
        mysql = health["MySQL"]["data"]
        cassandra = health["Cassandra"]["data"]
        checkpoints = health["Checkpoints"]["data"]

        apps = spark.get("activeapps", [])

        streaming = next(
            (app for app in apps
             if app.get("name") == "AfriMarketContinuousFraudScoring"
             and app.get("state") == "RUNNING"),
            None,
        )

        st.subheader("Infrastructure Health")

        services = [
            ("Apache Kafka", "Kafka", "Port reachable"),
            ("Apache Spark", "Spark", spark.get("status", "Responding")),
            ("Hadoop HDFS", "HDFS", "Operational" if hdfs.get("FSState") == "Operational" else "Check state"),
            ("Apache Cassandra", "Cassandra", "Connected"),
            ("MySQL", "MySQL", "Connected"),
        ]

        columns = st.columns(3)

        for index, (label, key, status) in enumerate(services):
            with columns[index % 3]:
                with st.container(border=True):
                    st.markdown(f"**{label}**")

                    if not health[key]["available"]:
                        st.error("Unavailable")
                        st.caption(health[key]["error"])
                    elif key == "HDFS" and (
                        hdfs.get("UnderReplicatedBlocks", 0) > 0
                        or hdfs.get("NumLiveDataNodes", 0) == 0
                        or hdfs.get("FSState") != "Operational"
                    ):
                        st.warning("Attention required")
                    elif key == "Spark" and spark.get("status") != "ALIVE":
                        st.warning(status)
                    elif key == "Kafka":
                        st.info(status)
                    else:
                        st.success(status)

        with columns[2]:
            with st.container(border=True):
                st.markdown("**Streaming Pipeline**")
                if streaming:
                    st.success("Running")
                elif not health["Spark"]["available"]:
                    st.warning("Status unknown")
                else:
                    st.warning("Not detected")

        st.divider()
        st.subheader("Spark Distributed Processing")

        if health["Spark"]["available"]:
            metrics = st.columns(4)
            metrics[0].metric("Live Workers", spark.get("aliveworkers", 0))
            metrics[1].metric("Active Applications", len(apps))
            metrics[2].metric("Allocated Cores", spark.get("coresused", 0))
            metrics[3].metric("Executor Memory", f'{spark.get("memoryused", 0) / 1024:.1f} GiB')

            left, right = st.columns(2)

            with left:
                total = spark.get("cores", 0)
                used = spark.get("coresused", 0)
                st.markdown("**Registered CPU Allocation**")
                st.progress(min(used / total, 1.0) if total else 0.0)
                st.caption(f"{used} of {total} registered cores allocated")

            with right:
                total = spark.get("memory", 0)
                used = spark.get("memoryused", 0)
                st.markdown("**Registered Memory Allocation**")
                st.progress(min(used / total, 1.0) if total else 0.0)
                st.caption(f"{used:,} of {total:,} MiB allocated")

            st.caption("Registered Spark capacity is not physical VM utilization.")

            workers = pd.DataFrame(spark.get("workers", []))

            if not workers.empty:
                fields = {
                    "id": "Worker ID",
                    "state": "Status",
                    "cores": "Registered Cores",
                    "coresused": "Allocated Cores",
                    "memory": "Registered Memory (MiB)",
                    "memoryused": "Allocated Memory (MiB)",
                }

                st.dataframe(
                    workers[list(fields)].rename(columns=fields),
                    hide_index=True,
                    use_container_width=True,
                )

            if streaming:
                st.caption("Active streaming application")
                st.code(streaming["id"], language=None)
        else:
            st.warning("Spark monitoring data is unavailable.")

        st.divider()
        st.subheader("HDFS Storage and Reliability")

        if health["HDFS"]["available"]:
            capacity = hdfs.get("CapacityTotal", 0)
            used = hdfs.get("CapacityUsed", 0)
            remaining = hdfs.get("CapacityRemaining", 0)

            metrics = st.columns(4)
            metrics[0].metric("Total Capacity", f"{capacity / 1024**3:.2f} GiB")
            metrics[1].metric("HDFS Data Used", f"{used / 1024**2:.2f} MiB")
            metrics[2].metric("Available Storage", f"{remaining / 1024**3:.2f} GiB")
            metrics[3].metric("Live DataNodes", hdfs.get("NumLiveDataNodes", 0))

            other = max(capacity - used - remaining, 0)

            storage = pd.DataFrame({
                "Storage": ["HDFS Data", "Other / Reserved", "Available"],
                "GiB": [used / 1024**3, other / 1024**3, remaining / 1024**3],
            })

            chart = alt.Chart(storage).mark_arc(innerRadius=75).encode(
                theta=alt.Theta("GiB:Q"),
                color=alt.Color(
                    "Storage:N",
                    scale=alt.Scale(
                        domain=["HDFS Data", "Other / Reserved", "Available"],
                        range=["#008E92", "#94A3B8", "#16A34A"],
                    ),
                ),
                tooltip=["Storage:N", alt.Tooltip("GiB:Q", format=".3f")],
            ).properties(height=270)

            left, right = st.columns(2)

            with left:
                st.altair_chart(chart, use_container_width=True)

            with right:
                st.metric("Total Blocks", hdfs.get("BlocksTotal", 0))
                st.metric("Files and Directories", hdfs.get("FilesTotal", 0))
                st.metric("Dead DataNodes", hdfs.get("NumDeadDataNodes", 0))
                st.metric("Under-replicated Blocks", hdfs.get("UnderReplicatedBlocks", 0))

            if hdfs.get("UnderReplicatedBlocks", 0) > 0:
                st.warning("Some HDFS blocks have fewer replicas than requested. This is a cluster-wide metric.")
            else:
                st.success("No under-replicated blocks reported.")

            st.caption("Single-DataNode development deployment. Replication factor 1 does not provide storage redundancy.")
        else:
            st.warning("HDFS monitoring data is unavailable.")

        st.divider()
        st.subheader("Streaming Pipeline Activity")

        if health["Checkpoints"]["available"]:
            batch = checkpoints.get("latest_commit")
            offset = checkpoints.get("latest_offset")
            duration = checkpoints.get("duration_seconds")

            metrics = st.columns(4)
            metrics[0].metric("Latest Completed Batch", batch if batch is not None else "N/A")
            metrics[1].metric("Latest Offset Batch", offset if offset is not None else "N/A")
            metrics[2].metric("Batches Ahead of Commit", max(offset - batch, 0) if offset is not None and batch is not None else "N/A")
            metrics[3].metric("Latest Batch Duration", f"{duration:.1f} s" if duration is not None and duration >= 0 else "N/A")

            completed = checkpoints.get("completed_at")

            if completed is not None:
                st.caption(f"Latest checkpoint commit: {completed.strftime('%Y-%m-%d %H:%M:%S %Z')}")

            st.caption("Duration is estimated from offset and commit file timestamps. It is not end-to-end latency or Kafka consumer lag.")
        else:
            st.warning("Checkpoint monitoring is temporarily unavailable.")
            st.caption(health["Checkpoints"]["error"])

        st.divider()
        st.subheader("Data Services")

        metrics = st.columns(4)

        metrics[0].metric(
            "Scored Transactions",
            f'{mysql["total_scores"]:,}' if health["MySQL"]["available"] else "N/A",
        )

        metrics[1].metric(
            "Investigation Cases",
            f'{mysql["total_cases"]:,}' if health["MySQL"]["available"] else "N/A",
        )

        metrics[2].metric(
            "MySQL Check Duration",
            f'{mysql["query_ms"]:.1f} ms' if health["MySQL"]["available"] else "N/A",
        )

        metrics[3].metric(
            "Kafka TCP Connection",
            f'{kafka["connection_ms"]:.1f} ms' if health["Kafka"]["available"] else "N/A",
        )

        if mysql.get("latest_score") is not None:
            latest = localize_timestamp(mysql["latest_score"])
            st.caption(f"Latest stored transaction score: {latest.strftime('%Y-%m-%d %H:%M:%S %Z')}")

        if health["Cassandra"]["available"]:
            st.caption(f'Cassandra version: {cassandra["version"]} | Connection and query: {cassandra["query_ms"]:.1f} ms')

        st.caption("Kafka TCP connectivity does not establish broker health, topic availability or consumer lag.")

        st.divider()
        st.subheader("Processing Optimization Benchmarks")
        st.caption("Historical observations from three 1,000-transaction replay runs.")

        benchmark = pd.DataFrame({
            "Run": ["Earlier Run", "Intermediate Run", "Latest Run"],
            "Duration": [304, 164, 121],
            "Transactions": [1000, 1000, 1000],
        })

        benchmark["Throughput"] = benchmark["Transactions"] / benchmark["Duration"]

        metrics = st.columns(4)
        metrics[0].metric("Latest Benchmark", "121 s")
        metrics[1].metric("Benchmark Throughput", "8.26 tx/s")
        metrics[2].metric("Duration Reduction", "60.2%")
        metrics[3].metric("Observed Speedup", "2.51x")

        left, right = st.columns(2)

        order = ["Earlier Run", "Intermediate Run", "Latest Run"]

        with left:
            chart = alt.Chart(benchmark).mark_bar(cornerRadiusEnd=5).encode(
                x=alt.X("Run:N", sort=order, title=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("Duration:Q", title="Processing Duration (seconds)"),
                color=alt.Color("Run:N", scale=alt.Scale(domain=order, range=["#94A3B8", "#008E92", "#16A34A"]), legend=None),
                tooltip=["Run:N", "Duration:Q"],
            ).properties(height=280)

            st.altair_chart(chart, use_container_width=True)

        with right:
            chart = alt.Chart(benchmark).mark_bar(cornerRadiusEnd=5).encode(
                x=alt.X("Run:N", sort=order, title=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("Throughput:Q", title="Transactions per Second"),
                color=alt.Color("Run:N", scale=alt.Scale(domain=order, range=["#94A3B8", "#008E92", "#16A34A"]), legend=None),
                tooltip=["Run:N", alt.Tooltip("Throughput:Q", format=".2f")],
            ).properties(height=280)

            st.altair_chart(chart, use_container_width=True)

        st.caption("Historical benchmarks, not live throughput. Runs were not controlled experiments and used different transaction ranges.")

    render_system_monitoring()
