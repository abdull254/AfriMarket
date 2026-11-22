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


def get_scores():
    return pd.DataFrame(mysql_rows("""
        SELECT TransactionID, TransactionDT, risk_score, decision, stored_at
        FROM scores ORDER BY stored_at DESC LIMIT 1000
    """))


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


def get_investigations():
    return pd.DataFrame(mysql_rows("""
        SELECT c.case_id, c.TransactionID, s.risk_score,
               s.decision, c.case_status, c.assigned_to, c.notes,
               c.analyst_decision, c.created_at, c.updated_at
        FROM investigation_cases c
        JOIN scores s ON c.TransactionID = s.TransactionID
        ORDER BY c.updated_at DESC, c.case_id DESC LIMIT 500
    """))


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
                SET case_status = %s, assigned_to = %s, notes = %s,
                    analyst_decision = %s
                WHERE case_id = %s
            """, (status, analyst or None, notes or None, analyst_decision, case_id))
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
            <p>Transaction risk assessment and model decision monitoring</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("↻ Refresh transactions"):
        st.rerun()
    try:
        scores = get_scores()
        if scores.empty:
            st.info("No transactions have been scored yet.")
        else:
            total = len(scores)
            approved = int((scores["decision"] == "APPROVE").sum())
            review = int((scores["decision"] == "REVIEW").sum())
            blocked = int((scores["decision"] == "BLOCK").sum())
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(":material/receipt_long: Recent Transactions", f"{total:,}")

            with col2:
                st.metric(":material/check_circle: Model Approved", f"{approved:,}")

            with col3:
                st.metric(":material/search: Needs Review", f"{review:,}")

            with col4:
                st.metric(":material/shield: Model Blocked", f"{blocked:,}")

            left, right = st.columns([3, 2])
            with left:
                st.subheader("Model Decision Distribution")
                distribution = pd.DataFrame({
                    "Decision": ["APPROVE", "REVIEW", "BLOCK"],
                    "Count": [approved, review, blocked],
                })
                st.bar_chart(distribution, x="Decision", y="Count", color="#008E92")
            with right:
                st.subheader("Risk Summary")
                avg_risk = float(scores["risk_score"].mean())
                flagged = review + blocked

                st.metric(":material/speed: Average Risk Score", f"{avg_risk:.1%}")

                st.metric(":material/shield: Flagged Transactions", f"{flagged:,}")

                st.metric(":material/percent: Flagged Rate", f"{flagged / total:.1%}")
            st.divider()
            st.subheader("Recent Transactions")
            search = st.text_input("Search displayed Transaction IDs", placeholder="Enter a transaction ID...")
            decisions = st.multiselect(
                "Filter by model decision", ["APPROVE", "REVIEW", "BLOCK"],
                default=["APPROVE", "REVIEW", "BLOCK"],
            )
            filtered = scores[scores["decision"].isin(decisions)].copy()
            if search:
                filtered = filtered[
                    filtered["TransactionID"].astype(str).str.contains(search, regex=False)
                ]
            st.dataframe(
                filtered, hide_index=True, use_container_width=True,
                column_config={
                    "TransactionID": st.column_config.NumberColumn("Transaction ID", format="%d"),
                    "TransactionDT": st.column_config.NumberColumn("Transaction Time (relative seconds)"),
                    "risk_score": st.column_config.ProgressColumn(
                        "Risk Score", min_value=0, max_value=1, format="%.2f"
                    ),
                    "decision": "Model Decision",
                    "stored_at": "Recorded At",
                },
            )
            st.caption("Showing up to 1,000 most recently stored transactions. Metrics on this page use this same set.")
    except mysql.connector.Error as error:
        st.error(f"Unable to retrieve transactions from MySQL: {error}")


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

        cases = get_investigations()
        analyst_counts = get_analyst_summary()
        st.subheader("Analyst Review Summary")
        r1, r2, r3 = st.columns(3)
        r1.metric("Approved", analyst_counts.get("APPROVE", 0))
        r2.metric("Declined", analyst_counts.get("DECLINE", 0))
        r3.metric("Pending Reviews", analyst_counts.get("PENDING", 0))
        st.caption("Analyst decisions are separate from the fraud model's APPROVE, REVIEW, and BLOCK decisions.")

        if cases.empty:
            st.info("No investigation cases have been created yet.")
        else:
            st.subheader("Investigation Workload")
            open_cases = int((cases["case_status"] == "OPEN").sum())
            in_progress = int((cases["case_status"] == "IN_PROGRESS").sum())
            resolved = int((cases["case_status"] == "RESOLVED").sum())
            c1, c2, c3 = st.columns(3)
            c1.metric("Open Cases", open_cases)
            c2.metric("In Progress", in_progress)
            c3.metric("Resolved", resolved)
            st.divider()
            st.subheader("Investigation Queue")
            status_filter = st.multiselect(
                "Filter by case status", CASE_STATUSES, default=CASE_STATUSES
            )
            filtered = cases[cases["case_status"].isin(status_filter)]
            st.dataframe(
                filtered[[
                    "case_id", "TransactionID", "risk_score", "decision",
                    "analyst_decision", "case_status", "assigned_to", "updated_at",
                ]],
                hide_index=True, use_container_width=True,
                column_config={
                    "risk_score": st.column_config.ProgressColumn(
                        "Risk Score", min_value=0, max_value=1, format="%.2f"
                    ),
                    "decision": "Model Decision",
                    "analyst_decision": "Analyst Decision",
                },
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
            st.caption("Showing up to 500 most recently updated investigation cases.")
    except (mysql.connector.Error, ValueError) as error:
        st.error(f"Unable to manage investigations: {error}")


elif page == "Complaints":
    st.title("Customer Complaints")
    st.caption("View active complaints, resolved history, and complaints linked to transactions")
    if "complaint_flash" in st.session_state:
        st.success(st.session_state.pop("complaint_flash"))

    active_tab, resolved_tab, search_tab = st.tabs(
        ["Active Complaints", "Resolved History", "Search Complaints"]
    )

    try:
        complaints = list_complaints()
        active = [c for c in complaints if normalize_complaint_status(c["complaint_status"]) != "Resolved"]
        resolved = [c for c in complaints if normalize_complaint_status(c["complaint_status"]) == "Resolved"]
        active.sort(key=lambda c: (str(c.get("complaint_date") or ""), c["complaint_id"]), reverse=True)
        resolved.sort(key=lambda c: (str(c.get("complaint_date") or ""), c["complaint_id"]), reverse=True)

        with active_tab:
            st.metric("Active Complaints", len(active))
            st.caption("Showing the 50 most recent active complaints. Use Search Complaints to find older records.")
            if not active:
                st.info("No open or under-review complaints found.")
            else:
                for complaint in active[:50]:
                    show_complaint(complaint, editable=False, prefix="active")

        with resolved_tab:
            st.metric("Resolved Complaints", len(resolved))
            st.caption("Showing the 50 most recent resolved complaints. Use Search Complaints to find older records.")
            if not resolved:
                st.info("No resolved complaints found.")
            else:
                for complaint in resolved[:50]:
                    show_complaint(complaint, editable=False, prefix="resolved")

        with search_tab:
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
        st.error(f"Unable to load complaints from Cassandra: {error}")


elif page == "Management":
    st.title("Management Overview")
    st.caption("Fraud monitoring and investigation performance")
    if st.button("↻ Refresh Management Data"):
        st.rerun()
    try:
        summary = get_management_summary()
        cases = get_case_summary()
        analyst_counts = get_analyst_summary()
        total = int(summary["total"])
        approved = int(summary["approved"])
        review = int(summary["review"])
        blocked = int(summary["blocked"])
        avg_risk = float(summary["avg_risk"])
        if total == 0:
            st.info("No transaction data is available yet.")
        else:
            approval_rate = approved / total
            review_rate = review / total
            block_rate = blocked / total
            flagged_rate = (review + blocked) / total
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(":material/receipt_long: Recent Transactions", f"{total:,}")
            with col2:
                st.metric(":material/check_circle: Model Approved", f"{approved:,}")

            with col3:
                st.metric(":material/search: Needs Review", f"{review:,}")
            with col4:
                st.metric(":material/shield: Model Blocked", f"{blocked:,}")
            st.divider()
            st.subheader("Model Decision Analysis")
            decisions = pd.DataFrame({
                "Decision": ["APPROVE", "REVIEW", "BLOCK"],
                "Count": [approved, review, blocked],
                "Rate": [approval_rate, review_rate, block_rate],
            })
            left, right = st.columns(2)
            with left:
                st.bar_chart(decisions, x="Decision", y="Count", color="#008E92")
            with right:
                st.dataframe(
                    decisions, hide_index=True, use_container_width=True,
                    column_config={
                        "Rate": st.column_config.ProgressColumn(
                            "Rate", min_value=0, max_value=1, format="percent"
                        )
                    },
                )
            st.divider()
            st.subheader("Investigation Workload")
            case_counts = {row["case_status"]: int(row["total"]) for row in cases}
            workload = pd.DataFrame({
                "Status": CASE_STATUSES,
                "Cases": [case_counts.get(status, 0) for status in CASE_STATUSES],
            })
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Open", case_counts.get("OPEN", 0))
            c2.metric("In Progress", case_counts.get("IN_PROGRESS", 0))
            c3.metric("Resolved", case_counts.get("RESOLVED", 0))
            c4.metric("Closed", case_counts.get("CLOSED", 0))
            st.bar_chart(workload, x="Status", y="Cases", color="#008E92")
            st.divider()
            st.subheader("Analyst Review Outcomes")
            a1, a2, a3 = st.columns(3)
            a1.metric("Approved", analyst_counts.get("APPROVE", 0))
            a2.metric("Declined", analyst_counts.get("DECLINE", 0))
            a3.metric("Pending", analyst_counts.get("PENDING", 0))
            decided = analyst_counts.get("APPROVE", 0) + analyst_counts.get("DECLINE", 0)
            if decided:
                st.write(f"**Analyst approval rate among decided cases:** {analyst_counts.get('APPROVE', 0) / decided:.1%}")
            st.caption("Model decisions and risk scores are predictions, not confirmed fraud outcomes.")
    except mysql.connector.Error as error:
        st.error(f"Unable to retrieve management data from MySQL: {error}")
