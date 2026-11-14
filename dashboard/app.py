import os
import pandas as pd
import mysql.connector
import streamlit as st
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


st.set_page_config(
    page_title="AfriMarket Fraud Intelligence",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dashboard styling
st.markdown("""
<style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    [data-testid="stMetric"] {
        background: #172338;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 18px;
    }

    [data-testid="stMetricLabel"] {
        color: #CBD5E1;
    }

    [data-testid="stMetricValue"] {
        color: #F8FAFC;
    }

    h1, h2, h3 {
        letter-spacing: -0.5px;
    }
</style>
""", unsafe_allow_html=True)


# Read transaction scores from MySQL
def get_scores():
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "fraud"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD")
    )

    try:
        query = """
            SELECT TransactionID, TransactionDT,
                   risk_score, decision, stored_at
            FROM scores
            ORDER BY stored_at DESC
            LIMIT 1000
        """

        cursor = connection.cursor(dictionary=True)

        try:
            cursor.execute(query)
            return pd.DataFrame(cursor.fetchall())
        finally:
            cursor.close()
    finally:
        connection.close()

# Open a connection to MySQL
def get_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "fraud"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD")
    )


# Retrieve investigation cases with their transaction scores
def get_investigations():
    connection = get_mysql_connection()

    try:
        query = """
            SELECT
                c.case_id,
                c.TransactionID,
                s.risk_score,
                s.decision,
                c.case_status,
                c.assigned_to,
                c.notes,
                c.created_at,
                c.updated_at
            FROM investigation_cases c
            JOIN scores s ON c.TransactionID = s.TransactionID
            ORDER BY c.updated_at DESC, c.case_id DESC
            LIMIT 500
        """

        cursor = connection.cursor(dictionary=True)

        try:
            cursor.execute(query)
            return pd.DataFrame(cursor.fetchall())
        finally:
            cursor.close()
    finally:
        connection.close()


# Save an analyst's updates to an investigation
def update_investigation(case_id, status, analyst, notes):
    connection = get_mysql_connection()

    try:
        cursor = connection.cursor()

        try:
            cursor.execute(
                """
                UPDATE investigation_cases
                SET case_status = %s,
                    assigned_to = %s,
                    notes = %s
                WHERE case_id = %s
                """,
                (status, analyst or None, notes or None, case_id)
            )

            if cursor.rowcount != 1:
                connection.rollback()
                raise ValueError("Investigation case was not found.")

            connection.commit()

        finally:
            cursor.close()

    finally:
        connection.close()

# Create an investigation case for a flagged transaction
def create_investigation(transaction_id):
    connection = get_mysql_connection()

    try:
        cursor = connection.cursor()

        try:
            # Confirm the transaction is flagged
            cursor.execute(
                """
                SELECT decision
                FROM scores
                WHERE TransactionID = %s
                """,
                (transaction_id,)
            )

            transaction = cursor.fetchone()

            if transaction is None:
                raise ValueError("Transaction not found.")

            if transaction[0] not in ("REVIEW", "BLOCK"):
                raise ValueError("Only flagged transactions can be investigated.")

            # Avoid creating duplicate active cases
            cursor.execute(
                """
                SELECT case_id
                FROM investigation_cases
                WHERE TransactionID = %s
                  AND case_status IN ('OPEN', 'IN_PROGRESS')
                LIMIT 1
                """,
                (transaction_id,)
            )

            if cursor.fetchone():
                raise ValueError("An active investigation already exists.")

            cursor.execute(
                """
                INSERT INTO investigation_cases (TransactionID)
                VALUES (%s)
                """,
                (transaction_id,)
            )

            connection.commit()
            return cursor.lastrowid

        finally:
            cursor.close()

    finally:
        connection.close()
# Search complaints in Cassandra
def get_complaints(search_type, search_value):
    cluster = Cluster(
        [os.getenv("CASSANDRA_HOST", "cassandra")],
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        connect_timeout=10
    )

    try:
        session = cluster.connect("afrimarket")
        session.row_factory = dict_factory

        if search_type == "Complaint ID":
            query = """
                SELECT complaint_id, transaction_id,
                       complaint_date, complaint_category,
                       complaint_text, complaint_status
                FROM complaints_by_id
                WHERE complaint_id = ?
            """
            value = search_value.strip()

        else:
            query = """
                SELECT complaint_id, transaction_id,
                       complaint_date, complaint_category,
                       complaint_text, complaint_status
                FROM complaints_by_transaction
                WHERE transaction_id = ?
            """
            value = int(search_value)

        statement = session.prepare(query)
        return list(session.execute(statement, (value,)))

    finally:
        cluster.shutdown()

# Calculate management metrics across all stored transactions
def get_management_summary():
    connection = get_mysql_connection()

    try:
        cursor = connection.cursor(dictionary=True)

        try:
            cursor.execute("""
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(decision = 'APPROVE'), 0) AS approved,
                    COALESCE(SUM(decision = 'REVIEW'), 0) AS review,
                    COALESCE(SUM(decision = 'BLOCK'), 0) AS blocked,
                    COALESCE(AVG(risk_score), 0) AS avg_risk
                FROM scores
            """)

            return cursor.fetchone()

        finally:
            cursor.close()
    finally:
        connection.close()


# Count investigation cases by status
def get_case_summary():
    connection = get_mysql_connection()

    try:
        cursor = connection.cursor(dictionary=True)

        try:
            cursor.execute("""
                SELECT case_status, COUNT(*) AS total
                FROM investigation_cases
                GROUP BY case_status
            """)

            return pd.DataFrame(cursor.fetchall())

        finally:
            cursor.close()
    finally:
        connection.close()

# Sidebar navigation
st.sidebar.markdown("## 🛡️ AfriMarket")
st.sidebar.caption("FRAUD INTELLIGENCE PLATFORM")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    ["Overview", "Investigations", "Complaints", "Management"]
)

st.sidebar.divider()
st.sidebar.caption("CIS 641 • Academic Project")


if page == "Overview":
    st.title("Fraud Monitoring Overview")
    st.caption("Transaction risk assessment and decision monitoring")

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

            # Summary cards
            col1, col2, col3, col4 = st.columns(4)

            col1.metric("Transactions", f"{total:,}")
            col2.metric("Approved", f"{approved:,}")
            col3.metric("Needs Review", f"{review:,}")
            col4.metric("Blocked", f"{blocked:,}")

            st.write("")

            left, right = st.columns([3, 2])

            with left:
                st.subheader("Decision Distribution")

                distribution = pd.DataFrame({
                    "Decision": ["APPROVE", "REVIEW", "BLOCK"],
                    "Count": [approved, review, blocked]
                })

                st.bar_chart(
                    distribution,
                    x="Decision",
                    y="Count",
                    color="#60A5FA"
                )

            with right:
                st.subheader("Risk Summary")

                avg_risk = scores["risk_score"].mean()
                flagged = review + blocked

                st.metric("Average Risk Score", f"{avg_risk:.1%}")
                st.metric("Flagged Transactions", f"{flagged:,}")
                st.metric("Flagged Rate", f"{flagged / total:.1%}")

            st.divider()

            st.subheader("Recent Transactions")

            search = st.text_input(
                "Search by Transaction ID",
                placeholder="Enter a transaction ID..."
            )

            decisions = st.multiselect(
                "Filter by decision",
                ["APPROVE", "REVIEW", "BLOCK"],
                default=["APPROVE", "REVIEW", "BLOCK"]
            )

            filtered = scores[scores["decision"].isin(decisions)].copy()

            if search:
                filtered = filtered[
                    filtered["TransactionID"].astype(str).str.contains(
                        search, regex=False
                    )
                ]

            st.dataframe(
                filtered,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "TransactionID": st.column_config.NumberColumn(
                        "Transaction ID",
                        format="%d"
                    ),
                    "TransactionDT": st.column_config.NumberColumn(
                        "Transaction Time (relative seconds)"
                    ),
                    "risk_score": st.column_config.ProgressColumn(
                        "Risk Score",
                        min_value=0,
                        max_value=1,
                        format="%.2f"
                    ),
                    "decision": "Decision",
                    "stored_at": "Recorded At"
                }
            )

            st.caption(
                "Showing up to 1,000 most recently stored transactions. "
                "Current records are test data until Spark integration."
            )

    except mysql.connector.Error:
        st.error(
            "Unable to retrieve transactions from MySQL. "
            "Check that the database is running and its credentials are configured."
        )


elif page == "Investigations":
    st.title("Investigation Center")
    st.caption("Review suspicious transactions and manage investigation cases")

    try:
        cases = get_investigations()

        if cases.empty:
            st.info("No investigation cases have been created yet.")

        else:
            # Investigation summary
            open_cases = int((cases["case_status"] == "OPEN").sum())
            in_progress = int((cases["case_status"] == "IN_PROGRESS").sum())
            resolved = int((cases["case_status"] == "RESOLVED").sum())

            col1, col2, col3 = st.columns(3)

            col1.metric("Open Cases", open_cases)
            col2.metric("In Progress", in_progress)
            col3.metric("Resolved", resolved)

            st.divider()

            with st.expander("➕ Create New Investigation"):
                flagged_scores = get_scores()

                if not flagged_scores.empty:
                    flagged_scores = flagged_scores[
                        flagged_scores["decision"].isin(["REVIEW", "BLOCK"])
                    ]

                if flagged_scores.empty:
                    st.info("No flagged transactions are available.")
                else:
                    transaction_ids = flagged_scores["TransactionID"].tolist()

                    with st.form("create_case_form"):
                        transaction_id = st.selectbox(
                            "Select a flagged transaction",
                            transaction_ids,
                            format_func=lambda x: f"Transaction #{x}"
                        )

                        create_clicked = st.form_submit_button(
                            "Create Investigation",
                            type="primary"
                        )

                        if create_clicked:
                            try:
                                new_case_id = create_investigation(
                                    int(transaction_id)
                                )
                                st.success(
                                    f"Investigation #{new_case_id} created!"
                                )
                                st.rerun()
                            except (mysql.connector.Error, ValueError) as error:
                                st.error(str(error))

            st.subheader("Investigation Queue")

            status_filter = st.multiselect(
                "Filter by case status",
                ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"],
                default=["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"]
            )

            filtered = cases[
                cases["case_status"].isin(status_filter)
            ]

            st.dataframe(
                filtered[
                    [
                        "case_id",
                        "TransactionID",
                        "risk_score",
                        "decision",
                        "case_status",
                        "assigned_to",
                        "updated_at"
                    ]
                ],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "risk_score": st.column_config.ProgressColumn(
                        "Risk Score",
                        min_value=0,
                        max_value=1,
                        format="%.2f"
                    )
                }
            )

            st.divider()
            st.subheader("Case Details")

            if filtered.empty:
                st.info("No cases match the selected filter.")

            else:
                case_ids = filtered["case_id"].tolist()

                selected_id = st.selectbox(
                    "Select an investigation case",
                    case_ids,
                    format_func=lambda x: f"Case #{x}"
                )

                selected_case = filtered[
                    filtered["case_id"] == selected_id
                ].iloc[0]

                left, right = st.columns(2)

                with left:
                    st.write(
                        f"**Transaction ID:** "
                        f"{selected_case['TransactionID']}"
                    )

                    st.write(
                        f"**Risk Score:** "
                        f"{selected_case['risk_score']:.1%}"
                    )

                    st.write(
                        f"**Fraud Decision:** "
                        f"{selected_case['decision']}"
                    )

                with right:
                    st.write(
                        f"**Created:** {selected_case['created_at']}"
                    )

                    st.write(
                        f"**Last Updated:** {selected_case['updated_at']}"
                    )

                # Show customer complaints linked to this investigation
                st.divider()
                st.subheader("Related Customer Complaints")

                transaction_id = int(selected_case["TransactionID"])

                try:
                    related_complaints = get_complaints(
                        "Transaction ID",
                        str(transaction_id)
                    )

                    if related_complaints:
                        st.info(
                            f"{len(related_complaints)} complaint(s) "
                            f"linked to transaction #{transaction_id}"
                        )

                        for complaint in related_complaints:
                            with st.expander(
                                f"Complaint #{complaint['complaint_id']} "
                                f"— {complaint['complaint_category']}"
                            ):
                                st.write(
                                    f"**Status:** {complaint['complaint_status']}"
                                )
                                st.write(
                                    f"**Date:** {complaint['complaint_date']}"
                                )
                                st.markdown("**Customer Complaint**")
                                st.write(complaint["complaint_text"])

                    else:
                        st.info(
                            "No customer complaints are linked "
                            "to this transaction."
                        )

                except Exception:
                    st.warning(
                        "Related complaints are temporarily unavailable. "
                        "You can still manage this investigation."
                    )

                with st.form("investigation_form"):
                    status = st.selectbox(
                        "Case Status",
                        ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"],
                        index=[
                            "OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"
                        ].index(selected_case["case_status"])
                    )

                    analyst = st.text_input(
                        "Assigned Analyst",
                        value=selected_case["assigned_to"] or ""
                    )

                    notes = st.text_area(
                        "Investigation Notes",
                        value=selected_case["notes"] or "",
                        height=140
                    )

                    submitted = st.form_submit_button(
                        "Save Investigation",
                        type="primary"
                    )

                    if submitted:
                        update_investigation(
                            int(selected_id),
                            status,
                            analyst.strip(),
                            notes.strip()
                        )

                        st.success("Investigation updated successfully!")
                        st.rerun()

            st.caption(
                "Showing up to 500 most recently updated investigation cases."
            )

    except (mysql.connector.Error, ValueError) as error:
        st.error(f"Unable to manage investigations: {error}")



elif page == "Complaints":
    st.title("Customer Complaints")
    st.caption("Search and review customer complaints stored in Cassandra")

    search_type = st.radio(
        "Search complaints by",
        ["Transaction ID", "Complaint ID"],
        horizontal=True
    )

    search_value = st.text_input(
        "Enter search value",
        placeholder="100002" if search_type == "Transaction ID" else "TEST001"
    )

    if st.button("Search Complaints", type="primary"):
        if not search_value.strip():
            st.warning("Please enter a search value.")

        elif search_type == "Transaction ID" and not search_value.strip().isdigit():
            st.warning("Transaction ID must contain only numbers.")

        else:
            try:
                complaints = get_complaints(search_type, search_value)

                if not complaints:
                    st.info("No complaints found for this search.")

                else:
                    st.success(f"Found {len(complaints)} complaint(s).")

                    for complaint in complaints:
                        with st.container(border=True):
                            st.subheader(
                                f"Complaint #{complaint['complaint_id']}"
                            )

                            col1, col2 = st.columns(2)

                            with col1:
                                st.write(
                                    f"**Transaction ID:** "
                                    f"{complaint['transaction_id']}"
                                )
                                st.write(
                                    f"**Category:** "
                                    f"{complaint['complaint_category']}"
                                )

                            with col2:
                                st.write(
                                    f"**Date:** "
                                    f"{complaint['complaint_date']}"
                                )
                                st.write(
                                    f"**Status:** "
                                    f"{complaint['complaint_status']}"
                                )

                            st.markdown("**Customer Complaint**")
                            st.write(complaint["complaint_text"])

            except (ValueError, Exception) as error:
                st.error(
                    "Unable to retrieve complaints from Cassandra. "
                    "Check that Cassandra is running and the search value is valid."
                )
                st.caption(f"Technical details: {error}")

    st.divider()
    st.caption(
        "Complaint records are currently test data. "
        "Kafka and Spark integration is pending."
    )


elif page == "Management":
    st.title("Management Overview")
    st.caption("Fraud monitoring and investigation performance")

    if st.button("↻ Refresh Management Data"):
        st.rerun()

    try:
        summary = get_management_summary()
        cases = get_case_summary()

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

            # Executive summary
            col1, col2, col3, col4 = st.columns(4)

            col1.metric("Total Transactions", f"{total:,}")
            col2.metric("Approval Rate", f"{approval_rate:.1%}")
            col3.metric("Flagged Rate", f"{flagged_rate:.1%}")
            col4.metric("Average Risk", f"{avg_risk:.1%}")

            st.divider()

            # Transaction decision analysis
            st.subheader("Transaction Decision Analysis")

            decisions = pd.DataFrame({
                "Decision": ["APPROVE", "REVIEW", "BLOCK"],
                "Count": [approved, review, blocked],
                "Rate": [approval_rate, review_rate, block_rate]
            })

            left, right = st.columns(2)

            with left:
                st.markdown("**Decision Distribution**")

                st.bar_chart(
                    decisions,
                    x="Decision",
                    y="Count",
                    color="#60A5FA"
                )

            with right:
                st.markdown("**Decision Rates**")

                st.dataframe(
                    decisions,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Rate": st.column_config.ProgressColumn(
                            "Rate",
                            min_value=0,
                            max_value=1,
                            format="percent"
                        )
                    }
                )

            st.divider()

            # Investigation workload
            st.subheader("Investigation Workload")

            statuses = [
                "OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"
            ]

            case_counts = {
                row["case_status"]: int(row["total"])
                for _, row in cases.iterrows()
            }

            workload = pd.DataFrame({
                "Status": statuses,
                "Cases": [
                    case_counts.get(status, 0)
                    for status in statuses
                ]
            })

            col1, col2, col3, col4 = st.columns(4)

            col1.metric("Open", case_counts.get("OPEN", 0))
            col2.metric("In Progress", case_counts.get("IN_PROGRESS", 0))
            col3.metric("Resolved", case_counts.get("RESOLVED", 0))
            col4.metric("Closed", case_counts.get("CLOSED", 0))

            st.bar_chart(
                workload,
                x="Status",
                y="Cases",
                color="#60A5FA"
            )

            st.divider()

            st.subheader("Operational Summary")

            st.write(
                f"Out of **{total:,} transactions**, "
                f"**{approved:,}** were approved, "
                f"**{review:,}** were sent for review, "
                f"and **{blocked:,}** were blocked."
            )

            st.write(
                f"The average risk score is **{avg_risk:.1%}**, "
                f"and **{flagged_rate:.1%}** of transactions "
                "were flagged for review or blocking."
            )

            st.caption(
                "Current results are based on test records. "
                "Live Spark integration is pending. "
                "Decisions and risk scores are model outputs, "
                "not confirmed fraud outcomes."
            )

    except mysql.connector.Error:
        st.error(
            "Unable to retrieve management data from MySQL."
        )
