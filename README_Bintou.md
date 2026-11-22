AfriMarket — Database, Storage & Dashboard Setup

Contributor: Bintou Ba
Project: CIS 641 — AfriMarket Fraud Detection System

1. Overview

This part of the AfriMarket project sets up MySQL, Cassandra, and a Streamlit dashboard using Docker Compose.

MySQL stores fraud prediction results and investigation cases.

Cassandra stores customer complaints for lookup by complaint ID or transaction ID.

Streamlit provides dashboards for fraud monitoring, investigations, customer complaints, and management reporting.

Docker volumes keep database records after container restarts.

The storage and dashboard components are working locally and are prepared for integration with the team's Kafka, Spark, and machine learning pipeline.

2. Requirements

Docker Engine

Docker Compose

Git

Enough available memory to run MySQL and Cassandra

A web browser to access the dashboard

Python does not need to be installed separately on the host because the dashboard runs inside Docker.

3. Files

compose.yaml — Docker service configuration

.env.example — Example database credentials

mysql/init/01_schema.sql — MySQL initialization script

cassandra/init/01_schema.cql — Cassandra initialization script

dashboard/app.py — Streamlit dashboard application

dashboard/Dockerfile — Dashboard Docker image configuration

dashboard/requirements.txt — Python dependencies

dashboard/.streamlit/config.toml — Streamlit configuration

CONNECTIONS.md — Database connection information

4. Setup Instructions

Step 1 — Get the project

Clone the Bintou branch:

git clone -b Bintou https://github.com/abdull254/AfriMarket.git
cd AfriMarket

If the repository is already cloned, fetch and merge the latest changes from the Bintou branch into your working branch.

Step 2 — Configure database credentials

cp .env.example .env
nano .env

Replace the placeholder passwords with your own passwords.

Do not upload .env to GitHub.

Step 3 — Build and start the services

docker compose up -d --build

Docker Compose starts MySQL, Cassandra, and the Streamlit dashboard.

On fresh database volumes:

MySQL creates the fraud database and its tables.

The cassandra-init service creates the Cassandra keyspace and complaint tables.

Cassandra may take several minutes to become healthy.

Step 4 — Verify the services

docker compose ps -a

Expected results:

MySQL: healthy

Cassandra: healthy

Dashboard: running

cassandra-init: Exited (0)

Step 5 — Open the dashboard

Inside the Ubuntu machine running Docker, open:

http://localhost:8501

The current Docker configuration binds the dashboard to 127.0.0.1:8501, so it is accessible from the Docker host rather than directly from another computer.

5. Database Structure

MySQL

Database: fraud

scores

TransactionID

TransactionDT

risk_score

decision

stored_at

investigation_cases

case_id

TransactionID

case_status

assigned_to

notes

created_at

updated_at

The decision field supports APPROVE, REVIEW, and BLOCK.

Cassandra

Keyspace: afrimarket

complaints_by_id

Used to retrieve a complaint using its complaint ID.

complaints_by_transaction

Used to retrieve complaints associated with a transaction.

The streaming pipeline must write new complaint records to both tables to support both lookup methods.

6. Streamlit Dashboard

The dashboard contains four main pages.

Overview

Displays fraud monitoring information, including:

Total transactions

Approved, reviewed, and blocked transactions

Decision distribution

Average risk score

Flagged transactions

Recent transaction records

Investigations

Allows analysts to:

View investigation cases

Select cases and review their details

Update case status, assigned analyst, and notes

Create investigation cases for flagged transactions

View related customer complaints

Complaints

Allows users to search Cassandra complaints using:

Complaint ID

Transaction ID

The page displays the complaint details returned by Cassandra.

Management

Provides a summary of fraud monitoring and investigation activity, including:

Total processed transactions

Approval and flagged rates

Average risk score

Decision distribution and rates

Investigation workload and case status summaries

Dashboard metrics are calculated from the records currently stored in the databases.

7. Connection Information

Service

Inside the AfriMarket Docker network

On the Ubuntu Docker host

MySQL

mysql:3306

127.0.0.1:3307

Cassandra

cassandra:9042

127.0.0.1:9042

Streamlit

dashboard:8501

http://localhost:8501

Spark must use compatible database connectors.

If Spark runs in a separate Docker Compose project, it must be connected to the appropriate Docker network.

See CONNECTIONS.md for additional connection details.

8. Verify the Database Tables

Check MySQL:

docker compose exec mysql mysql -u root -p -e "SHOW TABLES FROM fraud;"

Check Cassandra:

docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACE afrimarket;"

Check dashboard logs:

docker compose logs --tail=50 dashboard

9. Data Persistence

MySQL and Cassandra use Docker named volumes.

Database records remain stored when containers are restarted or recreated using the same volumes.

Do not use docker compose down -v unless you intentionally want to delete the database volumes.

The sample transactions, investigation cases, and complaints used during local testing are not automatically transferred to teammates through GitHub.






## 11. Updating an Existing Integration Environment

### Who Should Follow This Section?

**This section is only for teammates who have already installed and run a previous version of the Bintou dashboard and have an existing MySQL database.**

**If you have NEVER installed or run the Bintou dashboard before, SKIP this entire section.** Follow the fresh installation instructions in Section 4 instead. The latest database schema is already included, so no manual database updates are required.

If you have previously run the dashboard and want to update it while preserving your existing data, follow the steps below.

### Important: Protect Existing Data

Do not run `docker compose down -v`, delete Docker volumes, or reinitialize the databases.

The following steps are designed to preserve existing MySQL fraud scores, investigation records, and Cassandra complaints.

### Step 1 — Get the Latest Bintou Changes

From the AfriMarket project directory, if you are working directly on the Bintou branch:

```bash
git pull origin Bintou
```

If you are working on another branch, fetch the latest Bintou changes and integrate them using the team's Git workflow.

The latest updates include:

- Updated Streamlit fraud monitoring dashboard
- Automatic investigation case creation for new REVIEW transactions
- MySQL `analyst_decision` column
- Unique constraint preventing duplicate investigation cases

### Step 2 — Check and Update the Existing MySQL Schema

If the MySQL container is already running, connect to the database:

```bash
docker compose exec mysql mysql -u root -p fraud
```

Enter your MySQL root password when prompted.

If MySQL is not running, start it first:

```bash
docker compose up -d mysql
```

Then run the MySQL connection command above.

Once connected, inspect the investigation table:

```sql
SHOW COLUMNS FROM investigation_cases;
SHOW INDEX FROM investigation_cases;
```

**A. Add the analyst decision column (only if missing)**

If `analyst_decision` is not listed, run:

```sql
ALTER TABLE investigation_cases
ADD COLUMN analyst_decision ENUM('PENDING', 'APPROVE', 'DECLINE')
NOT NULL DEFAULT 'PENDING';
```

If the column already exists, skip this command.

**B. Check for duplicate investigation cases**

Before adding a unique constraint, run:

```sql
SELECT TransactionID, COUNT(*) AS case_count
FROM investigation_cases
GROUP BY TransactionID
HAVING COUNT(*) > 1;
```

If this query returns any rows, review and resolve the duplicates before continuing.

**C. Add the unique constraint (only if missing)**

If no duplicate transactions exist and `TransactionID` does not already have a unique index, run:

```sql
ALTER TABLE investigation_cases
ADD CONSTRAINT uq_investigation_transaction UNIQUE (TransactionID);
```

If the unique constraint already exists, skip this command.

**D. Verify the database changes**

Run:

```sql
SHOW COLUMNS FROM investigation_cases;
SHOW INDEX FROM investigation_cases;
```

Confirm that:

- `analyst_decision` exists and defaults to `PENDING`.
- `TransactionID` has a unique index.

Exit MySQL:

```sql
EXIT;
```

### Step 3 — Rebuild and Start the Updated Services

After completing any necessary database updates, run:

```bash
docker compose up -d --build
```

This rebuilds and starts the services using the latest code while preserving existing Docker volumes.

**Note:** Docker Compose does not automatically update an existing MySQL table when the initialization script changes. That is why Step 2 is necessary for previously initialized databases.

### Step 4 — Verify the Services

Check the containers:

```bash
docker compose ps -a
```

Check the dashboard logs:

```bash
docker compose logs --tail=50 dashboard
```

Open the Streamlit dashboard:

http://localhost:8501

Verify that the following pages load correctly:

- **Overview:** Fraud monitoring metrics, transaction counts, and decision distribution
- **Investigations:** Investigation cases, analyst decisions, assignments, and case statuses
- **Complaints:** Cassandra complaint searches
- **Management:** Fraud statistics and investigation summaries

If any page displays an error, capture the exact error message for troubleshooting.

### Step 5 — Understand Automatic Investigation Creation

The updated Spark streaming pipeline automatically creates investigation cases for newly processed transactions with a `REVIEW` decision.

For this feature to work:

- Spark must be running the updated streaming code.
- MySQL must contain the updated investigation schema.
- Transactions must be processed through the updated streaming pipeline.

Each transaction can have only one investigation case.

**Important:** Existing REVIEW transactions that were scored before the updated streaming pipeline was running will not automatically receive investigation cases simply because the schema was updated.

### Fresh Installations — No Migration Needed

If you have never installed or run the Bintou dashboard, skip all the update steps above.

Follow Section 4 of this README to set up the dashboard normally.

The updated `mysql/init/01_schema.sql` already includes:

- `analyst_decision` with a default value of `PENDING`
- A unique constraint on `TransactionID`

These are created automatically when MySQL initializes a fresh database.

### Final Reminder

Never delete existing Docker volumes just to update the dashboard or database schema.

Updating the schema addresses the missing `analyst_decision` column and duplicate-case protection. Any unrelated dashboard errors should be investigated separately.
