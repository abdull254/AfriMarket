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

10. Current Status

Completed

MySQL Docker setup and schema

Cassandra Docker setup and schema

Automatic Cassandra schema initialization

Database persistence testing

Streamlit dashboard Docker setup

Fraud monitoring Overview page

Investigation case creation and updates

Cassandra complaint lookup

Related complaints displayed in investigations

Management dashboard with decision rates and case summaries

Local testing using sample records

Pending Team Integration

Connect Spark to MySQL and Cassandra

Stream complaint records from Kafka into Cassandra

Store actual fraud prediction results from Spark/ML in MySQL

Test the dashboard with real pipeline outputs

Test the complete end-to-end fraud detection workflow

Perform final dashboard usability and reliability improvements

Note: The dashboard currently uses locally stored test records. Full pipeline integration is still in progress.
