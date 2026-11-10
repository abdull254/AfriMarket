# AfriMarket — Database & Storage Setup

**Contributor:** Bintou Ba  
**Project:** CIS 641 — AfriMarket Fraud Detection System

## 1. Overview

This part of the project sets up MySQL and Cassandra using
Docker Compose.

- MySQL stores fraud prediction results and investigation cases.
- Cassandra stores customer complaints for lookup by complaint ID
  or transaction ID.
- Docker volumes keep database records after container restarts.

The databases are prepared for integration with the team's
Kafka and Spark processing pipeline.

## 2. Requirements

- Docker Engine
- Docker Compose
- Git
- Enough available memory to run MySQL and Cassandra

## 3. Files

- `compose.yaml` — Docker service configuration
- `.env.example` — Example database credentials
- `mysql/init/01_schema.sql` — MySQL initialization script
- `cassandra/init/01_schema.cql` — Cassandra initialization script
- `CONNECTIONS.md` — Database connection information

## 4. Setup Instructions

### Step 1 — Get the project

Clone the Bintou branch:

```bash
git clone -b Bintou https://github.com/abdull254/AfriMarket.git
cd AfriMarket
```

### Step 2 — Configure MySQL credentials

```bash
cp .env.example .env
nano .env
```

Replace the placeholder passwords with your own passwords.

Do not upload `.env` to GitHub.

### Step 3 — Start the databases

```bash
docker compose up -d
```

Docker will start MySQL and Cassandra.

On a fresh database volume:
- MySQL creates the `fraud` database and its tables.
- The `cassandra-init` service creates the Cassandra keyspace
  and complaint tables.

### Step 4 — Verify the services

```bash
docker compose ps -a
```

Expected results:
- MySQL: healthy
- Cassandra: healthy
- cassandra-init: Exited (0)

Cassandra can take a few minutes to initialize.

## 5. Database Structure

### MySQL

Database: `fraud`

**scores**
- TransactionID
- TransactionDT
- risk_score
- decision
- stored_at

**investigation_cases**
- case_id
- TransactionID
- case_status
- assigned_to
- notes
- created_at
- updated_at

The decision field supports APPROVE, REVIEW, and BLOCK.

### Cassandra

Keyspace: `afrimarket`

**complaints_by_id**

Used to retrieve a complaint using its complaint ID.

**complaints_by_transaction**

Used to retrieve complaints associated with a transaction.

Both tables contain complaint information. The streaming
pipeline must write to both tables when new complaints arrive.

## 6. Connection Information

| Database | Spark inside the AfriMarket Docker network | Application running on Ubuntu |
|---|---|---|
| MySQL | `mysql:3306` | `127.0.0.1:3307` |
| Cassandra | `cassandra:9042` | `127.0.0.1:9042` |

Spark must use a compatible database connector.

If Spark runs in a separate Docker Compose project, it must
be connected to the same Docker network as the databases.

See `CONNECTIONS.md` for more details.

## 7. Verify the Database Tables

Check MySQL:

```bash
docker compose exec mysql mysql -u root -p -e "SHOW TABLES FROM fraud;"
```

Check Cassandra:

```bash
docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACE afrimarket;"
```

## 8. Data Persistence

MySQL and Cassandra use Docker named volumes.

Database records remain stored when containers are restarted
or recreated using the same volumes.

Do not use `docker compose down -v` unless you intentionally
want to delete the database volumes.

## 9. Current Status

Completed:
- MySQL Docker setup and schema
- Cassandra Docker setup and schema
- Automatic Cassandra schema initialization
- Database persistence testing
- Local Ubuntu and Docker connection configuration

Pending team integration:
- Connect Spark to MySQL and Cassandra
- Stream complaint records from Kafka into Cassandra
- Store Spark fraud predictions in MySQL
- Connect stored results to the analyst dashboard
- Test the complete pipeline
