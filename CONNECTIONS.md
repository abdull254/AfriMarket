# AfriMarket Database Connections

## Cassandra

Keyspace: afrimarket
Port: 9042

Tables:
- complaints_by_id
- complaints_by_transaction

### Spark running inside Docker

Host: cassandra
Port: 9042

The Spark container must be connected to the
AfriMarket Docker network.

### Spark running directly on Ubuntu

Host: 127.0.0.1
Port: 9042

### Spark configuration

Use environment variables:

CASSANDRA_HOST
CASSANDRA_PORT

The Spark application should read these values
instead of hardcoding a database address.

A compatible Spark Cassandra connector is required.

## MySQL

Database: fraud

Docker host: mysql
Docker port: 3306

Ubuntu host: 127.0.0.1
Ubuntu port: 3307

Tables:
- scores
- investigation_cases

Credentials are stored privately in .env.
Do not commit passwords to GitHub.
