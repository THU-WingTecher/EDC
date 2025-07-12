# EDC

Equivalent Data Construction (EDC) is a testing framework designed to detect logic bugs in database management systems by leveraging data-level equivalence. it precomputes the results of selected expressions and stores them in an equivalent table. During testing, EDC systematically replaces expressions in queries with their precomputed equivalents and compares the outputs of the original and transformed queries. Any discrepancy in results signals a potential logic error in the DBMS, such as incorrect type coercion, expression evaluation, or aggregation behavior. By focusing on data semantics rather than query structure, EDC provides a lightweight, generalizable, and SQL-dialect-independent approach for identifying subtle logic bugs across diverse DBMS platforms.

# Getting Started

## Requirements

* Python
* Docker and Docker Compose
* The corresponding DBMSs for testing, check the config file to make sure EDC gets the right connection
* Create a database named 'test' in the corresponding DBMS

## Using Docker

1. Clone the repository:

```bash
git clone <repository-url>
cd edc
```

2. Start the services:

```bash
docker-compose up -d
```

3. Enter the EDC container:

```bash
docker-compose exec edc-app bash
```

4. Run tests:

```bash
cd src

# Test MySQL
python main.py mysql

# Test MariaDB
python main.py mariadb

# Test Percona
python main.py percona

# Test Dameng
python main.py dameng
```

5. View results:

* Test results are stored in `./res/{db}/`
* Logs are stored in `./log/{db}/`

6. Stop services:

```bash
docker-compose down
```

## Manual Installation

If you prefer to run EDC without Docker:

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

2. Run EDC:

```bash
cd src
python main.py mysql
```

Other supported parameters: mysql, mariadb, clickhouse, tidb, percona, oceanbase.

Output files are stored under ./res/{db}, and logs are stored under ./log/{db}.

## Configuration

Database connection settings can be found in `src/config/conn.ini`. Make sure to update these settings according to your environment.

## Troubleshooting

1. If you can't connect to the database:

   * Check if the database service is running
   * Verify the connection settings in `src/config/conn.ini`
   * Make sure the database user has proper permissions

# Code Structure

* `seed/`: This directory stores database-specific metadata. Each subfolder (e.g., `mysql/`) contains definitions for supported data types (`type/`), function (`func/`), predicate (`pred/`), and aggregate (`agg/`) operations. 
* `conn/`: Manages database connections. base.py defines the abstract base_connection interface. To connect to a new database, users implement this interface (e.g., mysql.py) by defining methods like create_conn and execute to handle database-specific connection and query execution.
* `sql/`: Responsible for SQL query generation. `expr_generator.py` handles the generation of complex expressions for WHERE and HAVING clauses. `sql_generator.py` builds the overall SELECT query structure, including the transformation logic for equivalent queries. For more advanced SQL generation or to support DBMS-specific syntax, developers can extend or customize the logic within these files.
