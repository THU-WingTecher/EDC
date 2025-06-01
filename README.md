# EDC

Equivalent Data Construction (EDC) is a testing framework designed to detect logic bugs in database management systems by leveraging data-level equivalence. it precomputes the results of selected expressions and stores them in an equivalent table. During testing, EDC systematically replaces expressions in queries with their precomputed equivalents and compares the outputs of the original and transformed queries. Any discrepancy in results signals a potential logic error in the DBMS, such as incorrect type coercion, expression evaluation, or aggregation behavior. By focusing on data semantics rather than query structure, EDC provides a lightweight, generalizable, and SQL-dialect-independent approach for identifying subtle logic bugs across diverse DBMS platforms.

# Getting Started

Requirements:

* Python
* The corresponding DBMSs for testing, check the config file to make sure EDC gets the right connection
* Create a database named 'test' in the corresponding DBMS

Running the following commands to start EDC:

```
cd src
python main.py mysql
```

Other suported parameters: mysql, mariadb, clickhouse, tidb, percona, oceanbase.

Output files are stored under ./res/{db}, and logs are stored under ./log/{db}.
