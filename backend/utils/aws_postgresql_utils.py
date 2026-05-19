"""
Read-only AWS PostgreSQL client.

Mirrors the read-side surface of `azure_postgresql_utils.py` against the
AWS RDS instance reached with the `helios_readonly_analyst_1` role.
Writes (upsert / DDL helpers) are intentionally omitted — the role is
read-only and any INSERT/CREATE/UPDATE would fail at the server anyway.
"""

import logging
import warnings
from typing import List

import pandas as pd
import psycopg2

from backend import credentials

warnings.simplefilter(action="ignore", category=Warning)

logging.basicConfig(level=logging.DEBUG)
logging.getLogger().handlers[0].setLevel(logging.DEBUG)


def _connect_to_aws_postgresql(
    database: str = None,
) -> psycopg2.extensions.connection:
    """Open a read-only connection to the AWS RDS instance."""
    connection = psycopg2.connect(
        user=credentials.AWS_POSTGRESQL_DB_USER,
        password=credentials.AWS_POSTGRESQL_DB_PASSWORD,
        host=credentials.AWS_POSTGRESQL_DB_HOST,
        port=credentials.AWS_POSTGRESQL_DB_PORT,
        dbname=database or credentials.AWS_POSTGRESQL_DB_NAME,
        sslmode=credentials.AWS_POSTGRESQL_DB_SSLMODE,
    )
    connection.set_session(readonly=True)
    return connection


def pull_from_db(
    query: str,
    database: str = None,
) -> pd.DataFrame:
    """Run a SELECT against AWS RDS and return the result as a DataFrame."""
    try:
        connection = _connect_to_aws_postgresql(database=database)
        df = pd.read_sql(query, connection)
        connection.close()
        return df
    except Exception as e:
        logging.info(e)
        return None


def get_table_dtypes(
    schema: str,
    table_name: str,
    database: str = None,
) -> List[str]:
    """Return the data types of every column in `{schema}.{table_name}`."""
    connection = _connect_to_aws_postgresql(database=database)
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
            AND table_schema = '{schema}';
    """
    df = pd.read_sql(query, connection)
    connection.close()
    return df["data_type"].tolist()


def get_table_primary_keys(
    schema: str,
    table_name: str,
    database: str = None,
) -> List[str]:
    """Return the primary-key columns of `{schema}.{table_name}`."""
    connection = _connect_to_aws_postgresql(database=database)
    query = f"""
        SELECT c.column_name, c.data_type,
            CASE WHEN kcu.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_primary_key
        FROM information_schema.columns c
        LEFT JOIN information_schema.key_column_usage kcu
            ON c.column_name = kcu.column_name
            AND kcu.table_name = '{table_name}'
            AND kcu.table_schema = '{schema}'
        WHERE c.table_name = '{table_name}'
            AND c.table_schema = '{schema}';
    """
    df = pd.read_sql(query, connection)
    connection.close()
    return df[df["is_primary_key"] == "YES"]["column_name"].tolist()


"""
"""

if __name__ == "__main__":
    print(f"HOST={credentials.AWS_POSTGRESQL_DB_HOST}")
    print(f"USER={credentials.AWS_POSTGRESQL_DB_USER}")
    print(f"DB={credentials.AWS_POSTGRESQL_DB_NAME}")
    print()
    conn = _connect_to_aws_postgresql()
    cur = conn.cursor()
    cur.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name NOT LIKE 'pg_%' "
        "AND schema_name != 'information_schema' "
        "ORDER BY schema_name"
    )
    for row in cur.fetchall():
        print(row[0])
    cur.close()
    conn.close()
