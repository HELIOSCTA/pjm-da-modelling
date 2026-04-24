import csv
import io
import os
import pytz
from typing import List
from datetime import date as datetime_date
from datetime import time as datetime_time

import numpy as np
import pandas as pd
import psycopg2

# ignore warnings
import warnings
warnings.simplefilter(action='ignore', category=Warning)

# init logging
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger().handlers[0].setLevel(logging.DEBUG)

from backend import (
    credentials,
)

"""
"""


def _connect_to_azure_postgressql(
        database: str = "helioscta",
    ) -> psycopg2.extensions.connection:
    """
    """
    connection = psycopg2.connect(
        user=credentials.AZURE_POSTGRESQL_DB_USER,
        password=credentials.AZURE_POSTGRESQL_DB_PASSWORD,
        host=credentials.AZURE_POSTGRESQL_DB_HOST,
        port=credentials.AZURE_POSTGRESQL_DB_PORT,
        dbname=database,
    )
    return connection


def pull_from_db(
        query: str,
        database: str = 'helioscta',
    ) -> pd.DataFrame:

    try: 
        # Create a database connection
        connection = _connect_to_azure_postgressql(database=database)
        
        # Execute the query and fetch the data
        # logging.info(query)
        df = pd.read_sql(query, connection)
        # logging.info(f"Pulled {len(df):,} rows ...")

        # close connection
        connection.close()
    
        return df

    except Exception as e:
        # logging.info(e)
        logging.info(e)
        return None


def infer_sql_data_types(df: pd.DataFrame) -> List[str]:

    def _infer_sql_data_type(df: pd.DataFrame, col: str):
        if isinstance(df[col].loc[0], str):
            return 'VARCHAR'
        elif isinstance(df[col].loc[0], int) or isinstance(df[col].loc[0], np.int64) or isinstance(df[col].loc[0], np.int32):
            return 'INTEGER'
        elif isinstance(df[col].loc[0], float) or isinstance(df[col].loc[0], np.float64) or isinstance(df[col].loc[0], np.float32):
            return 'FLOAT'
        elif isinstance(df[col].loc[0], bool) or isinstance(df[col].loc[0], np.bool_):
            return 'BOOLEAN'
        elif isinstance(df[col].loc[0], pd.Timestamp):
            return 'TIMESTAMP'
        elif isinstance(df[col].loc[0], datetime_date):
            return 'DATE'
        elif isinstance(df[col].loc[0], datetime_time):
            return 'VARCHAR'
        else:
            logging.info(f"{col} dtype: {type(df[col].loc[0])}")
            raise NotImplementedError

    return [f"{_infer_sql_data_type(df=df, col=col)}" for col in df.columns]


def get_table_dtypes(
        schema: str,
        table_name: str,
        database: str = "helioscta",
    ) -> List[str]:
    """
    """
    # connect to db
    connection = _connect_to_azure_postgressql(database=database)
    
    query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
            AND table_schema = '{schema}';
    """
    
    # Use pandas to read the SQL query into a DataFrame
    df = pd.read_sql(query, connection)

    # close session
    connection.close()

    # return dtypes
    dtypes = df["data_type"].tolist()
    # logging.info(f"DTYPES ... {[f'{column}: {dtype}' for column, dtype in zip(df['column_name'].tolist(), df['data_type'].tolist())]}")

    return dtypes


def get_table_primary_keys(
        schema: str,
        table_name: str,
        database: str = "helioscta",
    ) -> List[str]:
    """
    """
    # Create a database connection
    connection = _connect_to_azure_postgressql(database=database)

    # ERROR: sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) connection to server at "heliosctadb.postgres.database.azure.com" (13.91.217.56), port 5432 failed: FATAL:  remaining connection slots are reserved for roles with privileges of the "pg_use_reserved_connections" role
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
    
    # Use pandas to read the SQL query into a DataFrame
    df = pd.read_sql(query, connection)

    # close session
    connection.close()

    # Return only the column names that are primary keys
    primary_keys = df[df['is_primary_key'] == 'YES']['column_name'].tolist()
    # logging.info(f"PRIMARY KEYS ... {primary_keys}")

    return primary_keys


def _get_query_create_table(
        schema: str,
        table_name: str,
        columns: List[str],
        data_types: List[str],
        primary_key: List[str],
    ) -> str:
    """
    with connection.cursor() as cursor:
        cursor.execute(f"
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table}(
                iud                 Varchar(2)       not null,
                location_role_id    Integer          not null,
                gas_day             Date             not null,
                cycle_code          Varchar(10)      not null,
                role_code           Varchar(10)      not null,
                operational_cap     Numeric(18,0)    not null,
                available_cap       Numeric(18,0)    not null,
                scheduled_cap       Numeric(18,0)    not null,
                design_cap          Numeric(18,0)    not null,
                units               Varchar(10)      not null,
                update_timestamp    TIMESTAMP        not null,
                insert_date         TIMESTAMP        not null,
                PRIMARY KEY (iud, location_role_id, gas_day)
                )
        "
        )
        connection.commit()
    """
    
    columns_dtypes_str = ', '.join([f"{col} {dtype}" for col, dtype in zip(columns, data_types)])
    primary_key_str = ', '.join([f"{col}" for col in primary_key])

    audit_columns_sql = (
        ", created_at TIMESTAMPTZ DEFAULT "
        "(CURRENT_TIMESTAMP AT TIME ZONE 'America/Edmonton'), "
        "updated_at TIMESTAMPTZ DEFAULT "
        "(CURRENT_TIMESTAMP AT TIME ZONE 'America/Edmonton')"
    )

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
            {columns_dtypes_str}{audit_columns_sql},
            PRIMARY KEY ({primary_key_str})
        );
    """

    # logging.info("Prepared SQL Query to create tables ...")
    # logging.info(create_table_query)

    return create_table_query


def _get_query_upsert(
        schema: str,
        table_name: str,
        columns: List[str],
        data_types: List[str],
        primary_key: List[str],
    ) -> str:
    """
    """

    columns_str = ', '.join([f"{col}" for col in columns])
    source_columns_str = ', '.join([f"source.{col}" for col in columns])
    primary_key_str = ', '.join([f"{col}" for col in primary_key])
    update_set_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns])

    insert_columns = f"{columns_str}, created_at, updated_at"
    select_columns = (
        f"{source_columns_str}, NOW() AT TIME ZONE 'America/Edmonton', "
        "NOW() AT TIME ZONE 'America/Edmonton'"
    )
    update_set_str = (
        f"{update_set_str}, "
        "updated_at = NOW() AT TIME ZONE 'America/Edmonton'"
    )

    upsert_query = f""" 
        INSERT INTO {schema}.{table_name} ({insert_columns})
        SELECT {select_columns} FROM {schema}.temp_{table_name} AS source
        ON CONFLICT ({primary_key_str}) 
        DO UPDATE SET
            {update_set_str}
        ;
    """

    return upsert_query


def upsert_to_azure_postgresql(
        schema: str,
        table_name: str,
        df: pd.DataFrame, 
        columns: List[str],
        primary_key: List[str],
        data_types: List[str] = None,
        database: str = "helioscta",
    ) -> bool:
    """
    """

    # TODO: null values
    df = df.fillna(0)

    # infer data types
    if not data_types: data_types = infer_sql_data_types(df=df)

    create_temp_table_query = _get_query_create_table(
        schema=schema,
        table_name=f"temp_{table_name}",
        columns=columns,
        data_types=data_types,
        primary_key=primary_key,
    )
    
    create_table_query = _get_query_create_table(
        schema=schema,
        table_name=table_name,
        columns=columns,
        data_types=data_types,
        primary_key=primary_key,
    )

    upsert_query = _get_query_upsert(
        schema=schema,
        table_name=table_name,
        columns=columns,
        data_types=data_types,
        primary_key=primary_key,
    )

    try:
        # Create connection to Azure PostgreSQL
        connection = _connect_to_azure_postgressql(database=database)
        cursor = connection.cursor()

        # create schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # create table
        cursor.execute(create_temp_table_query)
        cursor.execute(create_table_query)

        # Add this before the COPY operation
        df_temp = df.copy()
        mst = pytz.timezone('America/Edmonton')
        df_temp['created_at'] = pd.Timestamp.now(tz=mst)
        df_temp['updated_at'] = pd.Timestamp.now(tz=mst)
        # push data to temp table
        sio = io.StringIO()
        sio.write(df_temp.to_csv(index=False, header=False, quoting=csv.QUOTE_NONNUMERIC,sep=','))  # Write the Pandas DataFrame as a csv to the buffer
        sio.seek(0)  # Be sure to reset the position to the start of the stream
        cursor.copy_expert(f"""COPY {schema}.temp_{table_name} FROM STDIN WITH (FORMAT CSV)""", sio)

        # upsert
        cursor.execute(upsert_query)
        
        # logging
        logging.info(f"Upserted {len(df)} rows into {schema}.{table_name} ...")
        # logging.info(f"\tPrimary Keys: {primary_key}")
        # logging.info(f"\tColumns: {columns}")
        # for col, dtype in zip(columns, data_types):
        #     logging.info(f"\t{col}: {dtype}")
                
        # delete temp table
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.temp_{table_name}")

        # commit changes
        connection.commit()

        # close connection
        if cursor: cursor.close()
        if connection: connection.close()
    
    except Exception as e:
        logging.error(f"Error upserting data into Azure PostgreSQL: {e}")
        raise


"""
"""

if __name__ == "__main__":
    print(f"BACKEND_ENV={credentials.BACKEND_ENV}")
    print(f"HOST={credentials.AZURE_POSTGRESQL_DB_HOST}")
    print(f"USER={credentials.AZURE_POSTGRESQL_DB_USER}")
    print()
    conn = _connect_to_azure_postgressql()
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