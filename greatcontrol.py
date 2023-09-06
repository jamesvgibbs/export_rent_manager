import os
import csv
import zipfile
import logging
from typing import Dict, Tuple
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import DictCursor
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

database = os.getenv("WH_DB_DATABASE")
user = os.getenv("WH_DB_USER")
password = os.getenv("WH_DB_PASSWORD")
host = os.getenv("WH_DB_HOST")
port = os.getenv("WH_PORT", 5432)

__MIN_CONNECTIONS = int(os.getenv("MIN_DB_CONNECTIONS", "0"))
__MAX_CONNECTIONS = int(os.getenv("MAX_DB_CONNECTIONS", "5"))

PoolKey = Tuple[str, str, str, str]

__POOLS: Dict[PoolKey, ThreadedConnectionPool] = {}


def table_to_csv(cursor, schema_name, table_name, batch_size=1000):
    offset = 0
    csv_file_name = f"{table_name}.csv"
    with open(csv_file_name, "w") as f:
        csv_writer = csv.writer(f)
        while True:
            cursor.execute(f"SELECT * FROM {schema_name}.{table_name} LIMIT {batch_size} OFFSET {offset}")
            rows = cursor.fetchall()
            if not rows:
                break
            if offset == 0:
                csv_writer.writerow([desc[0] for desc in cursor.description])  # header
            csv_writer.writerows(rows)
            offset += batch_size
    return csv_file_name


class ThreadedConnection(object):
    def __init__(self, pool):
        self.pool = pool

    def __enter__(self):
        self.conn = self.pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.putconn(self.conn)


def get_conn(*, database, user, password, host):
    logging.info(f"Connecting to database: {database}, user: {user}, host: {host}, port: {port}")

    key = (database, user, password, host)
    __POOLS[key] = ThreadedConnectionPool(__MIN_CONNECTIONS,
                                          __MAX_CONNECTIONS,
                                          user=user,
                                          password=password,
                                          host=host,
                                          port=port,
                                          database=database)
    conn = ThreadedConnection(__POOLS[key])
    return conn


@contextmanager
def get_cursor(conn=None, cursor_factory=DictCursor, **kwargs):
    logging.info(f"Getting cursor with factory {cursor_factory}...")
    local_conn = conn or get_conn(**kwargs)
    with local_conn as actual_conn:
        cursor = actual_conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            actual_conn.commit()
        finally:
            cursor.close()


def postgres_to_csv(schema_name, zip_files=False, batch_size=1000):
    logging.info(f"Exporting all tables from schema {schema_name}...")

    with get_cursor(database=database, user=user, password=password, host=host) as cursor:
        cursor.execute(f"""SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}';""")
        tables = cursor.fetchall()

        total_tables = len(tables)
        logging.info(f"Total tables to export: {total_tables}")

        output_folder = "csv_files"
        os.makedirs(output_folder, exist_ok=True)

        if zip_files:
            zip_file_name = f"all_{schema_name}_tables.zip"
            with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for i, table in enumerate(tables):
                    table_name = table[0]
                    try:
                        csv_file_name = table_to_csv(cursor, schema_name, table_name, batch_size)
                        zipf.write(csv_file_name, os.path.join(output_folder, csv_file_name))
                        os.remove(csv_file_name)
                        logging.info(f"Progress: {i + 1}/{total_tables} tables exported.")
                    except Exception as e:
                        logging.error(f"Failed to export table {table_name}. Error: {e}")
        else:
            for i, table in enumerate(tables):
                table_name = table[0]
                try:
                    csv_file_name = table_to_csv(cursor, schema_name, table_name, batch_size)
                    os.rename(csv_file_name, os.path.join(output_folder, csv_file_name))
                    logging.info(f"Progress: {i + 1}/{total_tables} tables exported.")
                except Exception as e:
                    logging.error(f"Failed to export table {table_name}. Error: {e}")

        logging.info("All tables exported.")


if __name__ == "__main__":
    postgres_to_csv('transactional')
