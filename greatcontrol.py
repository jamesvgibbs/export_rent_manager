import os
import csv
import logging
import boto3
import glob
from tqdm import tqdm
from typing import Dict, Tuple, List
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

bucket_name = "gj-etl-db-csv"

__MIN_CONNECTIONS = int(os.getenv("MIN_DB_CONNECTIONS", "0"))
__MAX_CONNECTIONS = int(os.getenv("MAX_DB_CONNECTIONS", "5"))

PoolKey = Tuple[str, str, str, str]

__POOLS: Dict[PoolKey, ThreadedConnectionPool] = {}


def remove_all_csv_files(folder):
    csv_files = glob.glob(f'{folder}/*.csv')
    for csv_file in csv_files:
        try:
            os.remove(csv_file)
            logging.info(f"Successfully removed {csv_file}")
        except Exception as e:
            logging.error(f"Failed to remove {csv_file}. Error: {e}")


def check_file_exists_in_s3(bucket, object_name, folder):
    s3 = boto3.client('s3')
    full_object_name = f"{folder}/{object_name}" if folder else object_name
    try:
        s3.head_object(Bucket=bucket, Key=full_object_name)
        return True
    except Exception as e:
        return False


def upload_file_to_s3(file_name, bucket, object_name=None):
    s3 = boto3.client('s3')

    if object_name is None:
        object_name = file_name

    try:
        s3.upload_file(file_name, bucket, object_name)
        logging.info(f"Successfully uploaded {file_name} to {bucket}/{object_name}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload {file_name} to {bucket}/{object_name}. Error: {e}")
        return False


def ensure_bucket_exists(bucket_name, region="us-east-2"):
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        logging.info(f"Bucket {bucket_name} does not exist. Attempting to create...")
        try:
            if region is None:
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            logging.info(f"Successfully created bucket {bucket_name}.")
        except Exception as create_bucket_exception:
            logging.error(f"Failed to create bucket {bucket_name}. Aborting. Error: {create_bucket_exception}")
            exit(1)


def table_to_csv(cursor, schema_name, table_name, batch_size=1000, max_rows_per_shard=200000):
    generated_files = []
    offset = 0
    total_rows = cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
    total_rows = cursor.fetchone()[0]
    num_shards = total_rows // max_rows_per_shard + (1 if total_rows % max_rows_per_shard != 0 else 0)
    for shard_num in range(1, num_shards + 1):
        offset = (shard_num - 1) * max_rows_per_shard
        shard_pbar = tqdm(total=min(max_rows_per_shard, total_rows - offset), desc=f"Processing {table_name} (shard {shard_num})")
        
        csv_file_name = f"{table_name}_shard_{shard_num}.csv"
        generated_files.append(csv_file_name)
        logging.info(f"Creating CSV from table {table_name}, shard {shard_num}")
        
        with open(csv_file_name, "w") as f:
            csv_writer = csv.writer(f)
            while True:
                cursor.execute(f"SELECT * FROM {schema_name}.{table_name} LIMIT {batch_size} OFFSET {offset}")
                rows = cursor.fetchall()
                if not rows:
                    break
                if offset == (shard_num - 1) * max_rows_per_shard:  # first batch of the shard
                    csv_writer.writerow([desc[0] for desc in cursor.description])  # header
                csv_writer.writerows(rows)
                shard_pbar.update(len(rows))
                offset += batch_size
        shard_pbar.close()
    
    return generated_files


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


def postgres_to_csv(schema_name, batch_size=1000):
    logging.info(f"Exporting all tables from schema {schema_name}...")

    with get_cursor(database=database, user=user, password=password, host=host) as cursor:
        cursor.execute(f"""SELECT
                table_name,
                pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name)) AS size
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            ORDER BY size;""")
        tables = cursor.fetchall()

        total_tables = len(tables)
        logging.info(f"Total tables to export: {total_tables}")

        output_folder = schema_name
        os.makedirs(output_folder, exist_ok=True)

        ensure_bucket_exists(bucket_name)
        
        pbar = tqdm(total=total_tables, desc="Exporting tables")
        for i, table in enumerate(tables):
            table_name = table[0]
            try:
                generated_files = table_to_csv(cursor, schema_name, table_name, batch_size)
                for csv_file_name in generated_files:
                    csv_file_name_to_check = csv_file_name  # Update this if needed

                    if check_file_exists_in_s3(bucket_name, csv_file_name_to_check, schema_name):
                        logging.info(f"Skipping {csv_file_name}, already uploaded to S3.")
                        continue

                    full_csv_path = os.path.join(output_folder, csv_file_name)
                    os.rename(csv_file_name, full_csv_path)

                    if upload_file_to_s3(full_csv_path, bucket_name):
                        if os.path.exists(full_csv_path):
                            os.remove(full_csv_path)
                        else:
                            logging.warning(f"{csv_file_name} does not exist on the local filesystem.")

                logging.info(f"Progress: {i + 1}/{total_tables} tables exported.")
            except Exception as e:
                logging.error(f"Failed to export table {table_name}. Error: {e}")
            pbar.update(1)
        pbar.close()

        logging.info("All tables exported.")


if __name__ == "__main__":
    remove_all_csv_files('transactional')
    postgres_to_csv('transactional')

    # postgres_to_csv('county_deeds_public')
    # postgres_to_csv('greatcontrol')
    # postgres_to_csv('zendesk')
    # postgres_to_csv('rent_manager')

    # remove_all_csv_files('latchel')
    # postgres_to_csv('latchel')
    # remove_all_csv_files('rently')
    # postgres_to_csv('rently')
    # remove_all_csv_files('meld')
    # postgres_to_csv('meld')
    # remove_all_csv_files('tools')
    # postgres_to_csv('tools')
