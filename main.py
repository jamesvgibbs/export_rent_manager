import os
import mysql.connector
import csv
import zipfile
import logging


logging.basicConfig(level=logging.INFO)


def table_to_csv(cursor, table_name):
    logging.info(f"Exporting table {table_name}...")
    cursor.execute(f"SELECT * FROM {table_name}")

    result = cursor.fetchall()

    csv_file_name = f"{table_name}.csv"

    # Write data to CSV
    with open(csv_file_name, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in cursor.description])  # Write headers
        writer.writerows(result)  # Write data

    logging.info(f"Table {table_name} exported successfully.")
    return csv_file_name


def mysql_to_csv():
    mydb = mysql.connector.connect(
        host="noho.oa.rentmanager.com",
        user="noho",
        database="noho_rm12",
        password="bO7RaphI"
    )

    cursor = mydb.cursor()

    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    total_tables = len(tables)
    logging.info(f"Total tables to export: {total_tables}")

    os.makedirs("csv_files", exist_ok=True)

    zip_file_name = "all_tables.zip"
    with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for i, table in enumerate(tables):
            table_name = table[0]
            try:
                csv_file_name = table_to_csv(cursor, table_name)
                zipf.write(csv_file_name, os.path.join("csv_files", csv_file_name))
                os.remove(csv_file_name)
                logging.info(f"Progress: {i + 1}/{total_tables} tables exported.")
            except Exception as e:
                logging.error(f"Failed to export table {table_name}. Error: {e}")

    mydb.close()
    logging.info("All tables exported and zipped.")


if __name__ == "__main__":
    mysql_to_csv()
