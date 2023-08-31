import os
import logging
import mysql.connector
import mysql
from dotenv import load_dotenv
from rm_api import get_download_url

load_dotenv()

logging.basicConfig(level=logging.INFO)

HOST = os.getenv("RM_DB_HOST")
USER = os.getenv("RM_DB_USER")
DATABASE = os.getenv("RM_DB_DATABASE")
DB_PASSWORD = os.getenv("RM_DB_PASSWORD")

FILE_METADATA_FIELDS = ['FileID', 'Description', 'CreateDate', 'UpdateDate', 'CreateUserID', 'UpdateUserID']

ENTITY_TYPES = {
    'Property': 3,
    'Unit': 4,
    'Bill': 28,
    'Journal': 24,
    'Noncommercial Lease': 30,
    'Check': 111,
    'CC Trans': 38,
    'Deposit': 118,
    'Resident': 1,
    'Contact': 8,
    'Owner': 7,
    'Vendor': 6,
    'Prospect': 2,
}


def query_database_for_ids(table_name):
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        database=DATABASE,
        password=DB_PASSWORD
    )

    cursor = mydb.cursor()

    cursor.execute("""select EntityKeyID from fileattachments 
        inner join files on fileattachments.FileID = files.FileID
        where EntityTypeID = """ + str(ENTITY_TYPES[table_name]) + """ LIMIT 1;""")
    records = cursor.fetchall()
    mydb.close()
    return records


def process_files(entity_name):
    dataset = {
        "payload": query_database_for_ids(entity_name)  # Fetch these from your database
    }

    get_file_url = get_download_url(url='/' + entity_name + 's?embeds=FileAttachments',
                                    source_key='EntityKeyID',
                                    entity_key=f'{entity_name}ID',
                                    paths_to_files=[['FileAttachments', 'File']])

    if entity_name == 'Deposit':
        get_file_url = get_download_url(url='/Deposits?embeds=FileAttachments',
                                        source_key='EntityKeyID',
                                        entity_key=f'{entity_name}ID',
                                        paths_to_files=[['FileAttachments', 'File']])
    elif entity_name == 'Check':
        get_file_url = get_download_url(url='/Checks?embeds=FileAttachments',
                                        source_key='EntityKeyID',
                                        entity_key='FileAttachments.EntityKeyID',
                                        paths_to_files=[['FileAttachments', 'File']])

    try:
        get_file_url(dataset)
        print("Download completed.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    for entity_type in ['Deposit', 'Check', 'History', 'Units',
                        'SignableDocuments']:
        process_files(entity_type)

    # for entity_type in ['Deposit', 'Bill', 'Check', 'Inspections', 'InspectionAreaItems', 'History', 'Units',
    #                     'SignableDocuments']:

