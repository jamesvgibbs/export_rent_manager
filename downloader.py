import os
import urllib.request
from urllib.error import HTTPError
import mysql.connector
from dotenv import load_dotenv


DB_CONFIG = {
    'host': os.getenv("RM_DB_HOST"),
    'user': os.getenv("RM_DB_USER"),
    'database': os.getenv("RM_DB_DATABASE"),
    'password': os.getenv("RM_DB_PASSWORD"),
}

API_CONFIG = {
    'url': os.getenv("RM_API_URL"),
    'username': os.getenv("RM_API_USERNAME"),
    'password': os.getenv("RM_API_PASSWORD"),
    'location_id': os.getenv("RM_API_LOCATION_ID"),
}

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
    with mysql.connector.connect(**DB_CONFIG) as mydb:
        cursor = mydb.cursor()
        query = f"""SELECT EntityKeyID 
                    FROM fileattachments 
                    INNER JOIN files ON fileattachments.FileID = files.FileID
                    WHERE EntityTypeID = {ENTITY_TYPES[table_name]};"""
        cursor.execute(query)
        records = cursor.fetchall()
    return records


def fetch_file(url, file_name, folder):
    os.makedirs(folder, exist_ok=True)
    try:
        with urllib.request.urlopen(url) as source_file, open(os.path.join(folder, file_name), 'wb') as f:
            f.write(source_file.read())
    except HTTPError as e:
        if e.code != 404:
            raise e


def download_files(entity_key, download_url, metadata, folder):
    os.makedirs(folder, exist_ok=True)
    file_name = f"{metadata['FileID']}/{metadata['Name']}{metadata['Extension']}"
    fetch_file(download_url, file_name, folder)


def main():
    # Your API token management logic here
    # API_TOKEN = refresh_token()  # for example

    dataset = {
        "payload": query_database_for_ids("Deposit")
    }

    # Replace the following mock API response with your actual API call
    api_response = [
        {
            "DepositID": 19472,
            "FileAttachments": [
                {
                    "File": {
                        "FileID": 1892771,
                        "Name": "SampleFile",
                        "Extension": ".pdf",
                        "DownloadURL": "https://example.com/sample.pdf"
                    }
                }
            ]
        }
    ]

    for record in api_response:
        entity_key = record.get("DepositID")
        file_attachments = record.get("FileAttachments", [])
        for attachment in file_attachments:
            file_info = attachment.get("File", {})
            download_url = file_info.get("DownloadURL")
            if download_url:
                download_files(entity_key, download_url, file_info)


if __name__ == "__main__":
    main()