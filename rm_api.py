import json
import os
import logging
import urllib3
import urllib.request

from dotenv import load_dotenv
from urllib.error import HTTPError

load_dotenv()

logging.basicConfig(level=logging.INFO)
http = urllib3.PoolManager()

API_TOKEN = os.getenv("API_TOKEN")
API_URL = os.getenv("RM_API_URL")
API_CONFIG = {
    'username': os.getenv("RM_API_USERNAME"),
    'password': os.getenv("RM_API_PASSWORD"),
    'locationid': os.getenv("RM_API_LOCATION_ID"),
}
FILE_METADATA_FIELDS = ['FileID', 'Description', 'CreateDate', 'UpdateDate', 'CreateUserID', 'UpdateUserID']
FOLDER="rm_files"


def update_dotenv(key, new_value):
    # Read the existing .env file
    with open('.env', 'r') as file:
        lines = file.readlines()

    # Update the value if key exists
    updated = False
    for index, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[index] = f"{key}={new_value}\n"
            updated = True
            break

    # Append the key-value pair if key doesn't exist
    if not updated:
        lines.append(f"{key}={new_value}\n")

    # Write back to the .env file
    with open('.env', 'w') as file:
        file.writelines(lines)


def refresh_token():
    auth_params = json.dumps(API_CONFIG)
    headers = {
        'Content-Type': 'application/json'
    }
    try:
        resp = http.request('POST', f"{API_URL}/Authentication/AuthorizeUser", body=auth_params, headers=headers)
        if resp.status == 200:
            token = resp.data.decode('utf-8')
            token = token[1:-1]
            if 'error' not in token:
                update_dotenv("API_TOKEN", token)
                return token
            else:
                logging.error(f"Error in API response: {token['error']}")
                return None
        else:
            logging.info(f"Received non-200 status code: {resp.status}")
            return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None


def rm_api_request(url):
    global API_TOKEN
    try:
        if not API_TOKEN:
            logging.info("Refreshing API token")
            API_TOKEN = refresh_token()
        content = resolve_http_report(url, headers=make_rm_headers())
    except ReportFetchException as e:
        if e.status == 401:
            API_TOKEN = refresh_token()
        else:
            raise e
        content = resolve_http_report(url, headers=make_rm_headers())

    return content


def make_rm_headers():
    return {
        "X-RM12Api-ApiToken": API_TOKEN
    }


class ReportFetchException(Exception):
    def __init__(self, response):
        self.response = response
        self.status = response.status


def resolve_http_report(url, method="GET", body=None, headers=None, fields=None):
    resp = http.request(method, url, fields=fields, headers=headers, body=body)
    report_content = resp.data.decode('utf-8-sig')
    if resp.status in [200, 206]:
        logging.debug(f"Got report from: {url}")
        return report_content
    elif resp.status in [204]:
        logging.debug(f"Got report without content from: {url}")
        return None
    else:
        logging.error(f"""
        Could not retrieve report:
        URL: {url}
        Headers: {resp.headers}
        Body: {report_content}
        """)
        raise ReportFetchException(resp)


def get_download_url(url, source_key, entity_key=None, paths_to_files=[]):
    if paths_to_files is None:
        paths_to_files = []
    entity_key = entity_key or source_key

    def get_file_url(dataset: dict) -> dict:
        entity_ids = ','.join(set(str(e[0]) for e in dataset['payload']))
        if not entity_ids:
            return dataset

        entities_url = API_URL + url + "&filters={key},in,({ids})".format(key="FileAttachments.EntityKeyID", ids=entity_ids)
        logging.debug(f"get_download_url - Fetching from {entities_url} ")

        content = rm_api_request(entities_url)
        if not content:
            return dataset
        for rm_entity in json.loads(content):
            entity = None

            for entity_key_id, in dataset['payload']:
                if entity_key_id == rm_entity[entity_key]:
                    entity = rm_entity[entity_key]
                    break

            assert entity, f"Entity not found for rm_entity: {rm_entity}"

            files = []
            for path_to_files in paths_to_files:
                entities = get_path(rm_entity, path_to_files)
                if type(entities) == list:
                    files.extend(entities)
                elif entities:
                    files.append(entities)

            for f in files:
                metadata = {key: val for key, val in f.items() if key in FILE_METADATA_FIELDS}

                downloadURL = f.get('DownloadURL')

                if downloadURL:
                    logging.info(f"get_download_url - Requesting {metadata} from {downloadURL}")
                    # fetch_file(downloadURL, format_file_name(f))
        return dataset

    return get_file_url


def get_path(obj, path):
    elem, *rest = path
    new_obj = obj.get(elem)
    if rest:
        if type(new_obj) == list:
            return [get_path(no, rest) for no in new_obj]

        return get_path(new_obj, rest)
    return new_obj


def fetch_file(url, file_name):
    logging.debug(f"Fetching file {file_name} from {url}")
    sub_folder, actual_file_name = os.path.split(file_name)
    folder = os.path.join(FOLDER, sub_folder)
    try:
        os.makedirs(folder, exist_ok=True)
        source_file = urllib.request.urlopen(url)
        path = os.path.join(folder, actual_file_name)
        logging.info(path)
        with open(path, 'wb') as f:
            f.write(source_file.read())

    except HTTPError as e:
        logging.error(f"Download of {file_name} failed. {e}")
        if e.code != 404:
            raise e
    except Exception as ex:
        logging.error(f"Not sure what happened. {ex}")
        raise ex


def format_file_name(file_obj):
    return f"{file_obj['FileID']}/{file_obj['Name']}{file_obj['Extension']}"


def main():
    try:
        api_token = refresh_token()
        logging.info(f"api_token: {api_token}")

        url = "https://noho.api.rentmanager.com/Deposits?embeds=FileAttachments&filters=FileAttachments.EntityKeyID," \
              "in,(1471,3969,17930)"
        content = rm_api_request(url)

        dataset = {
             'payload': [(1343,), (1425,), (1426,), (1471,)]
        }

        get_file_url = get_download_url(url='/Deposits?embeds=FileAttachments',
                                        source_key='EntityKeyID',
                                        entity_key='DepositID',
                                        paths_to_files=[['FileAttachments', 'File']])

        logging.info(get_file_url(dataset))

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()


def deposit_files():
    dataset = {
        "payload": query_database_for_ids("Deposit")  # Fetch these from your database
    }

    get_file_url = get_download_url(url='/Deposits?embeds=FileAttachments',
                                    source_key='EntityKeyID',
                                    entity_key='DepositID',
                                    paths_to_files=[['FileAttachments', 'File']])

    try:
        get_file_url(dataset)
        print("Download completed.")
    except Exception as e:
        print(f"An error occurred: {e}")
