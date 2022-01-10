import os
from google.cloud import storage
from google.cloud.exceptions import NotFound
from tests.utils.resources import bq_client, bucket
from tests.utils.ids import build_table_id


def table_exists(table_name):
    table_id = build_table_id(table_name)
    try:
        bq_client.get_table(table_id)
        return True
    except NotFound:
        return False


def blob_exists(blob_name):
    storage.Blob(name=blob_name, bucket=bucket).exists()


def local_file_exists(local_file_path):
    return os.path.isfile(local_file_path)
