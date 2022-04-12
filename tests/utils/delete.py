import os
import shutil
from google.cloud import storage
from tests.utils.constants import bq_client, bucket, local_dir_path
from tests.utils.ids import build_table_id


def delete_table(table_name):
    table_id = build_table_id(table_name)
    bq_client.delete_table(table_id, not_found_ok=False)


def delete_blob(blob_name):
    storage.Blob(name=blob_name, bucket=bucket).delete()


def delete_local_file(local_file_path):
    os.remove(local_file_path)


def delete_local_dir():
    if os.path.isdir(local_dir_path):
        shutil.rmtree(local_dir_path)
