import os
import shutil
from google.cloud import storage
from tests import utils


def delete_table(table_name):
    table_id = utils.ids.build_table_id(table_name)
    utils.constants.bq_client.delete_table(table_id, not_found_ok=False)


def delete_blob(blob_name):
    storage.Blob(name=blob_name, bucket=utils.constants.bucket).delete()


def delete_local_file(local_file_path):
    os.remove(local_file_path)


def delete_local_dir():
    if os.path.isdir(utils.constants.local_dir_path):
        shutil.rmtree(utils.constants.local_dir_path)
