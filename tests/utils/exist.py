import os
from google.cloud import exceptions, storage
from tests import utils


def table_exists(table_name):
    table_id = utils.ids.build_table_id(table_name)
    try:
        utils.constants.bq_client.get_table(table_id)
        return True
    except exceptions.NotFound:
        return False


def blob_exists(blob_name):
    return storage.Blob(name=blob_name, bucket=utils.constants.bucket).exists()


def local_file_exists(local_file_path):
    return os.path.isfile(local_file_path)
