import os
from tests import utils


def build_table_id(table_name):
    return f'{utils.constants.dataset_id}.{table_name}'


def build_bucket_uri(blob_name):
    return f'gs://{utils.constants.bucket_name}/{blob_name}'


def build_blob_name(bucket_dir_path_, blob_basename):
    if bucket_dir_path_ is None:
        blob_name = blob_basename
    else:
        blob_name = f'{bucket_dir_path_}/{blob_basename}'
    return blob_name


def build_blob_name_0(blob_basename):
    return build_blob_name(None, blob_basename)


def build_blob_name_1(blob_basename):
    return build_blob_name(utils.constants.bucket_dir_path, blob_basename)


def build_blob_name_2(blob_basename):
    return build_blob_name(utils.constants.bucket_subdir_path, blob_basename)


def build_local_file_path(local_dir_path_, local_file_basename):
    return os.path.join(local_dir_path_, local_file_basename)


def build_local_file_path_0(local_file_basename):
    return build_local_file_path(
        utils.constants.local_dir_path, local_file_basename)


def build_local_file_path_1(local_file_basename):
    return build_local_file_path(
        utils.constants.local_subdir_path, local_file_basename)
