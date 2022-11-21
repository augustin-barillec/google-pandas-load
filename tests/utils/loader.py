import google_pandas_load
from tests import utils


def create_loader(
        bq_client=utils.constants.bq_client,
        dataset_id=utils.constants.dataset_id,
        gs_client=utils.constants.gs_client,
        bucket_name=utils.constants.bucket_name,
        bucket_dir_path=None,
        local_dir_path=utils.constants.local_dir_path,
        separator=utils.constants.separator,
        chunk_size=utils.constants.chunk_size,
        timeout=utils.constants.timeout):
    return google_pandas_load.Loader(
        bq_client=bq_client,
        dataset_id=dataset_id,
        gs_client=gs_client,
        bucket_name=bucket_name,
        bucket_dir_path=bucket_dir_path,
        local_dir_path=local_dir_path,
        separator=separator,
        chunk_size=chunk_size,
        timeout=timeout)


def create_loader_quick_setup(
        project_id=utils.constants.project_id,
        dataset_name=utils.constants.dataset_name,
        bucket_name=utils.constants.bucket_name,
        bucket_dir_path=None,
        credentials=utils.constants.credentials,
        local_dir_path=utils.constants.local_dir_path,
        separator=utils.constants.separator,
        chunk_size=utils.constants.chunk_size,
        timeout=utils.constants.timeout):
    return google_pandas_load.LoaderQuickSetup(
        project_id=project_id,
        dataset_name=dataset_name,
        bucket_name=bucket_name,
        bucket_dir_path=bucket_dir_path,
        credentials=credentials,
        local_dir_path=local_dir_path,
        separator=separator,
        chunk_size=chunk_size,
        timeout=timeout)
