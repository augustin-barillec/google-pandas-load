from google_pandas_load import Loader, LoaderQuickSetup
from tests.utils import constants


def create_loader(
        bq_client=constants.bq_client,
        dataset_id=constants.dataset_id,
        gs_client=constants.gs_client,
        bucket_name=constants.bucket_name,
        bucket_dir_path=None,
        local_dir_path=constants.local_dir_path,
        separator=constants.separator,
        chunk_size=constants.chunk_size,
        timeout=constants.timeout):
    return Loader(
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
        project_id=constants.project_id,
        dataset_name=constants.dataset_name,
        bucket_name=constants.bucket_name,
        bucket_dir_path=None,
        credentials=constants.credentials,
        local_dir_path=constants.local_dir_path,
        separator=constants.separator,
        chunk_size=constants.chunk_size,
        timeout=constants.timeout):
    return LoaderQuickSetup(
        project_id=project_id,
        dataset_name=dataset_name,
        bucket_name=bucket_name,
        bucket_dir_path=bucket_dir_path,
        credentials=credentials,
        local_dir_path=local_dir_path,
        separator=separator,
        chunk_size=chunk_size,
        timeout=timeout)
