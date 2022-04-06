from google_pandas_load import Loader, LoaderQuickSetup
from tests.utils.resources import project_id, bq_client, dataset_id, \
    dataset_name, gs_client, bucket_name, bucket_dir_path, gs_subdir_path, \
    local_dir_path, local_subdir_path

gpl00 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    gs_client=gs_client,
    bucket_name=bucket_name,
    bucket_dir_path=None,
    local_dir_path=local_dir_path,
    separator='|',
    chunk_size=2**28,
    timeout=60)

gpl01 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    gs_client=gs_client,
    bucket_name=bucket_name,
    bucket_dir_path=None,
    local_dir_path=local_subdir_path,
    separator='|',
    chunk_size=2**28,
    timeout=30)

gpl10 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    gs_client=gs_client,
    bucket_name=bucket_name,
    bucket_dir_path=bucket_dir_path,
    local_dir_path=local_dir_path,
    separator='@',
    chunk_size=2**28,
    timeout=60)

gpl11 = LoaderQuickSetup(
    project_id=project_id,
    dataset_name=dataset_name,
    bucket_name=bucket_name,
    bucket_dir_path=bucket_dir_path,
    local_dir_path=local_subdir_path,
    separator='|',
    chunk_size=2**28,
    timeout=60)

gpl20 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    gs_client=gs_client,
    bucket_name=bucket_name,
    bucket_dir_path=gs_subdir_path,
    local_dir_path=local_dir_path,
    separator='|',
    chunk_size=2**20,
    timeout=60)

gpl21 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    gs_client=gs_client,
    bucket_name=bucket_name,
    bucket_dir_path=gs_subdir_path,
    local_dir_path=local_subdir_path,
    chunk_size=2**28,
    timeout=60)
