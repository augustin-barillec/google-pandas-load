from google_pandas_load import Loader, LoaderQuickSetup
from tests.resources import project_id, bq_client, dataset_id, dataset_name, \
    bucket_name, bucket, gs_dir_path, gs_subdir_path, \
    local_dir_path, local_subdir_path

gpl1 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    bucket=bucket,
    gs_dir_path=None,
    local_dir_path=local_dir_path,
    separator='|',
    chunk_size=2**28)

gpl2 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    bucket=bucket,
    gs_dir_path='dir/subdir',
    local_dir_path=local_dir_path,
    separator='@',
    chunk_size=2**28)

gpl3 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    bucket=bucket,
    gs_dir_path='dir/subdir',
    local_dir_path=local_dir_path + '/',
    separator='|',
    chunk_size=2**28)

gpl4 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    bucket=bucket,
    gs_dir_path='dir',
    local_dir_path=local_dir_path,
    separator='@',
    chunk_size=2**28)

gpl5 = Loader(
    bq_client=bq_client,
    dataset_id=dataset_id,
    bucket=bucket,
    gs_dir_path=None,
    local_dir_path=local_dir_path,
    separator='|',
    chunk_size=2**29)

gpl6 = LoaderQuickSetup(
    project_id=project_id,
    dataset_name=dataset_name,
    bucket_name=bucket_name,
    local_dir_path=local_dir_path)


gpl_no_bq_client = Loader(
    bq_client=None,
    dataset_id=dataset_id)

gpl_no_dataset_id = Loader(
    bq_client=bq_client,
    dataset_id=None)

gpl_no_bucket = Loader(
    bucket=None,
    local_dir_path=local_dir_path)

gpl_no_local_dir_path = Loader(
    bucket=bucket,
    local_dir_path=None)
