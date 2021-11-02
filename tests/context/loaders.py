from google_pandas_load import Loader, LoaderQuickSetup
from tests.context.resources import project_id, bq_client, dataset_name, \
    dataset_ref, bucket_name, bucket, local_dir_path

gpl1 = Loader(
    bq_client=bq_client,
    dataset_ref=dataset_ref,
    bucket=bucket,
    gs_dir_path=None,
    local_dir_path=local_dir_path,
    separator='|',
    chunk_size=2**28)

gpl2 = Loader(
    bq_client=bq_client,
    dataset_ref=dataset_ref,
    bucket=bucket,
    gs_dir_path='dir/subdir',
    local_dir_path=local_dir_path,
    separator='@',
    chunk_size=2**28)

gpl3 = Loader(
    bq_client=bq_client,
    dataset_ref=dataset_ref,
    bucket=bucket,
    gs_dir_path='dir/subdir',
    local_dir_path=local_dir_path + '/',
    separator='|',
    chunk_size=2**28)

gpl4 = Loader(
    bq_client=bq_client,
    dataset_ref=dataset_ref,
    bucket=bucket,
    gs_dir_path=None,
    local_dir_path=local_dir_path,
    separator='|',
    chunk_size=2**29)

gpl5 = LoaderQuickSetup(
    project_id=project_id,
    dataset_name=dataset_name,
    bucket_name=bucket_name,
    local_dir_path=local_dir_path)

gpl_no_bq_client = Loader(
    bq_client=None,
    dataset_ref=dataset_ref)

gpl_no_dataset_ref = Loader(
    bq_client=bq_client,
    dataset_ref=None)

gpl_no_bucket = Loader(
    bucket=None,
    local_dir_path=local_dir_path)

gpl_no_local_dir_path = Loader(
    bucket=bucket,
    local_dir_path=None)
