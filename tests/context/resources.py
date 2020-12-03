from google.cloud import bigquery
from google.cloud import storage
from tests.context.resources_names import \
    PROJECT_ID, DATASET_NAME, BUCKET_NAME, LOCAL_DIR_PATH

project_id = PROJECT_ID
dataset_name = DATASET_NAME
bucket_name = BUCKET_NAME
local_dir_path = LOCAL_DIR_PATH

bq_client = bigquery.Client(project=project_id)
dataset_ref = bigquery.dataset.DatasetReference(project=project_id,
                                                dataset_id=dataset_name)
gs_client = storage.Client(project=project_id)
bucket = storage.bucket.Bucket(client=gs_client, name=bucket_name)
