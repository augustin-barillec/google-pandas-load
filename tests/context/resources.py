from google.cloud import bigquery
from google.cloud import storage
from tests.context.resource_names import PROJECT_ID, DATASET_ID, BUCKET_NAME, LOCAL_DIR_PATH

project_id = PROJECT_ID
bq_client = bigquery.Client(project=project_id)
dataset_id = DATASET_ID
dataset_ref = bigquery.dataset.DatasetReference(project=project_id, dataset_id=dataset_id)
gs_client = storage.Client(project=project_id)
bucket_name = BUCKET_NAME
bucket = storage.bucket.Bucket(client=gs_client, name=bucket_name)
local_dir_path = LOCAL_DIR_PATH
