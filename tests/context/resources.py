from google.cloud import bigquery
from google.cloud import storage
from tests.context.resource_names import \
    PROJECT_ID, DATASET_NAME, BUCKET_NAME, LOCAL_DIR_PATH

project_id = PROJECT_ID
dataset_name = DATASET_NAME
dataset_id = '{}.{}'.format(project_id, dataset_name)
bucket_name = BUCKET_NAME
local_dir_path = LOCAL_DIR_PATH

bq_client = bigquery.Client(project=project_id)
dataset_ref = bigquery.DatasetReference(project=project_id,
                                        dataset_id=dataset_name)
gs_client = storage.Client(project=project_id)
bucket = storage.Bucket(client=gs_client, name=bucket_name)
