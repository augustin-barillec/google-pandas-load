from google.cloud import bigquery, storage
from google.auth.credentials import Credentials
from google_pandas_load.loader import Loader
from typing import Optional


class LoaderQuickSetup(Loader):
    """
    The purpose of this class is to quickly set up a loader.

    An instance of LoaderQuickSetup is simply an instance of the base class
    built with the following arguments:

    ::

         bq_client=bq_client
         dataset_id=dataset_id
         gs_client=gs_client
         bucket_name=bucket_name
         bucket_dir_path=bucket_dir_path
         local_dir_path=local_dir_path
         separator=separator
         chunk_size=chunk_size
         timeout=timeout

    where

    ::

        bq_client = google.cloud.bigquery.Client(
            project=project_id,
            credentials=credentials)
        dataset_id = project_id + '.' + dataset_name
        gs_client = google.cloud.storage.Client(
            project=project_id,
            credentials=credentials)

    Note:
        If the optional argument bucket_dir_path is not given, data will be
        stored at the root of the bucket. It is a good practice to specify this
        argument so that data is stored in a defined bucket directory.

    Args:
        project_id (str, optional): The project id.
        dataset_name (str, optional): The dataset name.
        bucket_name (str, optional): The bucket name.
        bucket_dir_path (str, optional): See base class.
        credentials (google.auth.credentials.Credentials): Credentials used to
            build the bq_client and the gs_client. If not passed, falls back to
            the default inferred from the environment.
        local_dir_path (str, optional): See base class.
        separator (str, optional): See base class.
        chunk_size (int, optional): See base class.
        timeout (int, optional): See base class.
    """

    def __init__(
            self,
            project_id: Optional[str] = None,
            dataset_name: Optional[str] = None,
            bucket_name: Optional[str] = None,
            bucket_dir_path: Optional[str] = None,
            credentials: Optional[Credentials] = None,
            local_dir_path: Optional[str] = None,
            separator: Optional[str] = '|',
            chunk_size: Optional[int] = 2**28,
            timeout: Optional[int] = 60):
        self._project_id = project_id
        self._check_project_id_dataset_name_bucket_name_consistency(
            dataset_name, bucket_name)
        bq_client = None
        dataset_id = None
        gs_client = None
        if dataset_name is not None:
            bq_client = bigquery.Client(
                project=self._project_id, credentials=credentials)
            dataset_id = f'{self._project_id}.{dataset_name}'
        if bucket_name is not None:
            gs_client = storage.Client(
                project=self._project_id, credentials=credentials)

        super().__init__(
            bq_client=bq_client,
            dataset_id=dataset_id,
            gs_client=gs_client,
            bucket_name=bucket_name,
            bucket_dir_path=bucket_dir_path,
            local_dir_path=local_dir_path,
            separator=separator,
            chunk_size=chunk_size,
            timeout=timeout)

    @property
    def project_id(self) -> str:
        """str: The project_id."""
        return self._project_id

    def _check_project_id_dataset_name_bucket_name_consistency(
            self, dataset_name, bucket_name):
        c1 = self._project_id is None
        c2 = dataset_name is None
        c3 = bucket_name is None
        if not c1 and c2 and c3:
            msg = ('At least one of dataset_name or bucket_name '
                   'must be provided if project_id is provided')
            raise ValueError(msg)
        if not c2 and c1:
            msg = 'project_id must be provided if dataset_name is provided'
            raise ValueError(msg)
        if not c3 and c1:
            msg = 'project_id must be provided if bucket_name is provided'
            raise ValueError(msg)
