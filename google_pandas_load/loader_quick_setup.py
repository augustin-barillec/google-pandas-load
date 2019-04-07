import logging
from google.cloud import bigquery, storage
from google_pandas_load.loader import Loader

not_propagating_logger = logging.getLogger(name='LoaderQuickSetup')
not_propagating_logger.setLevel(level=logging.DEBUG)
not_propagating_logger.propagate = False
ch = logging.StreamHandler()
formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(fmt=formatter)
not_propagating_logger.addHandler(hdlr=ch)


class LoaderQuickSetup(Loader):
    """
    The purpose of this class is to quickly set up a loader.

    An instance of LoaderQuickSetup is simply an instance of the base class built with the following arguments:

    ::

         bq_client=bq_client
         dataset_ref=dataset_ref
         bucket=bucket
         gs_dir_path_in_bucket=gs_dir_path_in_bucket
         local_dir_path=local_dir_path
         generated_data_name_prefix=generated_data_name_prefix
         max_concurrent_google_jobs=max_concurrent_google_jobs
         use_wildcard=use_wildcard
         compress=compress
         separator=separator
         chunk_size=chunk_size
         logger=logger

    where

    ::

        bq_client = google.cloud.bigquery.Client(
            project=project_id,
            credentials=credentials)
        dataset_ref = google.cloud.bigquery.dataset.DatasetReference(
            project=project_id,
            dataset_id=dataset_id)
        gs_client = google.cloud.storage.Client(
            project=project_id,
            credentials=credentials)
        bucket = google.cloud.storage.bucket.Bucket(
            client=gs_client,
            name=bucket_name)

    Args:
        project_id (str, optional): The project id.
        dataset_id (str, optional): The dataset id.
        bucket_name (str, optional): The bucket name.
        gs_dir_path_in_bucket (str, optional): see base class.
        credentials (google.auth.credentials.Credentials): The credentials used to build the bq_client and the
            bucket. If not passed, falls back to the default inferred from the environment.
        local_dir_path (str, optional): see base class.
        generated_data_name_prefix (str, optional): see base class.
        max_concurrent_google_jobs (int, optional): see base class. The default value is 40 whereas the default value
            for the base class is 10. This is because the first value is intended for a scripting environment and
            the second one for a production environment, where the amount of google computing resources is usually
            shared by several other programs running also in production.
        use_wildcard (bool, optional): see base class.
        compress (bool, optional): see base class.
        separator (str, optional): see base class.
        chunk_size (int, optional): see base class.
        logger (logging.Logger, optional): see base class. The default value is a logger named LoaderQuickSetup,
            which contrary to the default logger of the base class, is set to not propagate its log records to its
            logger ancestors and is equipped with an handler which displays the log records to the console. This is
            convenient when working in a notebook for instance.
    """

    def __init__(
            self,
            project_id=None,
            dataset_id=None,
            bucket_name=None,
            gs_dir_path_in_bucket=None,
            credentials=None,
            local_dir_path=None,
            generated_data_name_prefix=None,
            max_concurrent_google_jobs=40,
            use_wildcard=True,
            compress=True,
            separator='|',
            chunk_size=2 ** 28,
            logger=not_propagating_logger):
        self._bq_client = None
        self._dataset_ref = None
        self._gs_client = None
        self._bucket = None
        if project_id is not None:
            self._bq_client = bigquery.Client(project=project_id, credentials=credentials)
            if dataset_id is not None:
                self._dataset_ref = bigquery.dataset.DatasetReference(project=project_id, dataset_id=dataset_id)
            if bucket_name is not None:
                self._gs_client = storage.Client(project=project_id, credentials=credentials)
                self._bucket = storage.bucket.Bucket(client=self.gs_client, name=bucket_name)

        super().__init__(bq_client=self.bq_client,
                         dataset_ref=self.dataset_ref,
                         bucket=self.bucket,
                         gs_dir_path_in_bucket=gs_dir_path_in_bucket,
                         local_dir_path=local_dir_path,
                         generated_data_name_prefix=generated_data_name_prefix,
                         max_concurrent_google_jobs=max_concurrent_google_jobs,
                         use_wildcard=use_wildcard,
                         compress=compress,
                         separator=separator,
                         chunk_size=chunk_size,
                         logger=logger)

    @property
    def bq_client(self):
        """google.cloud.bigquery.client.Client: See the bq_client parameter of the base class."""
        return self._bq_client

    @property
    def dataset_ref(self):
        """google.cloud.bigquery.dataset.DatasetReference: See the dataset_ref parameter of the base class."""
        return self._dataset_ref

    @property
    def gs_client(self):
        """google.cloud.storage.client.Client: The Storage client used to create the bucket."""
        return self._gs_client

    @property
    def bucket(self):
        """google.cloud.storage.bucket.Bucket: See the bucket parameter of the base class."""
        return self._bucket
