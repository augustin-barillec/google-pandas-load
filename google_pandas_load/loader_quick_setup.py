import logging
from google.cloud import bigquery, storage
from google_pandas_load.loader import Loader

not_propagating_logger = logging.getLogger(name='LoaderQuickSetup')
not_propagating_logger.setLevel(level=logging.DEBUG)
not_propagating_logger.propagate = False
ch = logging.StreamHandler()
fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(fmt=fmt)
ch.setFormatter(fmt=formatter)
not_propagating_logger.addHandler(hdlr=ch)


class LoaderQuickSetup(Loader):
    """
    The purpose of this class is to quickly set up a loader.

    An instance of LoaderQuickSetup is simply an instance of the base class
    built with the following arguments:

    ::

         bq_client=bq_client
         dataset_ref=dataset_ref
         bucket=bucket
         gs_dir_path=gs_dir_path
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
        dataset_ref = google.cloud.bigquery.DatasetReference(
            project=project_id,
            dataset_id=dataset_id)
        gs_client = google.cloud.storage.Client(
            project=project_id,
            credentials=credentials)
        bucket = google.cloud.storage.Bucket(
            client=gs_client,
            name=bucket_name)

    Args:
        project_id (str, optional): The project id.
        dataset_id (str, optional): The dataset id.
        bucket_name (str, optional): The bucket name.
        gs_dir_path (str, optional): See base class.
        credentials (google.auth.credentials.Credentials): Credentials used to
            build the bq_client and the bucket. If not passed, falls back to
            the default inferred from the environment.
        local_dir_path (str, optional): See base class.
        generated_data_name_prefix (str, optional): See base class.
        max_concurrent_google_jobs (int, optional): See base class.
            Default value is 40 while the default value for the base class is
            10. The first value is intended for a notebook environment whereas
            the second one for a production environment. In the latter case
            the amount of google computing resources is usually shared by
            several programs running in production.
        use_wildcard (bool, optional): See base class.
        compress (bool, optional): See base class.
        separator (str, optional): See base class.
        chunk_size (int, optional): See base class.
        logger (logging.Logger, optional): See base class. Default value is a
            logger called LoaderQuickSetup. Contrary to the default base class
            logger, it is set to not propagate its log records to its logger
            ancestors and it is equipped with an handler displaying the log
            records to the console. This is, for instance, convenient when
            working with a notebook.
    """

    def __init__(
            self,
            project_id=None,
            dataset_id=None,
            bucket_name=None,
            gs_dir_path=None,
            credentials=None,
            local_dir_path=None,
            generated_data_name_prefix=None,
            max_concurrent_google_jobs=40,
            use_wildcard=True,
            compress=True,
            separator='|',
            chunk_size=2 ** 28,
            logger=not_propagating_logger):
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._bucket_name = bucket_name

        self._bq_client = None
        self._dataset_ref = None
        self._gs_client = None
        self._bucket = None
        if self.project_id is not None:
            self._bq_client = bigquery.Client(
                project=self.project_id, credentials=credentials)
            if self.dataset_id is not None:
                self._dataset_ref = bigquery.dataset.DatasetReference(
                    project=self.project_id, dataset_id=self.dataset_id)
            if self.bucket_name is not None:
                self._gs_client = storage.Client(
                    project=self.project_id, credentials=credentials)
                self._bucket = storage.bucket.Bucket(
                    client=self.gs_client, name=self.bucket_name)

        super().__init__(bq_client=self.bq_client,
                         dataset_ref=self.dataset_ref,
                         bucket=self.bucket,
                         gs_dir_path=gs_dir_path,
                         local_dir_path=local_dir_path,
                         generated_data_name_prefix=generated_data_name_prefix,
                         max_concurrent_google_jobs=max_concurrent_google_jobs,
                         use_wildcard=use_wildcard,
                         compress=compress,
                         separator=separator,
                         chunk_size=chunk_size,
                         logger=logger)

    @property
    def project_id(self):
        """str: The project_id given in the argument."""
        return self._project_id

    @property
    def dataset_id(self):
        """str: The dataset_id given in the argument."""
        return self._dataset_id

    @property
    def bucket_name(self):
        """str: The bucket_name givent in the argument."""
        return self._bucket_name

    @property
    def gs_client(self):
        """google.cloud.storage.client.Client: The Storage client used to
        create the bucket."""
        return self._gs_client
