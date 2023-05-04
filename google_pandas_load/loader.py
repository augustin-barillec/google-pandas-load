import os
import logging
import pandas
from typing import Literal, List, Dict, Any, Optional
from datetime import datetime
from copy import deepcopy
from google.cloud import bigquery, storage
from google_pandas_load import constants, load_config, utils
logger = logging.getLogger(name=__name__)


class Loader:
    """Wrapper for transferring data between A and B where A and B are
    distinct and chosen between a BigQuery dataset, a Storage bucket directory,
    a local directory and the RAM (with type pandas.DataFrame).

    The Loader bundles all the parameters that do not change often when
    executing load jobs during a workflow.

    Note:
        If the optional argument bucket_dir_path is not given, data will be
        stored at the root of the bucket. It is a good practice to specify this
        argument so that data is stored in a defined bucket directory.

    Args:
        bq_client (google.cloud.bigquery.client.Client, optional): Client to
            manage connections to the BigQuery API.
        dataset_id (str, optional): The dataset id.
        gs_client (google.cloud.storage.client.Client, optional): Client for
            interacting with the Storage API.
        bucket_name (str, optional): The bucket name.
        bucket_dir_path (str, optional): The bucket directory path.
        local_dir_path (str, optional): The local directory path.
        separator (str, optional): The character which separates the columns of
            the data. Defaults to '|'.
        chunk_size (int, optional): The chunk size of a Storage blob created
            when data is uploaded. See
            `here <https://googleapis.dev/python/storage/latest/blobs.html>`_
            for more information. Defaults to 2**28.
        timeout (int, optional): The amount of time, in seconds, to wait
            for the server response when uploading a Storage blob.
            Defaults to 60.
    """
    def __init__(
            self,
            bq_client: Optional[bigquery.Client] = None,
            dataset_id: Optional[str] = None,
            gs_client: Optional[storage.Client] = None,
            bucket_name: Optional[str] = None,
            bucket_dir_path: Optional[str] = None,
            local_dir_path: Optional[str] = None,
            separator: Optional[str] = '|',
            chunk_size: Optional[int] = 2**28,
            timeout: Optional[int] = 60):
        self._bq_client = bq_client
        self._dataset_id = dataset_id
        self._gs_client = gs_client
        self._bucket_name = bucket_name
        self._bucket_dir_path = bucket_dir_path
        self._local_dir_path = local_dir_path
        self._separator = separator
        self._chunk_size = chunk_size
        self._timeout = timeout

        self._check_bq_client_dataset_id_consistency()
        self._check_gs_client_bucket_name_consistency()

        if self._dataset_id is not None:
            self._check_dataset_id_format()
            self._dataset_name = self._dataset_id.split('.')[-1]
        if self._gs_client is not None:
            self._bucket = self._gs_client.bucket(self._bucket_name)
            self._bucket_uri = f'gs://{self._bucket_name}'
            if self._bucket_dir_path is None:
                self._blob_name_prefix = ''
            else:
                self._check_bucket_dir_path_format()
                self._blob_name_prefix = self._bucket_dir_path + '/'
            self._blob_uri_prefix = (
                    self._bucket_uri + '/' + self._blob_name_prefix)

    @property
    def bq_client(self) -> bigquery.Client:
        """google.cloud.bigquery.client.Client: The BigQuery client."""
        return self._bq_client

    @property
    def dataset_id(self) -> str:
        """str: The dataset id."""
        return self._dataset_id

    @property
    def dataset_name(self) -> str:
        """str: The dataset name."""
        return self._dataset_name

    @property
    def gs_client(self) -> storage.Client:
        """google.cloud.storage.client.Client: The Storage client."""
        return self._gs_client

    @property
    def bucket_name(self) -> str:
        """str: The bucket name."""
        return self._bucket_name

    @property
    def bucket(self) -> storage.Bucket:
        """google.cloud.storage.bucket.Bucket: The bucket."""
        return self._bucket

    @property
    def bucket_dir_path(self) -> str:
        """str: The bucket directory path."""
        return self._bucket_dir_path

    @property
    def local_dir_path(self) -> str:
        """str: The local directory path."""
        return self._local_dir_path

    def _check_bq_client_dataset_id_consistency(self):
        c1 = self._bq_client is None
        c2 = self._dataset_id is None
        if not c1 and c2:
            msg = 'dataset_id must be provided if bq_client is provided'
            raise ValueError(msg)
        if not c2 and c1:
            msg = 'bq_client must be provided if dataset_id is provided'
            raise ValueError(msg)

    def _check_gs_client_bucket_name_consistency(self):
        c1 = self._gs_client is None
        c2 = self._bucket_name is None
        if not c1 and c2:
            msg = 'bucket_name must be provided if gs_client is provided'
            raise ValueError(msg)
        if not c2 and c1:
            msg = 'gs_client must be provided if bucket_name is provided'
            raise ValueError(msg)

    def _check_dataset_id_format(self):
        assert self._dataset_id is not None
        if self._dataset_id.count('.') != 1:
            msg = 'dataset_id must contain exactly one dot'
            raise ValueError(msg)

    def _check_bucket_dir_path_format(self):
        assert self._bucket_dir_path is not None
        if self._bucket_dir_path == '':
            msg = 'bucket_dir_path must not be the empty string'
            raise ValueError(msg)
        if self._bucket_dir_path.startswith('/'):
            msg = 'bucket_dir_path must not start with /'
            raise ValueError(msg)
        if self._bucket_dir_path.endswith('/'):
            msg = 'bucket_dir_path must not end with /'
            raise ValueError(msg)

    @staticmethod
    def _check_data_name_not_contain_slash(data_name):
        utils.check_data_name_not_contain_slash(data_name)

    @staticmethod
    def _check_if_configs_is_a_list(configs):
        if type(configs) != list:
            raise ValueError('configs must be a list')

    @staticmethod
    def _check_if_configs_empty(configs):
        if len(configs) == 0:
            raise ValueError('configs must be non-empty')

    def _check_if_bq_client_missing(self, atomic_function_names):
        names = atomic_function_names
        if self._bq_client is None and any('dataset' in n for n in names):
            raise ValueError('bq_client must be provided if dataset is used')

    def _check_if_gs_client_missing(self, atomic_function_names):
        names = atomic_function_names
        if self._gs_client is None and any('bucket' in n for n in names):
            raise ValueError('gs_client must be provided if bucket is used')

    def _check_if_local_dir_path_missing(self, atomic_function_names):
        names = atomic_function_names
        if self._local_dir_path is None and any('local' in n for n in names):
            raise ValueError(
                'local_dir_path must be provided if local is used')

    def _check_if_data_in_source(self, atomic_config):
        n, s = atomic_config.data_name, atomic_config.source
        if self._is_source_clear(atomic_config):
            raise ValueError(f'There is no data named {n} in {s}')

    @staticmethod
    def _log(msg):
        logger.debug(msg)

    @staticmethod
    def _fill_missing_data_names(configs):
        for config in configs:
            if config.data_name is None:
                config.data_name = utils.timestamp_randint_string()

    def _build_table_id(self, table_name):
        return f'{self._dataset_id}.{table_name}'

    def _blob_is_considered(self, blob):
        c1 = blob.name.startswith(self._blob_name_prefix)
        c2 = '/' not in blob.name[len(self._blob_name_prefix):]
        return c1 and c2

    def _local_file_is_considered(self, local_file_path):
        c1 = local_file_path.startswith(self._local_dir_path)
        c2 = os.path.isfile(local_file_path)
        return c1 and c2

    def list_blobs(self, data_name: str) -> List[storage.Blob]:
        """Return the data named_ data_name in Storage as a list of
        Storage blobs."""
        self._check_data_name_not_contain_slash(data_name)
        data_name_prefix = self._blob_name_prefix + data_name
        candidates = list(self._gs_client.list_blobs(
            bucket_or_name=self._bucket_name, prefix=data_name_prefix))
        res = [b for b in candidates if self._blob_is_considered(b)]
        res = sorted(res, key=lambda b: b.name)
        return res

    def list_blob_uris(self, data_name: str) -> List[str]:
        """Return the list of the uris of Storage blobs forming the data
        named_ data_name in Storage."""
        return [self._bucket_uri + '/' + blob.name
                for blob in self.list_blobs(data_name)]

    def list_local_file_paths(self, data_name: str) -> List[str]:
        """Return the list of the paths of the files forming the data named_
        data_name in local."""
        self._check_data_name_not_contain_slash(data_name)
        res = []
        for basename in os.listdir(self._local_dir_path):
            path = os.path.join(self._local_dir_path, basename)
            c1 = self._local_file_is_considered(path)
            c2 = basename.startswith(data_name)
            if c1 and c2:
                res.append(path)
        return sorted(res)

    def exist_in_dataset(self, data_name: str) -> bool:
        """Return True if data named_ data_name exist in BigQuery."""
        table_id = self._build_table_id(data_name)
        return utils.table_exists(self._bq_client, table_id)

    def exist_in_bucket(self, data_name: str) -> bool:
        """Return True if data named_ data_name exist in Storage."""
        return len(self.list_blobs(data_name)) > 0

    def exist_in_local(self, data_name: str) -> bool:
        """Return True if data named_ data_name exist in local."""
        return len(self.list_local_file_paths(data_name)) > 0

    def delete_in_dataset(self, data_name: str) -> None:
        """Delete the data named_ data_name in BigQuery."""
        table_id = self._build_table_id(data_name)
        self._bq_client.delete_table(table_id, not_found_ok=True)

    def delete_in_bucket(self, data_name: str) -> None:
        """Delete the data named_ data_name in Storage."""
        self._bucket.delete_blobs(blobs=self.list_blobs(data_name))

    def delete_in_local(self, data_name: str) -> None:
        """Delete the data named_ data_name in local."""
        for local_file_path in self.list_local_file_paths(data_name):
            os.remove(local_file_path)

    def _exist(self, location, data_name):
        return getattr(self, f'exist_in_{location}')(data_name)

    def _delete(self, location, data_name):
        return getattr(self, f'delete_in_{location}')(data_name)

    def _is_source_clear(self, atomic_config):
        return not self._exist(atomic_config.source, atomic_config.data_name)

    def _clear_source(self, atomic_config):
        self._delete(atomic_config.source, atomic_config.data_name)

    def _clear_destination(self, atomic_config):
        self._delete(atomic_config.destination, atomic_config.data_name)

    def _blob_to_local_file(self, blob):
        blob_basename = blob.name.split('/')[-1]
        local_file_path = os.path.join(self._local_dir_path, blob_basename)
        blob.download_to_filename(filename=local_file_path)

    def _local_file_to_blob(self, local_file_path):
        local_file_basename = os.path.basename(local_file_path)
        blob_name = self._blob_name_prefix + local_file_basename
        blob = storage.Blob(
            name=blob_name,
            bucket=self._bucket,
            chunk_size=self._chunk_size)
        blob.upload_from_filename(
            filename=local_file_path,
            timeout=self._timeout)

    def _local_file_to_dataframe(
            self, local_file_path, dtype, parse_dates):
        return pandas.read_csv(
            filepath_or_buffer=local_file_path,
            sep=self._separator,
            dtype=dtype,
            parse_dates=parse_dates,
            skip_blank_lines=False)

    def _dataframe_to_local_file(self, dataframe, local_file_path):
        dataframe.to_csv(
            path_or_buf=local_file_path,
            sep=self._separator,
            index=False,
            compression='gzip')

    def _query_to_dataset_job(self, query_to_dataset_config):
        config = query_to_dataset_config
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self._build_table_id(
            query_to_dataset_config.data_name)
        job_config.write_disposition = config.write_disposition
        job = self._bq_client.query(
            query=config.query,
            job_config=job_config)
        return job

    def _dataset_to_bucket_job(self, dataset_to_bucket_config):
        config = dataset_to_bucket_config
        source = self._build_table_id(config.data_name)
        job_config = bigquery.ExtractJobConfig()
        job_config.compression = 'GZIP'
        destination_uri = (
                self._blob_uri_prefix + config.data_name + '-*.csv.gz')
        job_config.field_delimiter = self._separator
        job = self._bq_client.extract_table(
            source=source,
            destination_uris=destination_uri,
            job_config=job_config)
        return job

    def _bucket_to_dataset_job(self, bucket_to_dataset_config):
        config = bucket_to_dataset_config
        job_config = bigquery.LoadJobConfig()
        job_config.field_delimiter = self._separator
        if config.schema is None:
            job_config.autodetect = True
        else:
            job_config.schema = config.schema
            job_config.skip_leading_rows = 1
        job_config.write_disposition = config.write_disposition
        source_uris = self.list_blob_uris(config.data_name)
        destination = self._build_table_id(config.data_name)
        job = self._bq_client.load_table_from_uri(
            source_uris=source_uris,
            destination=destination,
            job_config=job_config)
        return job

    def _bucket_to_local(self, bucket_to_local_config):
        data_name = bucket_to_local_config.data_name
        blobs = self.list_blobs(data_name)
        for b in blobs:
            self._blob_to_local_file(b)

    def _local_to_bucket(self, local_to_bucket_config):
        data_name = local_to_bucket_config.data_name
        local_file_paths = self.list_local_file_paths(data_name)
        for p in local_file_paths:
            self._local_file_to_blob(p)

    def _local_to_dataframe(self, local_to_dataframe_config):
        config = local_to_dataframe_config
        data_name = config.data_name
        local_file_paths = self.list_local_file_paths(data_name)
        dataframes = map(
            lambda local_file_path:
            self._local_file_to_dataframe(
                local_file_path,
                config.dtype,
                config.parse_dates),
            local_file_paths)
        dataframe = pandas.concat(dataframes)
        return dataframe

    def _dataframe_to_local(self, dataframe_to_local_config):
        config = dataframe_to_local_config
        data_name = config.data_name
        dataframe = config.dataframe
        local_file_path = os.path.join(
            self._local_dir_path, data_name + '.csv.gz')
        self._dataframe_to_local_file(dataframe, local_file_path)

    def _launch_bq_client_job(self, atomic_config):
        s = atomic_config.source
        d = atomic_config.destination
        assert s == 'dataset' or d == 'dataset'
        job = getattr(self, f'_{s}_to_{d}_job')(atomic_config)
        return job

    def _execute_bq_client_loads(self, atomic_configs):
        configs = atomic_configs
        jobs = [self._launch_bq_client_job(c) for c in configs]
        utils.wait_for_jobs(jobs)
        return jobs

    def _execute_local_load(self, atomic_config):
        s = atomic_config.source
        d = atomic_config.destination
        assert s == 'local' or d == 'local'
        return getattr(self, f'_{s}_to_{d}')(atomic_config)

    def _execute_local_loads(self, atomic_configs):
        return list(map(self._execute_local_load, atomic_configs))

    def _execute_same_type_loads(self, atomic_configs):
        assert len(atomic_configs) > 0
        configs = atomic_configs
        source = configs[0].source
        destination = configs[0].destination
        assert all([c.source == source and c.destination == destination
                    for c in configs])
        atomic_function_name = f'{source}_to_{destination}'
        self._log(f'Starting {source} to {destination}...')
        start_timestamp = datetime.now()
        if source in constants.MIDDLE_LOCATIONS:
            for c in configs:
                self._check_if_data_in_source(c)
        if destination in constants.DESTINATIONS_TO_ALWAYS_CLEAR:
            for c in configs:
                self._clear_destination(c)
        try:
            if atomic_function_name in \
                    constants.BQ_CLIENT_ATOMIC_FUNCTION_NAMES:
                res = self._execute_bq_client_loads(configs)
            else:
                res = self._execute_local_loads(configs)
        finally:
            if source in constants.MIDDLE_LOCATIONS:
                for c in configs:
                    if c.clear_source:
                        self._clear_source(c)
        end_timestamp = datetime.now()
        duration = round((end_timestamp - start_timestamp).total_seconds())
        if atomic_function_name != 'query_to_dataset':
            msg = f'Ended {source} to {destination} [{duration}s]'
            self._log(msg)
        else:
            jobs = res
            total_bytes_processed_list = [
                j.total_bytes_processed for j in jobs]
            gb_processed_list = [
                round(tbb / 10 ** 9, 2) for tbb in total_bytes_processed_list]
            gb_processed = sum(gb_processed_list)
            msg = f'Ended query to dataset [{duration}s, {gb_processed}GB]'
            self._log(msg)
        return res

    def multi_load(self, configs: List[load_config.LoadConfig]):
        """Execute several load jobs specified by the configurations.

        The BigQuery Client executes simultaneously the query_to_dataset parts
        (resp. the dataset_to_bucket and bucket_to_dataset parts) from the
        configurations.

        Args:
            configs (list of google_pandas_load.load_config.LoadConfig):
                See :class:`google_pandas_load.load_config.LoadConfig` for the
                format of one configuration.

        Returns:
            list of (pandas.DataFrame or NoneType): A list of load
            results. The i-th element is the result of the load job configured
            by configs[i]. See :meth:`google_pandas_load.loader.Loader.load`
            for the format of one load result.
        """
        self._check_if_configs_is_a_list(configs)
        self._check_if_configs_empty(configs)
        configs = [deepcopy(config) for config in configs]
        nb_configs = len(configs)
        self._fill_missing_data_names(configs)
        data_names = [config.data_name for config in configs]
        utils.check_no_prefix(data_names)
        sliced_configs = [config.sliced for config in configs]
        names_atomic_functions_to_call = utils.union_keys(sliced_configs)

        self._check_if_bq_client_missing(names_atomic_functions_to_call)
        self._check_if_gs_client_missing(names_atomic_functions_to_call)
        self._check_if_local_dir_path_missing(names_atomic_functions_to_call)

        res = dict()
        for n in constants.ATOMIC_FUNCTION_NAMES:
            indexed_atomic_configs = [
                (i, s[n]) for i, s in enumerate(sliced_configs) if n in s]
            if len(indexed_atomic_configs) == 0:
                continue
            atomic_configs = [iac[1] for iac in indexed_atomic_configs]
            n_res = self._execute_same_type_loads(atomic_configs)
            if n == 'local_to_dataframe':
                indexes = [iac[0] for iac in indexed_atomic_configs]
                for i in indexes:
                    res[i] = n_res.pop(0)
        res = [res.get(i) for i in range(nb_configs)]
        return res

    def load(
            self,
            source: Literal[
                'query', 'dataset', 'bucket', 'local', 'dataframe'],
            destination: Literal['dataset', 'bucket', 'local', 'dataframe'],

            data_name: Optional[str] = None,
            query: Optional[str] = None,
            dataframe: Optional[pandas.DataFrame] = None,

            write_disposition: Optional[str] =
            bigquery.WriteDisposition.WRITE_TRUNCATE,
            dtype: Optional[Dict[str, Any]] = None,
            parse_dates: Optional[List[str]] = None,
            date_cols: Optional[List[str]] = None,
            timestamp_cols: Optional[List[str]] = None,
            bq_schema: Optional[List[bigquery.SchemaField]] = None):
        """Execute a load job whose configuration is specified by the
        arguments. The data is loaded from source to destination.

        The valid values for source are 'query', 'dataset', 'bucket', 'local'
        and 'dataframe'. The valid values for the destination are 'dataset',
        'bucket', 'local' and 'dataframe'.

        Downloading follows the path:
        'query' -> 'dataset' -> 'bucket' -> 'local' -> 'dataframe'.
        Uploading follows the path:
        'dataframe' -> 'local' -> 'bucket' -> 'dataset'.

        .. _named:

        Note:
            **What is the data named data_name?**

            - in BigQuery: the table in the dataset whose name is data_name.
            - in Storage: the blobs at the root of the bucket directory and
              whose basename begins with data_name.
            - in local: the files at the root of the local directory and
              whose basename begins with data_name.

            This definition is motivated by the fact that BigQuery splits a big
            table in several blobs when extracting it to Storage.

        Note:
            **Data is not renamed**

            Since renaming the data identified by a prefix (see previous note)
            rises too much difficulties, choice has been made to keep its
            original name.

        .. _pre-deletion:

        Warning:
            **By default, pre-existing data is deleted !**

            Since data is not renamed  (see previous note), the loader deletes
            any prior data having the same `name <named_>`_ before loading
            the new data. This is done in order to prevent any conflict.

            To illustrate this process, consider the following load:

            .. code-block:: python

                loader.load(
                    source='dataframe',
                    destination='dataset',
                    data_name='a0',
                    dataframe=df)

            Before populating a BigQuery table, data goes through a local
            directory and a bucket. If some existing data was `named <named_>`_
            a0 prior to the load job in any of these three locations, it is
            going to be erased first.

            Default behaviour can only be modified in the BigQuery location.
            To do this, the default value of the write_disposition parameter
            has to be changed.

        Args:
            source (str): one of 'query', 'dataset', 'bucket', 'local',
                'dataframe'.
            destination (str): one of 'dataset', 'bucket', 'local',
                'dataframe'.

            data_name (str, optional): The `name <named_>`_ of the data. If
                not passed, a name is generated by concatenating the current
                timestamp and a random integer. This is useful when
                source = 'query' and destination = 'dataframe' because the user
                may not need to know the data_name.
            query (str, optional): A BigQuery Standard SQL query. Required if
                source = 'query'.
            dataframe (pandas.DataFrame, optional): A pandas dataframe.
                Required if source = 'dataframe'.

            write_disposition (google.cloud.bigquery.job.WriteDisposition, optional):
                Specifies the action that occurs if data named_
                data_name already exist in BigQuery. Defaults to
                bigquery.WriteDisposition.WRITE_TRUNCATE.
            dtype (dict, optional): When destination = 'dataframe',
                pandas.read_csv() is used and dtype is one of its parameters.
            parse_dates (list of str, optional): When
                destination = 'dataframe', pandas.read_csv() is used and
                parse_dates is one of its parameters.
            date_cols (list of str, optional): If no bq_schema is passed,
                indicate which columns of a pandas dataframe should have the
                BigQuery type DATE.
            timestamp_cols (list of str, optional): If no bq_schema is passed,
                indicate which columns of a pandas dataframe should have the
                BigQuery type TIMESTAMP.
            bq_schema (list of google.cloud.bigquery.schema.SchemaField, optional):
                The table schema in BigQuery. Used when
                destination = 'dataset' and source != 'query'. When
                source = 'query', the bq_schema is inferred from the query.
                If source is one of 'bucket' or 'local' and the bq_schema is
                not passed, it falls back to an inferred value from the CSV
                with `google.cloud.bigquery.job.LoadJobConfig.autodetect <https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html#google.cloud.bigquery.job.LoadJobConfig>`__.
                If source = 'dataframe' and the bq_schema is not passed, it
                falls back to an inferred value from the dataframe with
                `this method <LoadConfig.html#google_pandas_load.load_config.LoadConfig.bq_schema_inferred_from_dataframe>`__.

        Returns:
            pandas.DataFrame or NoneType: The result of the load job:

            - When destination = 'dataframe', it returns a pandas dataframe
              populated with the data specified by the arguments.
            - In all other cases, it returns None.
        """
        config = load_config.LoadConfig(
            source=source,
            destination=destination,

            data_name=data_name,
            query=query,
            dataframe=dataframe,

            write_disposition=write_disposition,
            dtype=dtype,
            parse_dates=parse_dates,
            date_cols=date_cols,
            timestamp_cols=timestamp_cols,
            bq_schema=bq_schema)

        return self.multi_load(configs=[config])[0]
