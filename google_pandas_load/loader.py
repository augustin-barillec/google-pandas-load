import os
import logging
import pandas
from argparse import Namespace
from datetime import datetime
from copy import deepcopy
from google.cloud import bigquery, storage
from google_pandas_load.load_config import LoadConfig
from google_pandas_load.utils import \
    table_exists, \
    wait_for_jobs, \
    timestamp_randint_string, \
    check_no_prefix, \
    union_keys
from google_pandas_load.constants import \
    MIDDLE_LOCATIONS, \
    DESTINATIONS_TO_ALWAYS_CLEAR, \
    ATOMIC_FUNCTION_NAMES, \
    BQ_CLIENT_ATOMIC_FUNCTION_NAMES

logger_ = logging.getLogger(name='Loader')


class Loader:
    """Wrapper for transferring big data between A and B where A and B are
    distinct and chosen between a BigQuery dataset, a directory in a Storage
    bucket, a local folder and the RAM (with type pandas.DataFrame).

    The Loader bundles all the parameters that do not change often when
    executing load jobs during a workflow.

    Args:
        bq_client (google.cloud.bigquery.client.Client, optional): Client to
            execute google load jobs.
        dataset_ref (google.cloud.bigquery.dataset.DatasetReference, optional):
            The dataset reference.
        bucket (google.cloud.storage.bucket.Bucket, optional): The bucket.
        gs_dir_path (str, optional): The path of the directory in
            the bucket.
        local_dir_path (str, optional): The path of the local folder.
        separator (str, optional): The character which separates the columns of
            the data. Defaults to '|'.
        chunk_size (int, optional): The chunk size of a Storage blob created
            when data comes from the local folder. See
            `here <https://googleapis.dev/python/storage/latest/blobs.html>`_
            for more information. Defaults to 2**28.
        logger (logging.Logger, optional): The logger creating the log records
            of this class. Defaults to a logger called Loader.
    """

    def __init__(
            self,
            bq_client=None,
            dataset_ref=None,
            bucket=None,
            gs_dir_path=None,
            local_dir_path=None,
            separator='|',
            chunk_size=2**28,
            logger=logger_):

        self._bq_client = bq_client
        self._dataset_ref = dataset_ref
        if self._dataset_ref is not None:
            self._dataset_name = self._dataset_ref.dataset_id
            self._dataset_id = '{}.{}'.format(
                self._dataset_ref.project, self._dataset_name)
        self._bucket = bucket
        self._gs_dir_path = gs_dir_path
        self._check_gs_dir_path_format()
        if self._bucket is not None:
            self._bucket_name = self._bucket.name
            self._bucket_uri = 'gs://{}'.format(self.bucket.name)
            if self._gs_dir_path is None:
                self._gs_dir_uri = self._bucket_uri
            else:
                self._gs_dir_uri = self._bucket_uri + '/' + self._gs_dir_path
        self._local_dir_path = local_dir_path
        self._bq_to_gs_ext = '-*.csv.gz'
        self._dataframe_to_local_ext = '.csv.gz'
        self._bq_to_gs_compression = 'GZIP'
        self._dataframe_to_local_compression = 'gzip'
        self._separator = separator
        self._chunk_size = chunk_size
        self._logger = logger

    @property
    def bq_client(self):
        """google.cloud.bigquery.client.Client: The bq_client given in the
        argument.
        """
        return self._bq_client

    @property
    def dataset_ref(self):
        """google.cloud.bigquery.dataset.DatasetReference: The dataset_ref
        given in the argument."""
        return self._dataset_ref

    @property
    def dataset_id(self):
        """str: The id of the dataset_ref given in the argument."""
        return self._dataset_id

    @property
    def dataset_name(self):
        """str: The name of the dataset_ref given in the argument."""
        return self._dataset_name

    @property
    def bucket(self):
        """google.cloud.storage.bucket.Bucket: The bucket given in the
        argument."""
        return self._bucket

    @property
    def bucket_name(self):
        """str: The name of the bucket given in the argument."""
        return self._bucket_name

    @property
    def gs_dir_path(self):
        """str: The gs_dir_path given in the argument."""
        return self._gs_dir_path

    @property
    def local_dir_path(self):
        """str: The local_dir_path given in the argument."""
        return self._local_dir_path

    @staticmethod
    def _fill_missing_data_names(configs):
        for config in configs:
            if config.data_name is None:
                config.data_name = timestamp_randint_string()

    @staticmethod
    def _check_if_configs_is_a_list(configs):
        if type(configs) != list:
            raise ValueError('configs must be list')

    @staticmethod
    def _check_if_configs_empty(configs):
        if len(configs) == 0:
            raise ValueError('configs must be non-empty')

    def _check_gs_dir_path_format(self):
        if self._gs_dir_path is not None and self._gs_dir_path.endswith('/'):
            msg = ("To ease Storage path concatenation, gs_dir_path must "
                   "not end with /")
            raise ValueError(msg)

    def _check_if_bq_client_missing(self, atomic_function_names):
        names = atomic_function_names
        if self._bq_client is None and any('bq' in n for n in names):
            raise ValueError('bq_client must be given if bq is used')

    def _check_if_dataset_ref_missing(self, atomic_function_names):
        names = atomic_function_names
        if self._dataset_ref is None and any('bq' in n for n in names):
            raise ValueError('dataset_ref must be given if bq is used')

    def _check_if_bucket_missing(self, atomic_function_names):
        names = atomic_function_names
        if self._bucket is None and any('gs' in n for n in names):
            raise ValueError('bucket must be given if gs is used')

    def _check_if_local_dir_missing(self, atomic_function_names):
        names = atomic_function_names
        if self._local_dir_path is None and any('local' in n for n in names):
            raise ValueError('local_dir_path must be given if local is used')

    def _check_if_data_in_source(self, atomic_config):
        n, s = atomic_config.data_name, atomic_config.source
        if self._is_source_clear(atomic_config):
            raise ValueError('There is no data named {} in {}'.format(n, s))

    def list_blobs(self, data_name):
        """Return the data named_ data_name in Storage as a list of
        Storage blobs.
        """
        if self._gs_dir_path is None:
            prefix = data_name
        else:
            prefix = self._gs_dir_path + '/' + data_name
        return list(self._bucket.list_blobs(prefix=prefix))

    def list_blob_uris(self, data_name):
        """Return the list of the uris of Storage blobs forming the data
        named_ data_name in Storage.
        """
        return [self._bucket_uri + '/' + blob.name
                for blob in self.list_blobs(data_name)]

    def list_local_file_paths(self, data_name):
        """Return the list of the paths of the files forming the data named_
        data_name in local.
        """
        return [os.path.join(self._local_dir_path, basename)
                for basename in os.listdir(self._local_dir_path)
                if basename.startswith(data_name)]

    def exist_in_bq(self, data_name):
        """Return True if data named_ data_name exist in BigQuery."""
        table_ref = self._dataset_ref.table(table_id=data_name)
        return table_exists(self._bq_client, table_ref)

    def exist_in_gs(self, data_name):
        """Return True if data named_ data_name exist in Storage."""
        return len(self.list_blobs(data_name)) > 0

    def exist_in_local(self, data_name):
        """Return True if data named_ data_name exist in local."""
        return len(self.list_local_file_paths(data_name)) > 0

    def delete_in_bq(self, data_name):
        """Delete the data named_ data_name in BigQuery."""
        if self.exist_in_bq(data_name):
            table_ref = self._dataset_ref.table(table_id=data_name)
            self._bq_client.delete_table(table_ref)

    def delete_in_gs(self, data_name):
        """Delete the data named_ data_name in Storage."""
        if self.exist_in_gs(data_name):
            self._bucket.delete_blobs(blobs=self.list_blobs(data_name))

    def delete_in_local(self, data_name):
        """Delete the data named_ data_name in local."""
        if self.exist_in_local(data_name):
            for local_file_path in self.list_local_file_paths(data_name):
                os.remove(local_file_path)

    def _exist(self, location, data_name):
        return getattr(self, 'exist_in_{}'.format(location))(data_name)

    def _delete(self, location, data_name):
        return getattr(self, 'delete_in_{}'.format(location))(data_name)

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
        if self._gs_dir_path is None:
            blob_name = local_file_basename
        else:
            blob_name = self._gs_dir_path + '/' + local_file_basename
        blob = storage.Blob(name=blob_name,
                            bucket=self._bucket,
                            chunk_size=self._chunk_size)
        blob.upload_from_filename(filename=local_file_path)

    def _local_file_to_dataframe(
            self, local_file_path, dtype, parse_dates):
        return pandas.read_csv(
            filepath_or_buffer=local_file_path,
            sep=self._separator,
            dtype=dtype,
            parse_dates=parse_dates,
            infer_datetime_format=True)

    def _dataframe_to_local_file(self, dataframe, local_file_path):
        dataframe.to_csv(
            path_or_buf=local_file_path,
            sep=self._separator,
            index=False,
            compression=self._dataframe_to_local_compression)

    def _query_to_bq_job(self, query_to_bq_config):
        config = query_to_bq_config
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self._dataset_ref.table(
            table_id=config.data_name)
        job_config.write_disposition = config.write_disposition
        job = self._bq_client.query(
            query=config.query,
            job_config=job_config)
        return job

    def _bq_to_gs_job(self, bq_to_gs_config):
        config = bq_to_gs_config
        source = self._dataset_ref.table(table_id=config.data_name)
        job_config = bigquery.ExtractJobConfig()
        job_config.compression = self._bq_to_gs_compression
        destination_uri = (self._gs_dir_uri + '/'
                           + config.data_name
                           + self._bq_to_gs_ext)
        job_config.field_delimiter = self._separator
        job = self._bq_client.extract_table(
            source=source,
            destination_uris=destination_uri,
            job_config=job_config)
        return job

    def _gs_to_bq_job(self, gs_to_bq_config):
        config = gs_to_bq_config
        job_config = bigquery.LoadJobConfig()
        job_config.field_delimiter = self._separator
        if config.schema is None:
            job_config.autodetect = True
        else:
            job_config.schema = config.schema
            job_config.skip_leading_rows = 1
        job_config.write_disposition = config.write_disposition
        source_uris = self.list_blob_uris(config.data_name)
        destination = self._dataset_ref.table(
            table_id=config.data_name)
        job = self._bq_client.load_table_from_uri(
            source_uris=source_uris,
            destination=destination,
            job_config=job_config)
        return job

    def _gs_to_local(self, gs_to_local_config):
        data_name = gs_to_local_config.data_name
        blobs = self.list_blobs(data_name)
        for b in blobs:
            self._blob_to_local_file(b)

    def _local_to_gs(self, local_to_gs_config):
        data_name = local_to_gs_config.data_name
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
        ext = self._dataframe_to_local_ext
        dataframe = config.dataframe
        local_file_path = os.path.join(self._local_dir_path, data_name + ext)
        self._dataframe_to_local_file(dataframe, local_file_path)

    def _launch_bq_client_job(self, atomic_config):
        s = atomic_config.source
        d = atomic_config.destination
        assert s == 'bq' or d == 'bq'
        job = getattr(self, '_{}_to_{}_job'.format(s, d))(atomic_config)
        return job

    def _execute_bq_client_jobs(self, atomic_configs):
        configs = atomic_configs
        jobs = [self._launch_bq_client_job(c) for c in configs]
        wait_for_jobs(jobs)
        return jobs

    def _execute_local_load(self, atomic_config):
        s = atomic_config.source
        d = atomic_config.destination
        assert s == 'local' or d == 'local'
        return getattr(self, '_{}_to_{}'.format(s, d))(atomic_config)

    def _execute_local_loads(self, atomic_configs):
        return list(map(self._execute_local_load, atomic_configs))

    def _atomic_load(self, atomic_configs):
        assert len(atomic_configs) > 0

        configs = atomic_configs

        source = configs[0].source
        destination = configs[0].destination

        assert all([c.source == source and c.destination == destination
                    for c in configs])

        atomic_function_name = '{}_to_{}'.format(source, destination)

        self._logger.debug('Starting {} to {}...'.format(source, destination))

        start_timestamp = datetime.now()

        if source in MIDDLE_LOCATIONS:
            for c in configs:
                self._check_if_data_in_source(c)

        if destination in DESTINATIONS_TO_ALWAYS_CLEAR:
            for c in configs:
                self._clear_destination(c)

        try:
            if atomic_function_name in BQ_CLIENT_ATOMIC_FUNCTION_NAMES:
                load_results = self._execute_bq_client_jobs(configs)
            else:
                load_results = self._execute_local_loads(configs)
        finally:
            if source in MIDDLE_LOCATIONS:
                for c in configs:
                    if c.clear_source:
                        self._clear_source(c)

        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds

        res = {'load_results': load_results, 'duration': duration}

        if atomic_function_name != 'query_to_bq':
            msg = 'Ended {} to {} [{}s]'.format(source, destination, duration)
            self._logger.debug(msg)
        else:
            jobs = load_results
            total_bytes_billed_list = [j.total_bytes_billed for j in jobs]
            costs = [round(tbb / 10 ** 12 * 5, 5)
                     for tbb in total_bytes_billed_list]
            cost = sum(costs)
            msg = 'Ended query to bq [{}s, {}$]'.format(duration, cost)
            self._logger.debug(msg)
            res['cost'] = cost
            res['costs'] = costs

        return res

    def xmload(self, configs):
        """It works like :meth:`google_pandas_load.loader.Loader.mload` but
        also returns extra informations about the data and the mload
        job's execution.

        Args:
            configs (list of google_pandas_load.LoadConfig):
                See :class:`google_pandas_load.load_config.LoadConfig` for
                the format of one configuration.

        Returns:
            args.Namespace: The xmload result res with the following
            attributes:

            - res.load_results (list of (pandas.DataFrame or NoneType)):
              A list of load results.

            - res.data_names (list of str): The names of the data.
              The i-th element is the data_name attached to
              configs[i], either given as an argument or generated by the
              loader.

            - res.duration (int): The mload job's duration.

            - res.durations (args.Namespace): A report res.durations
              providing the duration of each step of the mload job.

            - res.query_cost (float or NoneType): The query cost in
              US dollars of the query_to_bq part if any.

            - res.query_costs (list of (float or NoneType)): The query
              costs in US dollars of the mload job. The i-th element is the
              query cost of the load job configured by configs[i].
        """
        self._check_if_configs_is_a_list(configs)
        self._check_if_configs_empty(configs)
        configs = [deepcopy(config) for config in configs]
        nb_of_configs = len(configs)
        self._fill_missing_data_names(configs)
        data_names = [config.data_name for config in configs]
        check_no_prefix(data_names)
        sliced_configs = [config.sliced_config for config in configs]
        names_of_atomic_functions_to_call = union_keys(sliced_configs)

        self._check_if_bq_client_missing(names_of_atomic_functions_to_call)
        self._check_if_dataset_ref_missing(names_of_atomic_functions_to_call)
        self._check_if_bucket_missing(names_of_atomic_functions_to_call)
        self._check_if_local_dir_missing(names_of_atomic_functions_to_call)

        load_results = dict()
        duration = 0
        durations = dict()
        query_cost = None
        query_costs = dict()
        for n in ATOMIC_FUNCTION_NAMES:
            n_indices = []
            n_configs = []
            for i, s in enumerate(sliced_configs):
                if n in s:
                    n_indices.append(i)
                    n_configs.append(s[n])
            if not n_configs:
                durations[n] = None
                continue
            n_res = self._atomic_load(n_configs)
            n_load_results = n_res['load_results']
            n_duration = n_res['duration']
            if n == 'query_to_bq':
                n_cost = n_res['cost']
                n_costs = n_res['costs']
                query_cost = n_cost
                for i in n_indices:
                    query_costs[i] = n_costs.pop(0)
            elif n == 'local_to_dataframe':
                for i in n_indices:
                    load_results[i] = n_load_results.pop(0)
            durations[n] = n_duration
            duration += n_duration

        load_results = [load_results.get(i) for i in range(nb_of_configs)]
        durations = Namespace(**durations)
        query_costs = [query_costs.get(i) for i in range(nb_of_configs)]

        res = Namespace()
        res.load_results = load_results
        res.data_names = data_names
        res.duration = duration
        res.durations = durations
        res.query_cost = query_cost
        res.query_costs = query_costs
        return res

    def mload(self, configs):
        """Execute several load jobs specified by the configurations.
        The prefix m means multi.

        The BigQuery Client executes simultaneously the query_to_bq parts
        (resp. the bq_to_gs and gs_to_bq parts) from the configurations.

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
        return self.xmload(configs=configs).load_results

    def xload(
            self,
            source,
            destination,

            data_name=None,
            query=None,
            dataframe=None,

            write_disposition='WRITE_TRUNCATE',
            dtype=None,
            parse_dates=None,
            date_cols=None,
            timestamp_cols=None,
            bq_schema=None):
        """It works like  :meth:`google_pandas_load.loader.Loader.load` but
        also returns extra informations about the data and the load job's
        execution. The prefix x is for extra.

        Returns:
            argparse.Namespace: A xload result res with the following
            attributes:

            - res.load_result (pandas.DataFrame or NoneType): The result
              of the load job.

            - res.data_name (str): The `name <named_>`_ of the loaded data.

            - res.duration (int): The load job's duration in seconds.

            - res.durations (argparse.Namespace): A report providing the
              durations of each step of the load job. It has the following
              attributes:

              * res.durations.query_to_bq (int or NoneType): the duration in
                seconds of the query_to_bq part if any.

              * res.durations.bq_to_gs (int or NoneType): the duration in
                seconds of the bq_to_gs part if any.

              * res.durations.gs_to_local (int or NoneType): the duration in
                seconds of the gs_to_local part if any.

              * res.durations.local_to_dataframe (int or NoneType): the
                duration in seconds of the local_to_dataframe part if any.

              * res.durations.dataframe_to_local (int or NoneType): the
                duration in seconds of the dataframe_to_local part if any.

              * res.durations.local_to_gs (int or NoneType): the duration in
                seconds of the local_to_gs part if any.

              * res.durations.gs_to_bq (int or NoneType): the duration in
                seconds of the gs_to_bq part if any.

            - res.query_cost (float or NoneType): The query cost in US dollars
              of the query_to_bq part if any.
         """

        config = LoadConfig(
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

        xmload_res = self.xmload(configs=[config])

        res = Namespace()
        res.load_result = xmload_res.load_results[0]
        res.data_name = xmload_res.data_names[0]
        res.duration = xmload_res.duration
        res.durations = xmload_res.durations
        res.query_cost = xmload_res.query_cost
        return res

    def load(
            self,
            source,
            destination,

            data_name=None,
            query=None,
            dataframe=None,

            write_disposition='WRITE_TRUNCATE',
            dtype=None,
            parse_dates=None,
            date_cols=None,
            timestamp_cols=None,
            bq_schema=None):
        """Execute a load job whose configuration is specified by the
        arguments.

        The data is loaded from source to destination.

        The valid values for source are 'query', 'bq', 'gs', 'local' and
        'dataframe'.

        The valid values for the destination are 'bq', 'gs', 'local' and
        'dataframe'.

        Downloading follows the path:
        'query' -> 'bq' -> 'gs' -> 'local' -> 'dataframe'.

        Uploading follows the path: 'dataframe' -> 'local' -> 'gs' -> 'bq'.

        .. _named:

        Note:
            **What is the data named data_name?**

            - in BigQuery: the table in the dataset whose name is data_name.
            - in Storage: the blobs whose basename begins with data_name
              inside the bucket directory.
            - in local: the files whose basename begins with data_name inside
              the local folder.

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
            any prior data having the same name before loading the new data.
            This is done in order to prevent any conflict.

            To illustrate this process, consider the following load:

            .. code-block:: python

                loader.load(
                    source='dataframe',
                    destination='bq',
                    data_name='a0',
                    dataframe=df)

            Before populating a BigQuery table, data goes through a local
            folder and Storage. If some existing data was named ‘a0’ prior the
            load job in any of these three locations, it is going to be erased
            first.

            Default behaviour can only be modified in the BigQuery location.
            To do this, the default value of the write_disposition parameter
            has to be changed.

        Args:
            source (str): one of 'query', 'bq', 'gs', 'local', 'dataframe'.
            destination (str): one of 'bq', 'gs', 'local', 'dataframe'.

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
                'WRITE_TRUNCATE'.
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
                The table's schema in BigQuery. Used when destination = 'bq'
                and source != 'query'. When source = 'query', the bq_schema is
                inferred from the query. If source is one of 'gs' or 'local'
                and the bq_schema is not passed, it falls back to an inferred
                value from the CSV with `google.cloud.bigquery.job.LoadJobConfig.autodetect <https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html#google.cloud.bigquery.job.LoadJobConfig>`__.
                If source = 'dataframe' and the
                bq_schema is not passed, it falls back to an inferred value
                from the dataframe with `this method <LoadConfig.html#google_pandas_load.load_config.LoadConfig.bq_schema_inferred_from_dataframe>`__.

        Returns:
            pandas.DataFrame or NoneType: The result of the load job:

            - When destination = 'dataframe', it returns a pandas dataframe
              populated with the data specified by the arguments.
            - In all other cases, it returns None.
        """

        return self.xload(
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
                    bq_schema=bq_schema).load_result
