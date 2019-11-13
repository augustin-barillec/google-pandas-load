import os
import logging
import pandas
from argparse import Namespace
from datetime import datetime
from copy import deepcopy
from google.cloud import bigquery, storage
from google_pandas_load.load_config import LoadConfig
from google_pandas_load.utils import \
    wait_for_jobs, \
    table_exists, \
    timestamp_randint_string, \
    check_no_prefix, \
    union_keys
from google_pandas_load.constants import \
    ATOMIC_FUNCTION_NAMES, \
    MIDDLE_LOCATIONS

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
        generated_data_name_prefix (str, optional): The prefix added to any
            generated data name in case the user does not give a name to the
            loaded data. It is a useful feature  to quickly find loaded data
            when debugging the code.
        max_concurrent_google_jobs (int, optional): The maximum number of
            concurrent google jobs allowed to be launched by the BigQuery
            Client. Defaults to 10.
        use_wildcard (bool, optional): If set to True, data moving from
            BigQuery to Storage will be split in several
            files whose basenames match a wildcard pattern. Defaults to True.
        compress (bool, optional): If set to True, data is compressed when
            moved from BigQuery to Storage or from pandas to the local folder.
            Defaults to True.
        separator (str, optional): The character which separates the columns of
            the data. Defaults to '|'.
        chunk_size (int, optional): The chunk size of a Storage blob created
            when data comes from the local folder. See
            `here <https://googleapis.github.io/google-cloud-python/latest/storage/blobs.html>`_
            for more information. Defaults to 2**28.
        logger (logging.Logger, optional): The logger creating the log records
            of this class. Defaults to a logger called Loader.

    .. _named:

    Note:
        **What is the data named data_name?**

        - in BigQuery: the table in the dataset whose id is data_name.
        - in Storage: the blobs whose basename begins with data_name inside the
          bucket directory.
        - in local: the files whose basename begins with data_name inside the
          local folder.

        This definition is motivated by the fact that BigQuery splits a big
        table in several blobs when extracting it to Storage.
    """

    def __init__(
            self,
            bq_client=None,
            dataset_ref=None,
            bucket=None,
            gs_dir_path=None,
            local_dir_path=None,
            generated_data_name_prefix=None,
            max_concurrent_google_jobs=10,
            use_wildcard=True,
            compress=True,
            separator='|',
            chunk_size=2**28,
            logger=logger_):

        if (
            gs_dir_path is not None
            and gs_dir_path.endswith('/')
        ):
            raise ValueError('To ease Storage path concatenation, '
                             'gs_dir_path must not end with /')

        self._bq_client = bq_client
        self._dataset_ref = dataset_ref
        self._bucket = bucket
        self._gs_dir_path = gs_dir_path
        if self._bucket is not None:
            self._bucket_uri = 'gs://{}'.format(self._bucket.name)
            if self._gs_dir_path is None:
                self._gs_dir_uri = self._bucket_uri
            else:
                self._gs_dir_uri = (self._bucket_uri + '/'
                                    + self._gs_dir_path)
        self._local_dir_path = local_dir_path
        self._generated_data_name_prefix = generated_data_name_prefix
        self._max_concurrent_google_jobs = max_concurrent_google_jobs

        self._use_wildcard = use_wildcard
        if self._use_wildcard:
            self._bq_to_gs_ext = '-*.csv'
        else:
            self._bq_to_gs_ext = '.csv'
        self._dataframe_to_local_ext = '.csv'
        self._compress = compress
        if self._compress:
            self._bq_to_gs_ext += '.gz'
            self._bq_to_gs_compression = 'GZIP'
            self._dataframe_to_local_ext += '.gz'
            self._dataframe_to_local_compression = 'gzip'
        else:
            self._bq_to_gs_compression = None
            self._dataframe_to_local_compression = None

        self._separator = separator
        self._chunk_size = chunk_size
        self._logger = logger
        self._atomic_functions = [
            self._query_to_bq,
            self._bq_to_gs,
            self._gs_to_local,
            self._local_to_dataframe,
            self._dataframe_to_local,
            self._local_to_gs,
            self._gs_to_bq,
            self._bq_to_query
        ]

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
    def bucket(self):
        """google.cloud.storage.bucket.Bucket: The bucket given in the
        argument."""
        return self._bucket

    @property
    def gs_dir_path(self):
        """str: The gs_dir_path given in the argument."""
        return self._bucket

    @property
    def local_dir_path(self):
        """str: The local_dir_path given in the argument."""
        return self._local_dir_path

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
                for blob in self.list_blobs(data_name=data_name)]

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
        return table_exists(client=self._bq_client, table_reference=table_ref)

    def exist_in_gs(self, data_name):
        """Return True if data named_ data_name exist in Storage,"""
        return len(self.list_blobs(data_name=data_name)) > 0

    def exist_in_local(self, data_name):
        """Return True if data named_ data_name exist in local."""
        return len(self.list_local_file_paths(data_name=data_name)) > 0

    def delete_in_bq(self, data_name):
        """Delete the data named_ data_name in BigQuery."""
        if self.exist_in_bq(data_name=data_name):
            table_ref = self._dataset_ref.table(table_id=data_name)
            self._bq_client.delete_table(table=table_ref)

    def delete_in_gs(self, data_name):
        """Delete the data named_ data_name in Storage."""
        if self.exist_in_gs(data_name=data_name):
            self._bucket.delete_blobs(
                blobs=self.list_blobs(data_name=data_name))

    def delete_in_local(self, data_name):
        """Delete the data named_ data_name in local."""
        if self.exist_in_local(data_name=data_name):
            for local_file_path in self.list_local_file_paths(
                    data_name=data_name):
                os.remove(local_file_path)

    def _exist(self, location, data_name):
        return self.__dict__['exist_in_{}'.format(location)][data_name]

    def _delete(self, location, data_name):
        return self.__dict__['delete_in_{}'.format(location)][data_name]

    def _is_source_clear(self, config):
        return self._exist(config.source, config.source_data_name)

    def _is_destination_clear(self, config):
        return self._exist(config.destination, config.destination_data_name)

    def _clear_source(self, config):
        self._delete(config.source, config.source_data_name)

    def _clear_destination(self, config):
        self._delete(config.destination, config.destination_data_name)

    def _check_if_data_in_source(self, config):
        n, s = config.source_data_name, config.source
        if self._is_source_clear(config):
            raise ValueError('There is no data {} in {}'.format(n, s))

    def _check_if_destination_clear(self, config):
        n, d = config.destination_data_name, config.destination
        if not self._is_destination_clear(config):
            raise ValueError('There is already data {} in {}'.format(n, d))

    def _query_to_bq_job(self, query_to_bq_config):
        config = query_to_bq_config
        job_config = bigquery.job.QueryJobConfig()
        job_config.destination = self._dataset_ref.table(
            table_id=config.destination_data_name)
        job_config.write_disposition = config.write_disposition
        job = self._bq_client.query(
            query=config.query,
            job_config=job_config)
        return job

    def _bq_to_gs_job(self, bq_to_gs_config):
        config = bq_to_gs_config
        source = self._dataset_ref.table(table_id=config.source_data_name)
        job_config = bigquery.job.ExtractJobConfig()
        job_config.compression = self._bq_to_gs_compression
        destination_uri = (self._gs_dir_uri + '/'
                           + config.destination_data_name
                           + self._bq_to_gs_ext)
        job_config.field_delimiter = self._separator
        job = self._bq_client.extract_table(
            source=source,
            destination_uris=destination_uri,
            job_config=job_config)
        return job

    def _gs_to_bq_job(self, gs_to_bq_config):
        config = gs_to_bq_config
        job_config = bigquery.job.LoadJobConfig()
        job_config.field_delimiter = self._separator
        job_config.schema = config.schema
        job_config.skip_leading_rows = 1
        job_config.write_disposition = config.write_disposition
        source_uris = self.list_blob_uris(data_name=config.source_data_name)
        destination = self._dataset_ref.table(
            table_id=config.destination_data_name)
        job = self._bq_client.load_table_from_uri(
            source_uris=source_uris,
            destination=destination,
            job_config=job_config)
        return job

    def _blob_to_local_file(self, blob):
        basename = os.path.basename(blob.name)
        local_file_path = os.path.join(self._local_dir_path, basename)
        blob.download_to_filename(local_file_path)

    def _local_file_to_blob(self, local_file_path):
        basename = os.path.basename(local_file_path)
        if self._gs_dir_path is None:
            blob_name = basename
        else:
            blob_name = self._gs_dir_path + '/' + basename
        blob = storage.Blob(name=blob_name,
                            bucket=self._bucket,
                            chunk_size=self._chunk_size)
        blob.upload_from_filename(filename=local_file_path)

    def _local_file_to_dataframe(self, local_file_path, dtype, parse_dates,
                                 infer_datetime_format):
        return pandas.read_csv(
            filepath_or_buffer=local_file_path,
            sep=self._separator,
            dtype=dtype,
            parse_dates=parse_dates,
            infer_datetime_format=infer_datetime_format)

    def _dataframe_to_local_file(self, dataframe, local_file_path):
        dataframe.to_csv(
            path_or_buf=local_file_path,
            sep=self._separator,
            index=False,
            compression=self._dataframe_to_local_compression)

    def _table_id_to_query(self, table_id):
        return 'select * from `{}.{}.{}`'.format(
            self._dataset_ref.project,
            self._dataset_ref.dataset_id,
            table_id)

    def _launch_bq_client_job(self, config):
        s = config.source
        d = config.destination
        job = self.__dict__['_{}_to_{}_job'.format(s, d)](config)
        return job

    def _execute_bq_client_jobs(self, configs):
        batch_size = self._max_concurrent_google_jobs
        nb_of_batches = len(configs) // batch_size + 1
        jobs = []
        for i in range(nb_of_batches):
            configs_to_process = configs[i * batch_size:(i + 1) * batch_size]
            jobs_to_process = [self._launch_bq_client_job(c)
                               for c in configs_to_process]
            jobs += jobs_to_process
            wait_for_jobs(jobs=jobs)
        return jobs

    def _execute_single_transfer(self, config):
        s = config.source
        d = config.destination
        return self.__dict__['_single_{}_to_{}'.format(s, d)](config)

    def _execute_transfers_sequentially(self, configs):
        return map(self._execute_single_transfer, configs)

    def _transfer_helper(self, configs, pre_process, parallel, post_process):
        start_timestamp = datetime.now()
        if pre_process:
            for config in configs:
                if config.clear_destination:
                    self._clear_destination(config)
                else:
                    self._check_if_destination_clear(config)

        if parallel:
            results = self._execute_bq_client_jobs(configs)
        else:
            results = self._execute_transfers_sequentially(configs)

        if post_process:
            for config in configs:
                if config.clear_source:
                    self._check_if_destination_clear(config)

        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        return results, duration

    def _common_transfer(
            self,
            configs,
            pre_process,
            parallel,
            post_process,
            return_results):

        source = configs[0].source
        destination = configs[0].destination

        self._logger.debug('Starting {} to {}...'.format(source, destination))
        results, duration = self._transfer_helper(configs, pre_process,
                                                  parallel, post_process)
        self._logger.debug('Ended {} to {} [{}s]'.format(
            source, destination, duration))
        if return_results:
            return results, duration
        else:
            return duration

    def _query_to_bq(self, query_to_bq_configs):
        self._logger.debug('Starting query to bq...')
        jobs, duration = self._transfer_helper(
            configs=query_to_bq_configs,
            pre_process=False,
            parallel=True,
            post_process=False)
        total_bytes_billed_list = [j.total_bytes_billed for j in jobs]
        costs = [round(tbb / 10 ** 12 * 5, 5) for tbb in
                 total_bytes_billed_list]
        cost = sum(costs)
        self._logger.debug('Ended query to bq [{}s, {}$]'.format(
            duration, cost))
        return duration, cost, costs

    def _bq_to_gs(self, bq_to_gs_configs):
        return self._common_transfer(
            configs=bq_to_gs_configs,
            pre_process=True,
            parallel=True,
            post_process=True,
            return_results=False)

    def _gs_to_local(self, gs_to_local_configs):
        return self._common_transfer(
            configs=gs_to_local_configs,
            pre_process=True,
            parallel=True,
            post_process=True,
            return_results=False)

    def _local_to_dataframe(self, local_to_dataframe_configs):
        return self._common_transfer(
            configs=local_to_dataframe_configs,
            pre_process=False,
            parallel=False,
            post_process=True,
            return_results=True)

    def _dataframe_to_local(self, dataframe_to_local_configs):
        return self._common_transfer(
            configs=dataframe_to_local_configs,
            pre_process=True,
            parallel=False,
            post_process=False,
            return_results=False)

    def _local_to_gs(self, local_to_gs_configs):
        return self._common_transfer(
            configs=local_to_gs_configs,
            pre_process=True,
            parallel=False,
            post_process=True,
            return_results=False)

    def _gs_to_bq(self, gs_to_bq_configs):
        return self._common_transfer(
            configs=gs_to_bq_configs,
            pre_process=False,
            parallel=True,
            post_process=True,
            return_results=False)

    def _bq_to_query(self, bq_to_query_configs):
        return self._common_transfer(
            configs=bq_to_query_configs,
            pre_process=False,
            parallel=False,
            post_process=False,
            return_results=True)

    def _single_local_to_gs(self, local_to_gs_config):
        data_name = local_to_gs_config.data_name
        local_file_paths = self.list_local_file_paths(data_name=data_name)
        map(self._local_file_to_blob, local_file_paths)

    def _one_local_to_dataframe(self, local_to_dataframe_config):
        config = local_to_dataframe_config
        data_name = config.data_name
        local_file_paths = self.list_local_file_paths(data_name=data_name)
        dataframes = map(
            lambda local_file_path:
                self._local_file_to_dataframe(
                    local_file_path,
                    config.dtype,
                    config.parse_dates,
                    config.infer_datetime_format),
            local_file_paths)
        dataframe = pandas.concat(dataframes)
        return dataframe

    def _one_dataframe_to_local(self, dataframe_to_local_config):
        config = dataframe_to_local_config
        data_name = config.data_name
        ext = self._dataframe_to_local_ext
        dataframe = config.dataframe
        local_file_path = os.path.join(self._local_dir_path, data_name + ext)
        self._dataframe_to_local_file(dataframe, local_file_path)

    def _one_bq_to_query(self, bq_to_query_config):
        data_name = bq_to_query_config.data_name
        return self._table_id_to_query(table_id=data_name)











    def _fill_missing_data_names(self, configs):
        for config in configs:
            if config.data_name is None:
                config.data_name = timestamp_randint_string(
                    prefix=self._generated_data_name_prefix)

    def _check_if_bq_client_missing(self, names_of_atomic_functions_to_call):
        if (self._bq_client is None
                and any('bq' in n for n in names_of_atomic_functions_to_call)):
            raise ValueError('bq_client must be given if bq is used')

    def _check_if_dataset_ref_missing(self, names_of_atomic_functions_to_call):
        if (self._dataset_ref is None
                and any('bq' in n for n in names_of_atomic_functions_to_call)):
            raise ValueError('dataset_ref must be given if bq is used')

    def _check_if_bucket_missing(self, names_of_atomic_functions_to_call):
        if (self._bucket is None
                and any('gs' in n for n in names_of_atomic_functions_to_call)):
            raise ValueError('bucket must be given if gs is used')

    def _check_if_local_dir_missing(self, names_of_atomic_functions_to_call):
        if (
            self._local_dir_path is None
            and any('local' in n for n in names_of_atomic_functions_to_call)
        ):
            raise ValueError('local_dir_path must be given if local is used')

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

            - res.load_results (list of (str or NoneType or pandas.DataFrame)):
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
              costs in US dollars of the mload. The i-th element is the
              query cost of the load job configured by configs[i].
        """


        for config in configs:
            if config._source in MIDDLE_LOCATIONS:
                self._check_if_data_in_source(config)

            if not config._overwrite:
                self._check_if_destination_clear(config)



            self.check_if_destination_empty(config)

        configs = [deepcopy(config) for config in configs]
        nb_of_configs = len(configs)
        self._fill_missing_data_names(configs=configs)
        data_names = [config.data_name for config in configs]
        check_no_prefix(strings=data_names)
        atomic_configs = [config.atomic_configs for config in configs]

        names_of_atomic_functions_to_call = union_keys(dicts=atomic_configs)
        self._check_required_resources(names_of_atomic_functions_to_call)

        load_results = dict()
        duration = 0
        durations = dict()
        query_cost = None
        query_costs = dict()
        for n, f in zip(ATOMIC_FUNCTION_NAMES, self._atomic_functions):
            f_indices = []
            f_configs = []
            for i, s in enumerate(atomic_configs):
                if n in s:
                    f_indices.append(i)
                    f_configs.append(s[n])
            if not f_configs:
                durations[n] = None
                continue
            res = f(configs=f_configs)
            if n == 'query_to_bq':
                f_duration, f_cost, f_costs = res
                query_cost = f_cost
                for i in f_indices:
                    query_costs[i] = f_costs.pop(0)
            elif n in ('local_to_dataframe', 'bq_to_query'):
                f_duration, f_load_results = res
                for i in f_indices:
                    load_results[i] = f_load_results.pop(0)
            else:
                f_duration = res
            durations[n] = f_duration
            duration += f_duration

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
        (resp. the bq_to_gs and gs_to_bq parts) from the configurations by
        batch of size max_concurrent_google_jobs.

        Args:
            configs (list of google_pandas_load.LoadConfig):
                See :class:`google_pandas_load.load_config.LoadConfig` for the
                format of one configuration.

        Returns:
            list of (str or NoneType or pandas.DataFrame): A list of of load
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
            infer_datetime_format=True,
            date_cols=None,
            timestamp_cols=None,
            bq_schema=None):
        """It works like  :meth:`google_pandas_load.loader.Loader.load` but
        also returns extra informations about the data and the load job's
        execution. The prefix x is for extra.

        Returns:
            argparse.Namespace: A xload result res with the following
            attributes:

            - res.load_result (str or NoneType or pandas.DataFrame): The result
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

              * res.durations.bq_to_query (int or NoneType): the duration in
                seconds of the bq_to_query part if any.

            - res.query_cost (float or NoneType): The query cost in US dollars
              of the query_to_bq part if any.
         """

        config = LoadConfig(
            source,
            destination,

            data_name=data_name,
            query=query,
            dataframe=dataframe,

            write_disposition=write_disposition,
            dtype=dtype,
            parse_dates=parse_dates,
            infer_datetime_format=infer_datetime_format,
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
            infer_datetime_format=True,
            date_cols=None,
            timestamp_cols=None,
            bq_schema=None):
        """Execute a load job whose configuration is specified by the
        arguments.

        The data is transferred from source to destination. The valid values
        for the source and the destination are: 'query', 'bq', 'gs', 'local'
        and 'dataframe'.

        Downloading follows the path:
        'query' -> 'bq' -> 'gs' -> 'local' -> 'dataframe' while uploading goes
        in the opposite direction.

        .. _moved:

        Warning:
            **In general, data is moved, not copied!**

            Once the load job has been executed, the data usually does not
            exist anymore in the source and in any transitional locations.

            However two exceptions exist:

            - When source = 'dataframe', the dataframe is not deleted in RAM.
            - When destination = ‘query’, the data is not deleted in BigQuery,
              so that it still exists somewhere. Indeed, in this case, the
              load job returns a simple query which represents the data but
              does not contain it.

            Use the delete_in_bq, delete_in_gs and delete_in_local parameters
            to control the data deletion, during the execution of the load
            job.


        .. _pre-deletion:

        Warning:
            **In general, pre-existing data is deleted!**

            Before new data is moved to any location, the loader will usually
            delete any prior data bearing the same name to prevent any
            conflict.

            There is one exception:

            - When destination = ‘bq’ and the write_dispostion parameter is
              set to ‘WRITE_APPEND’, new data is appended to pre-existing one
              with the same name.

        Args:
            source (str): one of 'query', 'bq', 'gs', 'local', 'dataframe'.
            destination (str): one of 'query', 'bq', 'gs', 'local',
                'dataframe'.

            data_name (str, optional): The `name <named_>`_ of the data. If
                not passed, a name is generated by concatenating the
                generated_data_name_prefix of the loader, if any, the current
                timestamp and a random integer. This is useful when
                source = 'query' and destination = 'dataframe' because the user
                may not need to know the data_name.

            query (str, optional): A BigQuery Standard Sql query. Required if
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
            infer_datetime_format (bool, optional): When
                destination = 'dataframe', pandas.read_csv() is used and
                infer_datetime_format is one of its parameters. Defaults to
                True.
            date_cols (list of str, optional): If no bq_schema is passed,
                indicate which columns of a pandas dataframe should have the
                BigQuery type DATE.
            timestamp_cols (list of str, optional): If no bq_schema is passed,
                indicate which columns of a pandas dataframe should have the
                BigQuery type TIMESTAMP.
            bq_schema (list of google.cloud.bigquery.schema.SchemaField, optional):
                The table's schema in BigQuery. Used when
                destination = 'bq' and source != 'query'. When
                source = 'query', the bq_schema is inferred from the query.
                If not passed and source = 'dataframe', falls back to
                an inferred value from the dataframe with `this method <LoadConfig.html#google_pandas_load.load_config.LoadConfig.bq_schema_inferred_from_dataframe>`__.

        Returns:
            str or pandas.DataFrame or NoneType: The result of the load job:

            - When destination = 'query', it returns the BigQuery standard
              SQL query: "select * from \`project_id.dataset_id.data_name\`",
              where the project_id is the dataset's one.
            - When destination = 'dataframe', it returns a pandas dataframe
              populated with the data specified by the arguments.
            - In all other cases, it returns None.
        """

        return self.xload(
                    source,
                    destination,

                    data_name=data_name,
                    query=query,
                    dataframe=dataframe,

                    write_disposition=write_disposition,
                    dtype=dtype,
                    parse_dates=parse_dates,
                    infer_datetime_format=infer_datetime_format,
                    date_cols=date_cols,
                    timestamp_cols=timestamp_cols,
                    bq_schema=bq_schema).load_result
