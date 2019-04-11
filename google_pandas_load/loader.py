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
from google_pandas_load.constants import ATOMIC_FUNCTION_NAMES

logger_ = logging.getLogger(name='Loader')


class Loader:
    """Wrapper for transferring big data between A and B where A and B are distinct and chosen between
    a BigQuery dataset, a directory in a Storage bucket, a local folder and the RAM
    (with type pandas.DataFrame).

    The Loader bundles all the parameters that do not change often when executing load jobs during a
    workflow.

    Args:
        bq_client (google.cloud.bigquery.client.Client, optional): Client to execute google load jobs.
        dataset_ref (google.cloud.bigquery.dataset.DatasetReference, optional): The dataset reference.
        bucket (google.cloud.storage.bucket.Bucket, optional): The bucket.
        gs_dir_path_in_bucket (str, optional): The path of the directory in the bucket.
        local_dir_path (str, optional): The path of the local folder.
        generated_data_name_prefix (str, optional): The prefix added to any generated data name in case the user does
            not give a name to the loaded data. It is a useful feature  to quickly find loaded data when debugging the
            code.
        max_concurrent_google_jobs (int, optional): The maximum number of concurrent google jobs allowed to be
            launched by the BigQuery Client. Defaults to 10.
        use_wildcard (bool, optional): If set to True, data moving from BigQuery to Storage will be split in several
            files whose basenames match a wildcard pattern. Defaults to True.
        compress (bool, optional): If set to True, data is compressed when moved from BigQuery to Storage or from
            pandas to the local folder. Defaults to True.
        separator (str, optional): The character which separates the columns of the data. Defaults to '|'.
        chunk_size (int, optional): The chunk size of a Storage blob created when data comes from the local folder.
            See `here <https://googleapis.github.io/google-cloud-python/latest/storage/blobs.html>`__ for more
            information. Defaults to 2**28.
        logger (logging.Logger, optional): The logger creating the log records of this class.
            Defaults to a logger called Loader.

    .. _named:

    Note:
        **What is the data named data_name?**

        - in BigQuery: the table in the dataset whose id is data_name.
        - in Storage: the blobs whose basename begins with data_name inside the bucket directory.
        - in local: the files whose basename begins with data_name inside the local folder.

        This definition is motivated by the fact that BigQuery splits a big table in several blobs when extracting it to
        Storage.
    """

    def __init__(
            self,
            bq_client=None,
            dataset_ref=None,
            bucket=None,
            gs_dir_path_in_bucket=None,
            local_dir_path=None,
            generated_data_name_prefix=None,
            max_concurrent_google_jobs=10,
            use_wildcard=True,
            compress=True,
            separator='|',
            chunk_size=2**28,
            logger=logger_):

        if gs_dir_path_in_bucket is not None and gs_dir_path_in_bucket.endswith('/'):
            raise ValueError('To ease Storage path concatenation, gs_dir_path_in_bucket must not end with /')

        self._bq_client = bq_client
        self._dataset_ref = dataset_ref
        self._bucket = bucket
        self._gs_dir_path_in_bucket = gs_dir_path_in_bucket
        if self._bucket is not None:
            self._bucket_uri = 'gs://{}'.format(self._bucket.name)
            if self._gs_dir_path_in_bucket is None:
                self._gs_dir_uri = self._bucket_uri
            else:
                self._gs_dir_uri = self._bucket_uri + '/' + self._gs_dir_path_in_bucket
        self._local_dir_path = local_dir_path
        self._generated_data_name_prefix = generated_data_name_prefix
        self._mcgj = max_concurrent_google_jobs

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
        self._atomic_functions = [self._query_to_bq, self._bq_to_gs, self._gs_to_local, self._local_to_dataframe,
                                  self._dataframe_to_local, self._local_to_gs, self._gs_to_bq, self._bq_to_query]

    def list_blobs(self, data_name):
        """Return the data named_ data_name in Storage as a list of Storage blobs.
        """
        if self._gs_dir_path_in_bucket is None:
            prefix = data_name
        else:
            prefix = self._gs_dir_path_in_bucket + '/' + data_name
        return list(self._bucket.list_blobs(prefix=prefix))

    def list_blob_uris(self, data_name):
        """Return the list of the uris of Storage blobs forming the data named_ data_name in Storage.
        """
        return [self._bucket_uri + '/' + blob.name for blob in self.list_blobs(data_name=data_name)]

    def list_local_file_paths(self, data_name):
        """Return the list of the paths of the files forming the data named_ data_name in local.
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
            self._bucket.delete_blobs(blobs=self.list_blobs(data_name=data_name))

    def delete_in_local(self, data_name):
        """Delete the data named_ data_name in local."""
        if self.exist_in_local(data_name=data_name):
            for local_file_path in self.list_local_file_paths(data_name=data_name):
                os.remove(local_file_path)

    def _query_to_bq(self, configs):
        self._logger.debug('Starting query to bq...')
        start_timestamp = datetime.now()
        total_bytes_billed_list = []
        nb_of_batches = len(configs)//self._mcgj + 1
        for i in range(nb_of_batches):
            jobs = []
            for config in configs[i*self._mcgj:(i+1)*self._mcgj]:
                job_config = bigquery.job.QueryJobConfig()
                job_config.destination = self._dataset_ref.table(table_id=config.data_name)
                job_config.write_disposition = config.write_disposition
                job = self._bq_client.query(query=config.query,
                                            job_config=job_config)
                jobs.append(job)
            wait_for_jobs(jobs=jobs)
            total_bytes_billed_list += [j.total_bytes_billed for j in jobs]
        costs = [round(tbb/10**12*5, 5) for tbb in total_bytes_billed_list]
        cost = sum(costs)
        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        self._logger.debug('Ended query to bq [{}s, {}$]'.format(duration, cost))
        return duration, cost, costs

    def _bq_to_gs(self, configs):
        self._logger.debug('Starting bq to gs...')
        start_timestamp = datetime.now()
        for config in configs:
            self.delete_in_gs(data_name=config.data_name)
        nb_of_batches = len(configs)//self._mcgj + 1
        for i in range(nb_of_batches):
            jobs = []
            for config in configs[i*self._mcgj:(i+1)*self._mcgj]:
                if not self.exist_in_bq(data_name=config.data_name):
                    raise ValueError('There is no data named {} in bq'.format(config.data_name))
                source = self._dataset_ref.table(table_id=config.data_name)
                job_config = bigquery.job.ExtractJobConfig()
                job_config.compression = self._bq_to_gs_compression
                destination_uri = self._gs_dir_uri + '/' + config.data_name + self._bq_to_gs_ext
                job_config.field_delimiter = self._separator
                job = self._bq_client.extract_table(source=source,
                                                    destination_uris=destination_uri,
                                                    job_config=job_config)
                jobs.append(job)
            wait_for_jobs(jobs=jobs)
        for config in configs:
            if config.delete_in_source:
                self.delete_in_bq(data_name=config.data_name)
        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        self._logger.debug('Ended bq to gs [{}s]'.format(duration))
        return duration

    def _gs_to_local(self, configs):
        self._logger.debug('Starting gs to local...')
        start_timestamp = datetime.now()
        for config in configs:
            self.delete_in_local(data_name=config.data_name)
        for config in configs:
            if not self.exist_in_gs(data_name=config.data_name):
                raise ValueError('There is no data named {} in gs'.format(config.data_name))
            for blob in self.list_blobs(data_name=config.data_name):
                blob.download_to_filename(filename=os.path.join(self._local_dir_path, os.path.basename(blob.name)))
        for config in configs:
            if config.delete_in_source:
                self.delete_in_gs(data_name=config.data_name)
        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        self._logger.debug('Ended gs to local [{}s]'.format(duration))
        return duration

    def _local_to_dataframe(self, configs):
        self._logger.debug('Starting local to dataframe...')
        start_timestamp = datetime.now()
        dataframes = []
        for config in configs:
            if not self.exist_in_local(data_name=config.data_name):
                raise ValueError('There is no data named {} in local'.format(config.data_name))
            dataframe_bits = []
            for local_file_path in self.list_local_file_paths(data_name=config.data_name):
                dataframe_bit = pandas.read_csv(filepath_or_buffer=local_file_path,
                                                sep=self._separator,
                                                dtype=config.dtype,
                                                parse_dates=config.parse_dates,
                                                infer_datetime_format=config.infer_datetime_format)
                dataframe_bits.append(dataframe_bit)
            dataframe = pandas.concat(dataframe_bits)
            dataframes.append(dataframe)
        for config in configs:
            if config.delete_in_source:
                self.delete_in_local(data_name=config.data_name)
        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        self._logger.debug('Ended local to dataframe [{}s]'.format(duration))
        return duration, dataframes

    def _dataframe_to_local(self, configs):
        self._logger.debug('Starting dataframe to local...')
        start_timestamp = datetime.now()
        for config in configs:
            self.delete_in_local(data_name=config.data_name)
        for config in configs:
            config.dataframe.to_csv(path_or_buf=os.path.join(self._local_dir_path,
                                                             config.data_name + self._dataframe_to_local_ext),
                                    sep=self._separator,
                                    index=False,
                                    compression=self._dataframe_to_local_compression)
        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        self._logger.debug('Ended dataframe to local [{}s]'.format(duration))
        return duration

    def _local_to_gs(self, configs):
        self._logger.debug('Starting local to gs...')
        start_timestamp = datetime.now()
        for config in configs:
            self.delete_in_gs(data_name=config.data_name)
        for config in configs:
            if not self.exist_in_local(data_name=config.data_name):
                raise ValueError('There is no data named {} in local'.format(config.data_name))
            for local_file_path in self.list_local_file_paths(data_name=config.data_name):
                basename = os.path.basename(local_file_path)
                if self._gs_dir_path_in_bucket is None:
                    name = basename
                else:
                    name = self._gs_dir_path_in_bucket + '/' + basename
                blob = storage.Blob(name=name,
                                    bucket=self._bucket,
                                    chunk_size=self._chunk_size)
                blob.upload_from_filename(filename=local_file_path)
        for config in configs:
            if config.delete_in_source:
                self.delete_in_local(data_name=config.data_name)
        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        self._logger.debug('Ended local to gs [{}s]'.format(duration))
        return duration

    def _gs_to_bq(self, configs):
        self._logger.debug('Starting gs to bq...')
        start_timestamp = datetime.now()
        nb_of_batches = len(configs)//self._mcgj + 1
        for i in range(nb_of_batches):
            jobs = []
            for config in configs[i*self._mcgj:(i+1)*self._mcgj]:
                if not self.exist_in_gs(data_name=config.data_name):
                    raise ValueError('There is no data named {} in gs'.format(config.data_name))
                job_config = bigquery.job.LoadJobConfig()
                job_config.field_delimiter = self._separator
                job_config.schema = config.schema
                job_config.skip_leading_rows = 1
                job_config.write_disposition = config.write_disposition
                job = self._bq_client.load_table_from_uri(
                    source_uris=self.list_blob_uris(data_name=config.data_name),
                    destination=self._dataset_ref.table(table_id=config.data_name),
                    job_config=job_config)
                jobs.append(job)
            wait_for_jobs(jobs=jobs)
        for config in configs:
            if config.delete_in_source:
                self.delete_in_gs(data_name=config.data_name)
        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        self._logger.debug('Ended gs to bq [{}s]'.format(duration))
        return duration

    def _bq_to_query(self, configs):
        self._logger.debug('Starting bq to query...')
        start_timestamp = datetime.now()
        queries = []
        for config in configs:
            if not self.exist_in_bq(data_name=config.data_name):
                raise ValueError('There is no data named {} in bq'.format(config.data_name))
            queries.append('select * from `{}.{}.{}`'.format(self._dataset_ref.project,
                                                             self._dataset_ref.dataset_id,
                                                             config.data_name))
        end_timestamp = datetime.now()
        duration = (end_timestamp - start_timestamp).seconds
        self._logger.debug('Ended bq to query [{}s]'.format(duration))
        return duration, queries

    def _fill_missing_data_names(self, configs):
        for config in configs:
            if config.data_name is None:
                config.data_name = timestamp_randint_string(prefix=self._generated_data_name_prefix)

    def _check_required_resources(self, names_of_atomic_functions_to_call):
        if self._bq_client is None and any('bq' in n for n in names_of_atomic_functions_to_call):
            raise ValueError('bq_client must be given if bq is used')

        if self._dataset_ref is None and any('bq' in n for n in names_of_atomic_functions_to_call):
            raise ValueError('dataset_ref must be given if bq is used')

        if self._bucket is None and any('gs' in n for n in names_of_atomic_functions_to_call):
            raise ValueError('bucket must be given if gs is used')

        if self._local_dir_path is None and any('local' in n for n in names_of_atomic_functions_to_call):
            raise ValueError('local_dir_path must be given if local is used')

    def xmload(self, configs):
        """It works like :meth:`google_pandas_load.Loader.mload` but also returns extra informations about the data
        and the mload job's execution.

        Args:
            configs (list of google_pandas_load.LoadConfig): See :class:`google_pandas_load.LoadConfig` for the format
                of one configuration.

        Returns:
            args.Namespace: The xmload result res with the following attributes:

                - res.load_results (list of (str or NoneType or pandas.DataFrame)): A list of load results.

                - res.data_names (list of str): The names of the data. The i-th element is the data_name attached to
                  configs[i], either given as an argument or generated by the loader.

                - res.duration (int): The mload job's duration.

                - res.durations (args.Namespace): A report res.durations providing the duration of each step of
                  the mload job.

                - res.query_cost (float or NoneType): The query cost in US dollars of the query_to_bq part if any.

                - res.query_costs (list of (float or NoneType)): The query costs in US dollars of the mload. The i-th
                  element is the query cost of the load job configured by configs[i].
        """
        configs = [deepcopy(config) for config in configs]
        nb_of_configs = len(configs)
        self._fill_missing_data_names(configs=configs)
        data_names = [config.data_name for config in configs]
        check_no_prefix(strings=data_names)
        sliced_configs = [config.slice_config() for config in configs]

        names_of_atomic_functions_to_call = union_keys(dicts=sliced_configs)
        self._check_required_resources(names_of_atomic_functions_to_call=names_of_atomic_functions_to_call)

        load_results = dict()
        duration = 0
        durations = dict()
        query_cost = None
        query_costs = dict()
        for n, f in zip(ATOMIC_FUNCTION_NAMES, self._atomic_functions):
            f_indices = []
            f_configs = []
            for i, s in enumerate(sliced_configs):
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
        """Execute several load jobs specified by the configurations. The prefix m means multi.

        The BigQuery Client executes simultaneously the query_to_bq parts (resp. the bq_to_gs and gs_to_bq parts) from
        the configurations by batch of size max_concurrent_google_jobs.

        Args:
            configs (list of google_pandas_load.LoadConfig): See :class:`google_pandas_load.LoadConfig` for the format
                of one configuration.

        Returns:
            list of (str or NoneType or pandas.DataFrame): A list of of load results. The i-th element is the result of
            the load job configured by configs[i]. See :meth:`google_pandas_load.Loader.load` for the
            format of one load result.
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
            bq_schema=None,

            delete_in_bq=True,
            delete_in_gs=True,
            delete_in_local=True):
        """It works like  :meth:`google_pandas_load.Loader.load` but also returns extra informations
        about the data and the load job's execution. The prefix x is for extra.

        Returns:
            argparse.Namespace: A xload result res with the following attributes:

            - res.load_result (str or NoneType or pandas.DataFrame): The result of the load job.

            - res.data_name (str): The `name <named_>`_ of the loaded data.

            - res.duration (int): The load job's duration in seconds.

            - res.durations (argparse.Namespace): A report providing the durations of each step of the
              load job. It has the following attributes:

              * res.durations.query_to_bq (int or NoneType): the duration in seconds of the query_to_bq part if any.

              * res.durations.bq_to_gs (int or NoneType): the duration in seconds of the bq_to_gs part if any.

              * res.durations.gs_to_local (int or NoneType): the duration in seconds of the gs_to_local part if any.

              * res.durations.local_to_dataframe (int or NoneType): the duration in seconds of the local_to_dataframe
                part if any.

              * res.durations.dataframe_to_local (int or NoneType): the duration in seconds of the dataframe_to_local
                part if any.

              * res.durations.local_to_gs (int or NoneType): the duration in seconds of the local_to_gs part if any.

              * res.durations.gs_to_bq (int or NoneType): the duration in seconds of the gs_to_bq part if any.

              * res.durations.bq_to_query (int or NoneType): the duration in seconds of the bq_to_query part if any.

            - res.query_cost (float or NoneType): The query cost in US dollars of the query_to_bq part if any.
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
            bq_schema=bq_schema,

            delete_in_bq=delete_in_bq,
            delete_in_gs=delete_in_gs,
            delete_in_local=delete_in_local)

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
            bq_schema=None,

            delete_in_bq=True,
            delete_in_gs=True,
            delete_in_local=True):
        """Execute a load job whose configuration is specified by the arguments.

         The data is transferred from source to destination. The valid values for the source and the destination are:
         'query', 'bq', 'gs', 'local' and 'dataframe'.

         Downloading follows the path : 'query' -> 'bq' -> 'gs' -> 'local' -> 'dataframe' while uploading goes in the
         opposite direction.

         .. _moved:

         Warning:
             **In general, data is moved, not copied!**

             Once the load job has been executed, the data usually does not exist anymore in the source and
             in any transitional locations.

             However two exceptions exist:

             - When source = 'dataframe', the dataframe is not deleted in RAM.
             - When destination = ‘query’, the data is not deleted in BigQuery, so that it still exists somewhere.
               Indeed, in this case, the load job returns a simple query which represents the data but does not
               contain it.

             Use the delete_in_bq, delete_in_gs and delete_in_local parameters to control the data deletion,
             during the execution of the load job.


         .. _pre-deletion:

         Warning:
             **In general, pre-existing data is deleted!**

             Before new data is moved to any location, the loader will usually delete any prior data bearing the same
             name to prevent any conflict.

             There is one exception:

             - When destination = ‘bq’ and the write_dispostion parameter is set to ‘WRITE_APPEND’, new data is
               appended to pre-existing one with the same name.

         Args:
             source (str): one of 'query', 'bq', 'gs', 'local', 'dataframe'.
             destination (str): one of 'query', 'bq', 'gs', 'local', 'dataframe'.

             data_name (str, optional): The `name <named_>`_ of the data. If not passed, a name is generated by
                concatenating the generated_data_name_prefix of the loader, if any, the current timestamp and a random
                integer. This is useful when source = 'query' and destination = 'dataframe' because the user may not
                need to know the data_name.

             query (str, optional): A BigQuery Standard Sql query. Required if source = 'query'.
             dataframe (pandas.DataFrame, optional): A pandas dataframe. Required if source = 'dataframe'.

             write_disposition (google.cloud.bigquery.job.WriteDisposition, optional): Specifies the action that occurs
                if data named_ data_name already exist in BigQuery. Defaults to 'WRITE_TRUNCATE'.
             dtype (dict, optional): When destination = 'dataframe', pandas.read_csv() is used and dtype is one
                of its parameters.
             parse_dates (list of str, optional): When destination = 'dataframe', pandas.read_csv() is used and
                parse_dates is one of its parameters.
             infer_datetime_format (bool, optional): When destination = 'dataframe', pandas.read_csv() is used and
                infer_datetime_format is one of its parameters. Defaults to True.
             date_cols (list of str, optional): If no bq_schema is passed, indicate which columns of a pandas dataframe
                should have the BigQuery type DATE.
             timestamp_cols (list of str, optional): If no bq_schema is passed, indicate which columns of a pandas
                dataframe should have the BigQuery type TIMESTAMP.
             bq_schema (list of google.cloud.bigquery.schema.SchemaField, optional): The table's schema in BigQuery.
                 Used when destination = 'bq' and source != 'query'. When source = 'query', the bq_schema is inferred
                 from the query. If not passed and source = 'dataframe', falls back to an inferred value
                 from the dataframe with :meth:`google_pandas_load.LoadConfig.bq_schema_inferred_from_dataframe`.

             delete_in_bq (bool, optional): If set to False, data going from or through Bigquery is not deleted
                in BigQuery. Defaults to True.
             delete_in_gs (bool, optional): If set to False, data going from or through Storage is not deleted in
                Storage. Defaults to True.
             delete_in_local (bool, optional): If set to False, data going from or through the local folder is not
                deleted in that folder. Defaults to True.

         Returns:
             str or pandas.DataFrame or NoneType: The result of the load job:

             - When destination = 'query', it returns the BigQuery standard SQL query:
               "select * from \`project_id.dataset_id.data_name\`",
               where the project_id is the dataset's one.
             - When destination = 'dataframe', it returns a pandas dataframe populated with the data specified
               by the arguments.
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
                    bq_schema=bq_schema,

                    delete_in_bq=delete_in_bq,
                    delete_in_gs=delete_in_gs,
                    delete_in_local=delete_in_local).load_result
