from argparse import Namespace
from google.cloud import bigquery
from .utils import build_atomic_function_names
from .constants import LOCATIONS, REVERSED_LOCATIONS, MIDDLE_LOCATIONS, ATOMIC_FUNCTION_NAMES


class LoadConfig:
    """Configuration for a load job.

    This class has the same parameters than :meth:`google_pandas_load.Loader.load`. It is used to execute
    simultaneously load jobs as follows:

    - A list of LoadConfig configs is built.
    - configs is passed to :meth:`google_pandas_load.Loader.mload`.
    """

    def __init__(
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

        self._source = source
        self._destination = destination

        self.data_name = data_name
        self._query = query
        self._dataframe = dataframe

        self._write_disposition = write_disposition
        self._dtype = dtype
        self._parse_dates = parse_dates
        self._infer_datetime_format = infer_datetime_format
        self._timestamp_cols = timestamp_cols
        self._date_cols = date_cols
        self._bq_schema = bq_schema

        self._delete_in_bq = delete_in_bq
        self._delete_in_gs = delete_in_gs
        self._delete_in_local = delete_in_local

        self._check_source_destination()
        self._check_required_values()

        self._names_of_atomic_functions_to_call = self._build_names_of_atomic_functions_to_call()

        if self._bq_schema is None and self._dataframe is not None:
            self._infer_bq_schema_from_dataframe()

    def _check_source_destination(self):
        if self._source not in LOCATIONS:
            raise ValueError("source must be one of 'query' or 'bq' or 'gs' or 'local' or 'dataframe'")

        if self._destination not in LOCATIONS:
            raise ValueError("destination must be one of 'query' or 'bq' or 'gs' or 'local' or 'dataframe'")

        if self._source == self._destination:
            raise ValueError('source must be different from destination')

    def _check_required_values(self):
        if self._source == 'query' and self._query is None:
            raise ValueError("query must be given if source == 'query'")

        if self._source == 'dataframe' and self._dataframe is None:
            raise ValueError("dataframe must be given if source == 'dataframe'")

        if self.data_name is None and (self._source in MIDDLE_LOCATIONS or self._destination in MIDDLE_LOCATIONS):
            raise ValueError("data_name must be given if source or destination is one of 'bq' or 'gs' or 'local'")

        if self._source in ('local', 'gs') and self._destination in ('bq', 'query') and self._bq_schema is None:
            raise ValueError('bq_schema is missing')

    def _build_names_of_atomic_functions_to_call(self):
        index_source = LOCATIONS.index(self._source)
        index_destination = LOCATIONS.index(self._destination)
        rindex_source = REVERSED_LOCATIONS.index(self._source)
        rindex_destination = REVERSED_LOCATIONS.index(self._destination)
        if index_source < index_destination:
            return build_atomic_function_names(LOCATIONS[index_source: index_destination + 1])
        else:
            return build_atomic_function_names(REVERSED_LOCATIONS[rindex_source: rindex_destination + 1])

    @staticmethod
    def bq_schema_inferred_from_dataframe(dataframe, date_cols=None, timestamp_cols=None):
        """Return a BigQuery schema inferred from a pandas dataframe schema.

        The pandas columns are given BigQuery types as follow and in this order of priority :

        - the columns whose name are in the list parameter date_cols are given the BigQuery type DATE.
        - the columns whose name are in the list parameter timestamp_cols are given the BigQuery type TIMESTAMP.
        - the columns with python type bool are given the BigQuery type BOOLEAN.
        - the columns with python type int are given the BigQuery type INTEGER.
        - the columns with python type float are given the BigQuery type FLOAT.
        - the other columns are given the BigQuery type STRING.

        Args:
            dataframe (pandas.DataFrame): The dataframe.
            date_cols (list of str, optional): The names of the columns receiving the BigQuery type DATE in
                the result.
            timestamp_cols (list of str, optional): The names of the columns receiving the BigQuery type TIMESTAMP in
                the result.

        Returns:
            list of google.cloud.bigquery.schema.SchemaField: A BigQuery schema.
        """
        if len(dataframe.columns) == 0:
            raise ValueError('A bq_schema cannot be inferred from a dataframe with no columns')
        if timestamp_cols is None:
            timestamp_cols = []
        if date_cols is None:
            date_cols = []
        bq_schema = []
        for col in dataframe.columns:
            dtype = dataframe[col].dtype
            if col in date_cols:
                bq_schema.append(bigquery.SchemaField(name=col, field_type='DATE'))
            elif col in timestamp_cols:
                bq_schema.append(bigquery.SchemaField(name=col, field_type='TIMESTAMP'))
            elif dtype == bool:
                bq_schema.append(bigquery.SchemaField(name=col, field_type='BOOLEAN'))
            elif dtype == int:
                bq_schema.append(bigquery.SchemaField(name=col, field_type='INTEGER'))
            elif dtype == float:
                bq_schema.append(bigquery.SchemaField(name=col, field_type='FLOAT'))
            else:
                bq_schema.append(bigquery.SchemaField(name=col, field_type='STRING'))
        return bq_schema

    def _infer_bq_schema_from_dataframe(self):
        self._bq_schema = self.bq_schema_inferred_from_dataframe(
            dataframe=self._dataframe,
            timestamp_cols=self._timestamp_cols,
            date_cols=self._date_cols)

    def _query_to_bq_config(self):
        return Namespace(
            data_name=self.data_name,
            query=self._query,
            write_disposition=self._write_disposition)

    def _bq_to_gs_config(self):
        return Namespace(
            data_name=self.data_name,
            delete_in_source=self._delete_in_bq)

    def _gs_to_local_config(self):
        return Namespace(
            data_name=self.data_name,
            delete_in_source=self._delete_in_gs)

    def _local_to_dataframe_config(self):
        return Namespace(
            data_name=self.data_name,
            dtype=self._dtype,
            parse_dates=self._parse_dates,
            infer_datetime_format=self._infer_datetime_format,
            delete_in_source=self._delete_in_local)

    def _dataframe_to_local_config(self):
        return Namespace(
            data_name=self.data_name,
            dataframe=self._dataframe)

    def _local_to_gs_config(self):
        return Namespace(
            data_name=self.data_name,
            delete_in_source=self._delete_in_local)

    def _gs_to_bq_config(self):
        return Namespace(
            data_name=self.data_name,
            schema=self._bq_schema,
            write_disposition=self._write_disposition,
            delete_in_source=self._delete_in_gs)

    def _bq_to_query_config(self):
        return Namespace(
            data_name=self.data_name)

    def slice_config(self):
        atomic_configs = [
            self._query_to_bq_config(),
            self._bq_to_gs_config(),
            self._gs_to_local_config(),
            self._local_to_dataframe_config(),
            self._dataframe_to_local_config(),
            self._local_to_gs_config(),
            self._gs_to_bq_config(),
            self._bq_to_query_config()]
        res = dict()
        for n, c in zip(ATOMIC_FUNCTION_NAMES, atomic_configs):
            if n in self._names_of_atomic_functions_to_call:
                res[n] = c
        return res
