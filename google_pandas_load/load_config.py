from argparse import Namespace
from google.cloud import bigquery
from pandas.api.types import infer_dtype
from google_pandas_load.utils import build_atomic_function_names
from google_pandas_load.constants import LOCATIONS, SOURCE_LOCATIONS, \
    DESTINATION_LOCATIONS, REVERSED_LOCATIONS, MIDDLE_LOCATIONS


class LoadConfig:
    """Configuration for a load job.

    This class has the same parameters as
    :meth:`google_pandas_load.loader.Loader.load`. It is used to launch
    simultaneously load jobs as follows:

    - A list of LoadConfig is built.
    - The list is passed to :meth:`google_pandas_load.loader.Loader.mload`.
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
            date_cols=None,
            timestamp_cols=None,
            bq_schema=None):

        self.source = source
        self.destination = destination

        self.data_name = data_name
        self._query = query
        self._dataframe = dataframe

        self._write_disposition = write_disposition
        self._dtype = dtype
        self._parse_dates = parse_dates
        self._timestamp_cols = timestamp_cols
        self._date_cols = date_cols
        self._bq_schema = bq_schema

        self._check_source_value()
        self._check_destination_value()
        self._check_source_different_from_destination()
        self._check_if_data_name_missing()
        self._check_if_query_missing()
        self._check_if_dataframe_missing()

        if self._bq_schema is None and self._dataframe is not None:
            self._infer_bq_schema_from_dataframe()

    def _check_source_value(self):
        msg = ("source must be one of 'query' or 'bq' or 'gs' or 'local' "
               "or 'dataframe")
        if self.source not in SOURCE_LOCATIONS:
            raise ValueError(msg)

    def _check_destination_value(self):
        msg = ("destination must be one of 'bq' or 'gs' or 'local' "
               "or 'dataframe'")
        if self.destination not in DESTINATION_LOCATIONS:
            raise ValueError(msg)

    def _check_source_different_from_destination(self):
        if self.source == self.destination:
            raise ValueError('source must be different from destination')

    def _check_if_data_name_missing(self):
        msg = ("data_name must be given if source or destination is one of "
               "'bq' or 'gs' or 'local'")
        condition_1 = self.data_name is None
        condition_2 = self.source in MIDDLE_LOCATIONS
        condition_3 = self.destination in MIDDLE_LOCATIONS

        if condition_1 and (condition_2 or condition_3):
            raise ValueError(msg)

    def _check_if_query_missing(self):
        if self.source == 'query' and self._query is None:
            raise ValueError("query must be given if source = 'query'")

    def _check_if_dataframe_missing(self):
        if self.source == 'dataframe' and self._dataframe is None:
            raise ValueError("dataframe must be given if source = 'dataframe'")

    @staticmethod
    def bq_schema_inferred_from_dataframe(
            dataframe, date_cols=None, timestamp_cols=None):
        """Return a BigQuery schema that is inferred from a pandas dataframe.

        Let infer_dtype(column) = `pandas.api.types.infer_dtype <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.api.types.infer_dtype.html>`__ (column).

        In BigQuery, a column is given its type according to the following
        rule:

        - if its name is listed in the date_cols parameter, its type in
          BigQuery should be DATE.
        - elif its name is listed in the timestamp_cols parameter, its type
          in BigQuery should be TIMESTAMP.
        - elif infer_dtype(column) = 'boolean', its type in BigQuery is
          BOOLEAN.
        - elif infer_dtype(column) = 'integer', its type in BigQuery is
          INTEGER.
        - elif infer_dtype(column) = 'floating', its type in BigQuery is
          FLOAT.
        - else its type in BigQuery is STRING.

        Args:
            dataframe (pandas.DataFrame): The dataframe.
            date_cols (list of str, optional): The names of the columns
                receiving the BigQuery type DATE.
            timestamp_cols (list of str, optional): The names of the columns
                receiving the BigQuery type TIMESTAMP.

        Returns:
            list of google.cloud.bigquery.schema.SchemaField: A BigQuery
            schema.
        """
        if len(dataframe.columns) == 0:
            msg = ('A non empty bq_schema cannot be inferred '
                   'from a dataframe with no columns')
            raise ValueError(msg)
        if timestamp_cols is None:
            timestamp_cols = []
        if date_cols is None:
            date_cols = []
        bq_schema = []
        for col in dataframe.columns:
            dtype_description = infer_dtype(dataframe[col])
            if col in date_cols:
                bq_schema.append(bigquery.SchemaField(name=col,
                                                      field_type='DATE'))
            elif col in timestamp_cols:
                bq_schema.append(bigquery.SchemaField(name=col,
                                                      field_type='TIMESTAMP'))
            elif dtype_description == 'boolean':
                bq_schema.append(bigquery.SchemaField(name=col,
                                                      field_type='BOOLEAN'))
            elif dtype_description == 'integer':
                bq_schema.append(bigquery.SchemaField(name=col,
                                                      field_type='INTEGER'))
            elif dtype_description == 'floating':
                bq_schema.append(bigquery.SchemaField(name=col,
                                                      field_type='FLOAT'))
            else:
                bq_schema.append(bigquery.SchemaField(name=col,
                                                      field_type='STRING'))
        return bq_schema

    def _infer_bq_schema_from_dataframe(self):
        self._bq_schema = self.bq_schema_inferred_from_dataframe(
            dataframe=self._dataframe,
            timestamp_cols=self._timestamp_cols,
            date_cols=self._date_cols)

    @property
    def _names_of_atomic_functions_to_call(self):
        index_source = LOCATIONS.index(self.source)
        index_destination = LOCATIONS.index(self.destination)
        rindex_source = REVERSED_LOCATIONS.index(self.source)
        rindex_destination = REVERSED_LOCATIONS.index(self.destination)
        if index_source < index_destination:
            return build_atomic_function_names(
                LOCATIONS[index_source: index_destination + 1])
        else:
            return build_atomic_function_names(
                REVERSED_LOCATIONS[rindex_source: rindex_destination + 1])

    def _query_to_bq_config(self):
        return Namespace(
            query=self._query,
            write_disposition=self._write_disposition)

    def _local_to_dataframe_config(self):
        return Namespace(
            dtype=self._dtype,
            parse_dates=self._parse_dates)

    def _dataframe_to_local_config(self):
        return Namespace(dataframe=self._dataframe)

    def _gs_to_bq_config(self):
        return Namespace(
            schema=self._bq_schema,
            write_disposition=self._write_disposition)

    @property
    def sliced_config(self):
        res = dict()
        for i, n in enumerate(self._names_of_atomic_functions_to_call):
            atomic_config_name = '_' + n + '_config'
            if atomic_config_name in dir(self):
                res[n] = getattr(self, atomic_config_name)()
            else:
                res[n] = Namespace()
            res[n].data_name = self.data_name
            source, destination = n.split('_to_')
            res[n].source = source
            res[n].destination = destination
            if res[n].source in MIDDLE_LOCATIONS:
                res[n].clear_source = (i != 0)
        return res
