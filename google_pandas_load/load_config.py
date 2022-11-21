import pandas
from argparse import Namespace
from typing import Literal, List, Dict, Any, Optional
from google.cloud import bigquery
from google_pandas_load import constants, utils


class LoadConfig:
    """Configuration for a load job.

    This class has the same parameters as
    :meth:`google_pandas_load.loader.Loader.load`. It is used to launch
    simultaneously load jobs as follows:

    - A list of LoadConfig is built.
    - The list is passed
      to :meth:`google_pandas_load.loader.Loader.multi_load`.
    """
    def __init__(
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

        if self.data_name is not None:
            self._check_data_name_not_empty_string()
            self._check_data_name_not_contain_slash()
        self._check_source_value()
        self._check_destination_value()
        self._check_source_different_from_destination()
        self._check_if_data_name_missing()
        self._check_if_query_missing()
        self._check_if_dataframe_missing()

        if self._bq_schema is None and self._dataframe is not None:
            self._infer_bq_schema_from_dataframe()

    def _check_data_name_not_empty_string(self):
        assert self.data_name is not None
        if self.data_name == '':
            msg = 'data_name must not be the empty string'
            raise ValueError(msg)

    def _check_data_name_not_contain_slash(self):
        assert self.data_name is not None
        utils.check_data_name_not_contain_slash(self.data_name)

    def _check_source_value(self):
        if self.source not in constants.SOURCE_LOCATIONS:
            msg = ("source must be one of 'query' or 'dataset' "
                   "or 'bucket' or 'local' or 'dataframe")
            raise ValueError(msg)

    def _check_destination_value(self):
        if self.destination not in constants.DESTINATION_LOCATIONS:
            msg = ("destination must be one of 'dataset' "
                   "or 'bucket' or 'local' or 'dataframe'")
            raise ValueError(msg)

    def _check_source_different_from_destination(self):
        if self.source == self.destination:
            raise ValueError('source must be different from destination')

    def _check_if_data_name_missing(self):
        c1 = self.data_name is None
        c2 = self.source in constants.MIDDLE_LOCATIONS
        c3 = self.destination in constants.MIDDLE_LOCATIONS
        if c1 and (c2 or c3):
            msg = ("data_name must be provided if source or destination is "
                   "one of 'dataset' or 'bucket' or 'local'")
            raise ValueError(msg)

    def _check_if_query_missing(self):
        if self.source == 'query' and self._query is None:
            raise ValueError("query must be provided if source = 'query'")

    def _check_if_dataframe_missing(self):
        if self.source == 'dataframe' and self._dataframe is None:
            raise ValueError(
                "dataframe must be provided if source = 'dataframe'")

    @staticmethod
    def bq_schema_inferred_from_dataframe(
            dataframe: pandas.DataFrame,
            date_cols: Optional[List[str]] = None,
            timestamp_cols: Optional[List[str]] = None) \
            -> List[bigquery.SchemaField]:
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
            dtype_description = pandas.api.types.infer_dtype(dataframe[col])
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
            elif dtype_description in ('floating', 'mixed-integer-float'):
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
    def _names_atomic_functions_to_call(self):
        index_source = constants.LOCATIONS.index(self.source)
        index_destination = constants.LOCATIONS.index(self.destination)
        rindex_source = constants.REVERSED_LOCATIONS.index(self.source)
        rindex_destination = constants.REVERSED_LOCATIONS.index(
            self.destination)
        if index_source < index_destination:
            return utils.build_atomic_function_names(
                constants.LOCATIONS[index_source: index_destination + 1])
        else:
            return utils.build_atomic_function_names(
                constants.REVERSED_LOCATIONS
                [rindex_source: rindex_destination + 1])

    def _query_to_dataset_config(self):
        return Namespace(
            query=self._query,
            write_disposition=self._write_disposition)

    def _local_to_dataframe_config(self):
        return Namespace(
            dtype=self._dtype,
            parse_dates=self._parse_dates)

    def _dataframe_to_local_config(self):
        return Namespace(dataframe=self._dataframe)

    def _bucket_to_dataset_config(self):
        return Namespace(
            schema=self._bq_schema,
            write_disposition=self._write_disposition)

    @property
    def sliced(self):
        res = dict()
        for i, n in enumerate(self._names_atomic_functions_to_call):
            atomic_config_name = f'_{n}_config'
            if atomic_config_name in dir(self):
                res[n] = getattr(self, atomic_config_name)()
            else:
                res[n] = Namespace()
            res[n].data_name = self.data_name
            source, destination = n.split('_to_')
            res[n].source = source
            res[n].destination = destination
            if res[n].source in constants.MIDDLE_LOCATIONS:
                res[n].clear_source = (i != 0)
        return res
