import unittest
import pandas
from google_pandas_load import LoadConfig


class LoadConfigTest(unittest.TestCase):

    def test_raise_error_if_inconsistent_source_or_destination(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='queryy', destination='dataframe')
        self.assertEqual(str(cm.exception), "source must be one of 'query' or 'bq' or 'gs' or 'local' or 'dataframe'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='dataframee')
        self.assertEqual(
            str(cm.exception), "destination must be one of 'query' or 'bq' or 'gs' or 'local' or 'dataframe'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='query')
        self.assertEqual(str(cm.exception), 'source must be different from destination')

    def test_raise_error_if_missing_required_values(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='dataframe')
        self.assertEqual(str(cm.exception), "query must be given if source == 'query'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='dataframe', destination='local')
        self.assertEqual(str(cm.exception), "dataframe must be given if source == 'dataframe'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='gs', query='select 3')
        self.assertEqual(
            str(cm.exception), "data_name must be given if source or destination is one of 'bq' or 'gs' or 'local'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='gs', destination='bq', data_name='e0')
        self.assertEqual(str(cm.exception), 'bq_schema is missing')

    def test_raise_error_if_no_columns(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig.bq_schema_inferred_from_dataframe(dataframe=pandas.DataFrame(data={}))
        self.assertEqual(str(cm.exception), 'A bq_schema cannot be inferred from a dataframe with no columns')
