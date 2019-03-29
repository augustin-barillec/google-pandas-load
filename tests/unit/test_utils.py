import unittest
import pandas
from google_pandas_load import LoadConfig
from tests.context.loaders import gpl1


class UtilsTest(unittest.TestCase):

    def test_instantiation_of_LoadConfig_raises_error_if_source_or_destination_is_invalid
