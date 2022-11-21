import pandas
from tests.utils import pandas_normalize


def assert_equal(df1, df2):
    pandas.testing.assert_frame_equal(
        pandas_normalize.normalize(df1),
        pandas_normalize.normalize(df2),
        check_dtype=False)
