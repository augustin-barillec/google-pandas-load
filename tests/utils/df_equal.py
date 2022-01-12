from tests.utils import df_normalize


def normalize_equal(df1, df2):
    normalized_df1 = df_normalize.normalize(df1)
    normalized_df2 = df_normalize.normalize(df2)
    return normalized_df1.equals(normalized_df2)
