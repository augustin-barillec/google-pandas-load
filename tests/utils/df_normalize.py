from copy import deepcopy


def sort(df):
    res = deepcopy(df)
    cols = list(res.columns)
    res = res.sort_values(cols)
    return res


def reset_index(df):
    res = deepcopy(df)
    return res.reset_index(drop=True)


def normalize(df):
    res = sort(df)
    res = reset_index(res)
    return res
