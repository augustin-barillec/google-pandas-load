import uuid
from datetime import datetime
from google.cloud.exceptions import NotFound


def table_exists(bq_client, table_ref):
    try:
        bq_client.get_table(table_ref)
        return True
    except NotFound:
        return False


def wait_for_jobs(jobs):
    for job in jobs:
        job.result()


def timestamp_randint_string():
    datetime_str = datetime.now().strftime('%Y%m%d%H%M%S_%f')
    random_value = '_rand' + str(uuid.uuid4().int)
    return datetime_str + random_value


def build_atomic_function_names(locations):
    return [locations[i] + '_to_' + locations[i + 1]
            for i in range(len(locations)-1)]


def check_no_prefix(strings):
    for i, s1 in enumerate(strings):
        for j, s2 in enumerate(strings):
            if i != j and s2.startswith(s1):
                raise ValueError('{} is a prefix of {}'.format(s1, s2))


def union_keys(dicts):
    res = set()
    for d in dicts:
        res = res.union(d.keys())
    return res
