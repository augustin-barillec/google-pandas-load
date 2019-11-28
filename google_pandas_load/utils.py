import time
import uuid
from datetime import datetime
from google.cloud.exceptions import NotFound


def table_exists(client, table_reference):
    try:
        client.get_table(table_reference)
        return True
    except NotFound:
        return False


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return True
        time.sleep(1)


def wait_for_jobs(jobs):
    for job in jobs:
        wait_for_job(job)


def timestamp_randint_string(prefix=None):
    if prefix is None:
        prefix = ''
    datetime_str = datetime.now().strftime('%Y%m%d%H%M%S_%f')
    random_value = '_rand' + str(uuid.uuid4().int)
    return prefix + datetime_str + random_value


def build_atomic_function_names(locations):
    return [locations[i] + '_to_' + locations[i + 1]
            for i in range(len(locations)-1)]


def check_no_prefix(strings):
    for i, dn1 in enumerate(strings):
        for j, dn2 in enumerate(strings):
            if i != j and dn2.startswith(dn1):
                raise ValueError('{} is a prefix of {}'.format(dn1, dn2))


def union_keys(dicts):
    res = set()
    for d in dicts:
        res = res.union(d.keys())
    return res


def build_numpy_leaf_types(dtype):
    subs = dtype.__subclasses__()
    if not subs:
        return [dtype]
    res = []
    for dt in subs:
        res += build_numpy_leaf_types(dt)
    return res
