import time
from datetime import datetime
from random import randint
from google.cloud.exceptions import NotFound


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


def table_exists(client, table_reference):
    try:
        client.get_table(table_ref=table_reference)
        return True
    except NotFound:
        return False


def timestamp_randint_string(prefix=None):
    if prefix is None:
        prefix = ''
    return prefix + datetime.now().strftime('%Y%m%d%H%M%S_%f') + '_rand' + str(randint(0, 10**4))


def build_atomic_function_names(locations):
    res = set()
    for i in range(len(locations) - 1):
        res.add(locations[i] + '_to_' + locations[i + 1])
    return res


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