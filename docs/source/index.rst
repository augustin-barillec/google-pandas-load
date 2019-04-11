google-pandas-load documentation
================================

Release v\ |release|.

.. image:: https://img.shields.io/pypi/l/google-pandas-load.svg
    :target: https://pypi.org/project/google-pandas-load/

.. image:: https://img.shields.io/pypi/pyversions/google-pandas-load.svg
    :target: https://pypi.org/project/google-pandas-load/

google-pandas-load is a wrapper library for transferring big data from A to B, where A and B are distinct
and chosen between BigQuery, Storage, a local folder and pandas.

This library enables faster data transfer than those performed by `Python Client for Google BigQuery`_'s methods:

- `google.cloud.bigquery.job.QueryJob.to_dataframe()`_
- `google.cloud.bigquery.client.Client.load_table_from_dataframe()`_

See `Speed Comparison`_.

Acknowledgements
----------------
I am grateful to my employer Ysance_ for providing me the resources to develop this library and for allowing me
to publish it.


Installation
------------

::

    $ pip install google-pandas-load

Quickstart
----------

Set up a loader.

In the following code, the credentials are inferred from the environment.
For further information about how to authenticate to Google Cloud Platform with the
`Google Cloud Client Library for Python`_, have a look
`here <https://googleapis.github.io/google-cloud-python/latest/core/auth.html?highlight=defaults/>`__.

.. code-block:: python

    from google_pandas_load import LoaderQuickSetup

    gpl = LoaderQuickSetup(
        project_id='pi',
        dataset_id='di',
        bucket_name='bn',
        local_dir_path='/tmp',
        credentials=None)

Transfer data seamlessly from and to various locations:

.. warning::
   In general, data is moved, not copied! The precise behavior is stated `here <Loader.html#moved>`__.

.. warning::
   In general, before data is moved to any location, it will delete any prior existing data having the same name in
   the location. This ensures a clean space for the upcoming data.
   The precise behavior is stated `here <Loader.html#pre-deletion>`__.

.. code-block:: python

    # Populate a dataframe with a query result.
    df = gpl.load(
        source='query',
        destination='dataframe',
        query='select 3 as x')

    # Apply a python transformation to the data.
    df['x'] = 2*df['x']

    # Upload the result to BigQuery.
    gpl.load(
        source='dataframe',
        destination='bq',
        data_name='a0',
        dataframe=df)

    # Extract the data to Storage.
    gpl.load(
        source='bq',
        destination='gs',
        data_name='a0')
    # The data is not in BigQuery anymore.
    # See warning above.

    # Download the data to the local folder
    # without deleting it in Storage.
    gpl.load(
        source='gs',
        destination='local',
        data_name='a0',
        delete_in_gs=False)

Launch simultaneously several load jobs with massive parallelization of the query_to_bq and bq_to_gs steps.
This is made possible by BigQuery.

.. code-block:: python

    from google_pandas_load import LoadConfig

    # Build the load configs.
    configs = []
    for i in range(100):
        config = LoadConfig(
            source='query',
            destination='local',
            data_name='b{}'.format(i),
            query='select {} as x'.format(i))
        configs.append(config)

    # Launch all the load jobs
    # at the same time.
    gpl.mload(configs=configs)

Main features
-------------

- Transfer big data faster (see `Speed Comparison`_).
- Transfer data seamlessly from and to various locations.
- Launch several load jobs simultaneously.
- Massive parallelization of the cloud steps with BigQuery.
- Monitor query costs and step durations of load jobs.

The basic mechanism
-------------------

This code essentially chains transferring data functions from the `Google Cloud Client Library for Python`_
and from pandas_.

To download, the following functions are chained:

- `google.cloud.bigquery.client.query()`_
- `google.cloud.bigquery.client.extract_table()`_
- `google.cloud.storage.blob.Blob.download_to_filename()`_
- `pandas.read_csv()`_

To upload, the following functions are chained:

- `pandas.DataFrame.to_csv()`_
- `google.cloud.storage.blob.Blob.upload_from_filename()`_
- `google.cloud.bigquery.client.load_table_from_uri()`_


Required packages
-----------------

- google-cloud-bigquery
- google-cloud-storage
- pandas

Table of Contents
-----------------

.. toctree::
   :maxdepth: 3

   Tutorial
   Speed_comparison
   API

.. _`Python Client for Google BigQuery`: https://googleapis.github.io/google-cloud-python/latest/bigquery/index.html

.. _`Speed Comparison`: Speed_comparison.ipynb

.. _Ysance: https://www.ysance.com/data-services/fr/home/

.. _`google.cloud.bigquery.job.QueryJob.to_dataframe()`: https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.job.QueryJob.to_dataframe.html#google.cloud.bigquery.job.QueryJob.to_dataframe
.. _`google.cloud.bigquery.client.Client.load_table_from_dataframe()`: https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.client.Client.load_table_from_dataframe.html#google.cloud.bigquery.client.Client.load_table_from_dataframe

.. _`Google Cloud Client Library for Python`: https://googleapis.github.io/google-cloud-python/latest/index.html
.. _pandas: https://pandas.pydata.org/pandas-docs/stable/index.html

.. _`google.cloud.bigquery.client.query()`: https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.query
.. _`google.cloud.bigquery.client.extract_table()`: https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.extract_table
.. _`google.cloud.storage.blob.Blob.download_to_filename()`: https://googleapis.github.io/google-cloud-python/latest/storage/blobs.html#google.cloud.storage.blob.Blob.download_to_filename
.. _`pandas.read_csv()`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

.. _`pandas.DataFrame.to_csv()`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html
.. _`google.cloud.storage.blob.Blob.upload_from_filename()`: https://googleapis.github.io/google-cloud-python/latest/storage/blobs.html#google.cloud.storage.blob.Blob.upload_from_filename
.. _`google.cloud.bigquery.client.load_table_from_uri()`: https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_uri
