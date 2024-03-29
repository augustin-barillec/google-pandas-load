google-pandas-load documentation
================================

.. image:: https://img.shields.io/pypi/v/google-pandas-load
    :target: https://pypi.org/project/google-pandas-load

.. image:: https://img.shields.io/pypi/l/google-pandas-load.svg
    :target: https://pypi.org/project/google-pandas-load

.. image:: https://img.shields.io/pypi/pyversions/google-pandas-load.svg
    :target: https://pypi.org/project/google-pandas-load

.. image:: https://codecov.io/gh/augustin-barillec/google-pandas-load/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/augustin-barillec/google-pandas-load

.. image:: https://pepy.tech/badge/google-pandas-load
    :target: https://pepy.tech/project/google-pandas-load

Wrapper for transferring data from A to B, where A and B are distinct
and chosen between BigQuery, Storage, a local directory and pandas.

Acknowledgements
----------------
I am grateful to my employer Easyence_ for providing me the resources to develop this library and for allowing me
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
`here <https://googleapis.dev/python/google-api-core/latest/auth.html>`__.

.. code-block:: python

    from google_pandas_load import LoaderQuickSetup

    gpl = LoaderQuickSetup(
        project_id='pi',
        dataset_name='dn',
        bucket_name='bn',
        bucket_dir_path='tmp',
        local_dir_path='/tmp',
        credentials=None)

Transfer data seamlessly from and to various locations:

.. warning::
   The loader will delete any prior existing data having the same name in any
   location it will go through or at.

   Explanation for this choice and one example can be found `here <Loader.html#pre-deletion>`__.

.. note::
   If the optional argument bucket_dir_path is not given, data will be stored at the root of the bucket.
   It is a good practice to specify this argument so that data is stored in a defined bucket directory.

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
        destination='dataset',
        dataframe=df,
        data_name='a0')

    # Extract the data to Storage.
    gpl.load(
        source='dataset',
        destination='bucket',
        data_name='a0')

    # Download the data to the local directory.
    gpl.load(
        source='bucket',
        destination='local',
        data_name='a0')

Launch simultaneously several load jobs with massive parallelization of the query_to_dataset and dataset_to_bucket steps.
This is made possible by BigQuery.

.. code-block:: python

    from google_pandas_load import LoadConfig

    # Build the load configs.
    configs = []
    for i in range(100):
        config = LoadConfig(
            source='query',
            destination='local',
            query=f'select {i} as x',
            data_name=f'b{i}')
        configs.append(config)

    # Launch all the load jobs at the same time.
    gpl.multi_load(configs=configs)

Main features
-------------

- Transfer data seamlessly from and to various locations.
- Launch several load jobs simultaneously.
- Massive parallelization of the cloud steps with BigQuery.

Limitation
-----------

- Only simple types can be downloaded or uploaded.

The methods:

    - `google.cloud.bigquery.job.QueryJob.to_dataframe()`_
    - `google.cloud.bigquery.client.Client.load_table_from_dataframe()`_

can handle more types.


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

   API
   history

.. _Easyence: https://www.easyence.com/

.. _Google Cloud Client Library for Python: https://github.com/googleapis/google-cloud-python#google-cloud-python-client
.. _pandas: https://pandas.pydata.org/pandas-docs/stable/index.html

.. _google.cloud.bigquery.job.QueryJob.to_dataframe(): https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJob.html#google.cloud.bigquery.job.QueryJob.to_dataframe
.. _google.cloud.bigquery.client.Client.load_table_from_dataframe(): https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_dataframe

.. _google.cloud.bigquery.client.query(): https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.query
.. _google.cloud.bigquery.client.extract_table(): https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.extract_table
.. _google.cloud.storage.blob.Blob.download_to_filename(): https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.download_to_filename
.. _pandas.read_csv(): https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

.. _pandas.DataFrame.to_csv(): https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html
.. _google.cloud.storage.blob.Blob.upload_from_filename(): https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.upload_from_filename
.. _google.cloud.bigquery.client.load_table_from_uri(): https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_uri
