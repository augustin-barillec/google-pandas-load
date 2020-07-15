.. :changelog:

History
=======

3.0 (2020-07-15)
----------------

API Changes
^^^^^^^^^^^
* pandas==1.* is now required.

* For :class:`google_pandas_load.loader_quick_setup.LoaderQuickSetup`, the
  parameter dataset_id is replaced by the parameter dataset_name. The reason
  for this choice is explained in the first entry of the Notes section below.

Improvement
^^^^^^^^^^^
* For :meth:`google_pandas_load.loader.Loader.load`, when the parameter
  destination is set to 'bq' and the parameter source is set to 'gs' or
  'local', the bq_schema parameter is not required anymore. If it is not
  passed, it falls back to an inferred value from the CSV with
  `google.cloud.bigquery.job.LoadJobConfig.autodetect`_.


Notes
^^^^^
* We use new conventions for naming some BigQuery objects. This causes only one
  API change (the second one in the API Changes section above). Let us describe
  the new conventions with an example. Suppose we have a BigQuery table whose
  address is project1.dataset1.table1. We say that:

  - project1 is a project_id.
  - project1.dataset1 is a dataset_id.
  - project1.dataset1.table1 is a table_id.
  - dataset1 is a dataset_name.
  - table1 is a table_name.

* We now use a "major.minor" scheme for the version identifier.


2.0.1 (2019-12-20)
------------------

Improvement
^^^^^^^^^^^
* The data is deleted in transitional locations even if its transfer fails.

Bugfixes
^^^^^^^^
* The method `google.cloud.bigquery.job.QueryJob.result()`_ is used again
  to wait for a google job to be completed. The timeout bug described in
  the previous "bugfixes" seems to be due to a Docker configuration problem.

* The end of a step "query_to_bq" produced the log: "Ended source to bq".
  It has been corrected to "Ended query to bq".

2.0.0 (2019-12-04)
------------------

API Changes
^^^^^^^^^^^
* The parameters delete_in_bq, delete_in_gs and delete_in_local of
  of :meth:`google_pandas_load.loader.Loader.load` do not exist anymore.
  There were used to choose if data had to be deleted once loaded to the next
  location. The new behavior is now the following:

  - The data is kept in the source.
  - The data is deleted in transitional locations after being transferred.

  This behavior is safer, simpler to understand and fits to the common use.

* The destination parameter of :meth:`google_pandas_load.loader.Loader.load`
  can no longer be set to 'query' since it appeared to be useless. It is now
  restricted to ‘bq’, ‘gs’, ‘local’ or ‘dataframe’.

* The gs_dir_path_in_bucket parameter of :class:`google_pandas_load.loader.Loader`
  has been renamed gs_dir_path.

* :class:`google_pandas_load.loader.Loader` has now the following getter
  functions: bq_client, dataset_ref, bucket, gs_dir_path and local_dir_path.
  They return the homonym arguments of the class.

* :class:`google_pandas_load.loader_quick_setup.LoaderQuickSetup` has three new
  getter functions: project_id, dataset_id and bucket_name. They return the
  homonym arguments of the class.

Bugfixes
^^^^^^^^
* The method `google.cloud.bigquery.job.QueryJob.result()`_ was used to wait
  for a google job to be completed. It appeared it could lead to a timeout if
  the google job was too long to run and is threfore no longer used. Instead,
  the google job is reloaded every second until it is completed.

1.0.0 (2019-04-11)
------------------
* Initial release on PyPI.


.. _google.cloud.bigquery.job.QueryJob.result(): https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJob.html#google.cloud.bigquery.job.QueryJob.result
.. _google.cloud.bigquery.job.LoadJobConfig.autodetect: https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html#google.cloud.bigquery.job.LoadJobConfig



