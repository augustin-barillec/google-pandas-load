.. :changelog:

History
=======

2.0.0 (2019-11-29)
------------------

API Changes
^^^^^^^^^^^
* The parameters delete_in_bq, delete_in_gs and delete_in_local of the load
  methods do not exist anymore. The fixed behavior is now the following:

  - The data is not deleted in the source during the execution of a load job.
  - The data is not kept in transitional locations.

  This fixed behavior is safer, simpler and fits the general usecases.

* The destination parameter of  :meth:`google_pandas_load.loader.Loader.load`
  cannot be set to 'query' anymore. It is restricted to 'bq', 'gs', 'local'
  or 'dataframe'. The option to set the destination parameter to 'query'
  was useless.

* The gs_dir_path_in_bucket parameter of :class:`google_pandas_load.loader.Loader`
  has been renamed gs_dir_path. It is shorter and equally understandable.

* :meth:`google_pandas_load.loader.Loader.load` has now properties: bq_client,
  dataset_ref, bucket, gs_dir_path and local_dir_path. They return the
  homonym arguments of the class.

* :meth:`google_pandas_load.loader.LoaderQuickSetup.load` has three new
  properties: project_id, dataset_id and bucket_name. They return the
  homonym arguments of the class.

* Deleted the renaming of the modules of the three main classes (Loader,
  LoaderQuickSetup and LoadConfig) in the __init__.py. This renaming
  shortened the references to these classes in the documentation but
  it also made them less explicit.

Bugfixes
^^^^^^^^
* The method `google.cloud.bigquery.job.QueryJob.result()`_ was used to wait
  for a google job to be completed. It is not used anymore because it could lead
  to a timeout if the google job was too long. Instead, the google job is
  reloaded each second until is is finished.


1.0.0 (2019-04-11)
------------------
* Initial release on PyPI.


.. _google.cloud.bigquery.job.QueryJob.result(): https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJob.html#google.cloud.bigquery.job.QueryJob.result