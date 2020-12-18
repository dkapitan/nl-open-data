## nl-open-data

A Flexible Python ETL toolkit for datawarehousing framework based on Dask, Prefect and the pydata stack. It follows the [original design principles](https://stories.dask.org/en/latest/prefect-workflows.html) from these libraries, combined with a [functional programming approach to data engineering](https://medium.com/@maximebeauchemin/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a).

[Google Cloud Platform (GCP)](https://cloud.google.com/docs/) is used as the core infrastructure, particularly [BigQuery (GBQ)](https://cloud.google.com/bigquery/docs/) and [Cloud Storage (GCS)](https://cloud.google.com/storage/) as the main storage engines. We follow Google's recommendations on how to use [BigQuery for data warehouse applications](https://cloud.google.com/solutions/bigquery-data-warehouse) with four layers:

- source data, in production environment or file-based
- staging, on GCS
- datavaault, on GBQ
- datamarts, on GBQ using [ARRAY_AGG, STRUCT, UNNEST SQL-pattern](https://medium.freecodecamp.org/exploring-a-powerful-sql-pattern-array-agg-struct-and-unnest-b7dcc6263e36)


### Motivation
In order to take advantage of open data, the ability to mix various datasets together must be available. As of now, in order to to that, a substantial knowledge of programming and data engineering must be available to any who wishes to do so. This project library aims to make that task easier.


### Build status
[![Pypi Status](https://img.shields.io/pypi/v/nl-open-data.svg)](https://pypi.python.org/pypi/nl-open-data) [![Build Status](https://img.shields.io/travis/dataverbinders/nl-open-data.svg)](https://travis-ci.com/dataverbinders/nl-open-data) [![Docs Status](https://readthedocs.org/projects/nl-open-data/badge/?version=latest)](https://dataverbinders.github.io/nl-open-data)


### Installation

Using pip:
    `pip install nl_open_data` -> **NOT IMPLEMENTED YET**

Using Poetry:
    Being a [Poetry](https://python-poetry.org/) managed package, installing via Poetry is also possible. Assuming Poetry is already installed:
    
1. Clone the repository
2. From your local clone's root folder, run `poetry install`
<!-- Use `gcloud` to initiatlize a project. To setup BigQuery run

```
>>> make bq-datasets
``` -->

### Configuration

There are two elements that need to be configured prior to using the library.

#### 1. GCP and Paths through config.toml

The GCP project id, bucket, and location should be given by editing `nl-open-data/nl_open_data/config.toml`, allowing up to 3 choices at runtime: `dev`, `test` and `prod`. Note that you must nest gcp projects details correctly for them to be interperted, as seen below. You must have the proper IAM (permissions) on the GCP projects (more details below).

Correct nesting in config file:
```
[gcp]
    [gcp.prod]
    project_id = "my_dev_project_id"
    bucket = "my_dev_bucket"
    location = "EU"

    [gcp.test]
    project_id = "my_test_project_id"
    bucket = "my_test_bucket"
    location = "EU"

    [gcp.prod]
    project_id = "my_prod_project_id"
    bucket = "my_prod_bucket"
    location = "EU"
```
Additionally, the local paths used by the library can configured here. Under `[paths]`, define the path to the library, and other temporary folders.


### Credits

<!-- This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.


.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage -->
