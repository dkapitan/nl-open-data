## nl-open-data

Datawarehouse of various Dutch open data sources with focus on healthcare and public domain. It is the overaching library orchestrating the upload of datasets from various sources into Google Big Query.


### Motivation
In order to take advantage of open data, the ability to mix various datasets together must be available. As of now, in order to to that, a substantial knowledge of programming and data engineering must be available to any who wishes to do so. This project library aims to make that task easier.


### Build status
[![Pypi Status](https://img.shields.io/pypi/v/nl-open-data.svg)](https://pypi.python.org/pypi/nl-open-data) [![Build Status](https://img.shields.io/travis/dataverbinders/nl-open-data.svg)](https://travis-ci.com/dataverbinders/nl-open-data) [![Docs Status](https://readthedocs.org/projects/nl-open-data/badge/?version=latest)](https://dataverbinders.github.io/nl-open-data)


### Installation

<!-- Use `gcloud` to initiatlize a project. To setup BigQuery run

```
>>> make bq-datasets
``` -->

### Configuration

There are two elements that need to be configured prior to using the CLI. If using as an imported library the exact usage determines wheteher these are both needed (or just one, or none)

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
Addiotionally, the local paths used by the library can configured here. Under `[paths]` define the path to the library, and other temporary folders if desired.

#### 2. Datasets through `datasets.toml`

Provide a list of all CBS dataset ids that are to be uploaded to GCP. i.e.:

`ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]`

This should be given by directly editing `nl-open-data/nl_open_data/datasets.toml`

### Credits

<!-- This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.


.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage -->
