============
nl-open-data
============


.. image:: https://img.shields.io/pypi/v/nl_open_data.svg
        :target: https://pypi.python.org/pypi/nl_open_data

.. image:: https://img.shields.io/travis/dkapitan/nl_open_data.svg
        :target: https://travis-ci.com/dkapitan/nl_open_data

.. image:: https://readthedocs.org/projects/nl-open-data/badge/?version=latest
        :target: https://nl-open-data.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




Datawarehouse of various Dutch open data sources with focus on healthcare and public domain.


* Free software: MIT license
* Documentation: https://dkapitan.github.io/nl-open-data/nl_open_data


Introduction
------------

Flexible Python ETL toolkit for datawarehousing framework based on Dask, Prefect and the pydata stack. It follows the [original design principles from these libraries](https://stories.dask.org/en/latest/prefect-workflows.html), combined with a [fuctional programming data engineering approach](https://medium.com/@maximebeauchemin/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a).

[Google Cloud Platform (GCP)](https://cloud.google.com/docs/) is used as the core infrastructure, particularly [Google BigQuery (GBQ)](https://cloud.google.com/bigquery/docs/) and [Google Cloud Storage](https://cloud.google.com/storage/) as the main storage engines. We follow Google's recommendations on how to [use BigQuery for data warehouse applications](https://cloud.google.com/solutions/bigquery-data-warehouse) with four layers:

    * source data, in production environment or file-based
    * staging, on GCS
    * datavaault, on GBQ
    * datamarts, on GBQ using [ARRAY_AGG, STRUCT, UNNEST SQL-pattern](https://medium.freecodecamp.org/exploring-a-powerful-sql-pattern-array-agg-struct-and-unnest-b7dcc6263e36)

*nimble (/ˈnɪmb(ə)l/): quick and light in movement or action; agile*, wink at the godfather of the star-schema, [Kimball](https://en.wikipedia.org/wiki/Ralph_Kimball)


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
