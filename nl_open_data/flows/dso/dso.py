from pathlib import Path
import requests
from zipfile import ZipFile


import prefect
from prefect import task, Parameter, Flow
from prefect.tasks.gcp import BigQueryTask
from prefect.engine.results import LocalResult, PrefectResult

import re

from nl_open_data.config import get_config


def qry_select_string(query_result):
    """Create select query as String.

    Args:
        - query_result (list): List with result of get_table_names query.
    
    Returns:
        - String of the SELECT Query.
    """

    # define search string
    reg1 = re.compile("(RegioS?)|(Huishouden)")

    # Base SELECT part of query.
    qry_select = f"""SELECT
    CAST(substr(fct.Perioden, 1, 4) AS INT64) AS jaar
    """

    # Add tables to SELECT statement based on query_result.
    for i in query_result:
        if reg1.match(i.get('key')):
            qry_select += f""", {i.get('key')}.Key
    , {i.get('key')}.Title
    """
        else:
            qry_select += f""", {i.get('key')}
    """
    
    return qry_select


def qry_where_string(tables, is_period_present):
    """Create conditions for the query, the where-part.

    Args:
        - tables (list): List with result of get_table_names query.
        - is_period_present (Boolean): Boolean to check whether the table Period is present.
    
    Returns:
        - String of the WHERE Query.
    """

    # Base WHERE part of query.
    qry_where = """WHERE
    """

    # Add WHERE conditions based on tables.
    for i in tables:
        if "Perioden" in i.get("key"):
            qry_where += f"""fct.{i.get("key")} LIKE '%JJ%'
    """
        elif "Regio" in i.get("key") and is_period_present:
            qry_where += f"""AND fct.{i.get("key")} LIKE 'GM%'
    """
        elif "Regio" in i.get("key") and not is_period_present:
            qry_where += f"""fct.{i.get("key")} LIKE 'GM%'
    """
        elif "Geslacht" in i.get("key"):
            qry_where += f"""AND fct.{i.get("key")} != 'T001038'
    """
        elif "Huishouden" in i.get("key"):
            qry_where += f"""AND fct.{i.get("key")} != 'T001139'
    """
        elif "KenmerkenVanHuishoudens" in i.get("key"):
            qry_where += f"""AND fct.{i.get("key")} != '1050055'
    """
        elif "Leeftijd" in i.get("key"):
            qry_where += f"""AND fct.{i.get("key")} NOT IN ('20300', '90120', '90200')
    """
        elif "LeveringsvormZorg" in i.get("key"):
            qry_where += f"""AND fct.{i.get("key")} != 'T001306'
    """
        elif "Populatie" in i.get("key"):
            qry_where += f"""AND fct.{i.get("key")} = '1050010'
    """
        elif "TypeMaatwerkvoorziening" in i.get("key"):
            qry_where += f"""AND fct.{i.get("key")} != 'T001024'
    """
        else:
            qry_where += f"""fct.{i.get("key")} LIKE 'GM%'
    """
    
    return qry_where


@task(name="Table Names", result=PrefectResult())
def get_table_names(GCP, from_schema, table):
    """Get names of the Dimension tables.

    Args:
        - GCP: config object.
        - schema (str): schema to retreive data from.
        - table (str): table to query on
        
    Returns:
        - List with result of the query as google.cloud.bigquery.table.Row and Table Name.
    """
    
    qry_data_properties = f"""SELECT
    type
    , key
    , title
    FROM `dataverbinders-dev.{from_schema}.{table}`
    WHERE type like '%Dimension'
    ORDER BY type DESC
    """

    bq_task = BigQueryTask(query=qry_data_properties, name="bq_task_query", project=GCP.project, location=GCP.location)
    qry_result = bq_task.run()

    return qry_result, table


@task(name="Query Generator")
def query_generator(GCP, query_result, schema):
    """Generate a query based on Dimension Tables.

    Args:
        - GCP: config object.
        - query_result (tuple): list with dimension tables and the name of the table.
        - schema (str): schema where tables are located.
        
    Returns:
        - String with generated query.
    """

    is_period = False
    pre_table = query_result[1].split("_")[0]

    # SELECT part of the query.
    qry_select = qry_select_string(query_result[0])

    # FROM/JOIN part of the query.
    qry_from = f"""FROM `dataverbinders.{schema}.{pre_table}_TypedDataSet` AS fct
    """

    for i in query_result[0]:
        qry_from += f"""INNER JOIN `dataverbinders.{schema}.{pre_table}_{i.get('key')}` AS {i.get('key')} ON {i.get('key')}.key = fct.{i.get('key')}
    """
        if "Perioden" in i.get("key"):
            is_period = True

    # WHERE part of the query.
    qry_where = qry_where_string(query_result[0], is_period)

    # Create complete query.
    qry = qry_select + qry_from + qry_where

    print(qry)

    return qry


with Flow("BigQuery Task Tester") as flow:
    # Defining Parameter gcp, to retrieve GCP-project and GCP-location
    gcp = Parameter("gcp", required=True)

    cbs_result = get_table_names(GCP=gcp, from_schema="cbs", table="84639NED_DataProperties")
    mlz_result = get_table_names(GCP=gcp, from_schema="mlz", table="40060NED_DataProperties")
    
    cbs_qry = query_generator(GCP=gcp, query_result=cbs_result, schema="cbs")
    mlz_qry = query_generator(GCP=gcp, query_result=mlz_result, schema="mlz")


def main(config):
    flow.run(
        parameters={
            "gcp": config.gcp,
        },
        
    )
    

if __name__ == "__main__":
    config = get_config("txe")
    main(config=config)
    