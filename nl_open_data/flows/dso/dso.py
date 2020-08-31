from pathlib import Path
import requests
from zipfile import ZipFile


import prefect
from prefect import task, Parameter, Flow
from prefect.tasks.gcp import BigQueryTask
from prefect.engine.results import LocalResult, PrefectResult

from google.cloud.bigquery.table import Row
import re

from nl_open_data.config import get_config


def qry_select_string(query_result):
    """Create select query as String.

    Args:
        - query_result (list): List with result of the query.
    
    Returns:
        - String of the SELECT Query.
    """

    # define search string
    reg1 = re.compile("(RegioS?)|(Huishouden)")


    qry_select = f"""SELECT
    CAST(substr(fct.Perioden, 1, 4) AS INT64) AS jaar
    """

    for i in query_result:
        if reg1.match(i.get('key')):
            qry_select += f""", {i.get('key')}.Key
    , {i.get('key')}.Title
    """
        else:
            qry_select += f""", {i.get('key')}
    """
    
    return qry_select


def qry_join_string(join_tables, gcp_project, schema, primary_table):
    """Create From and possible Join part of the query.

    Args:
        - join_tables (dict): Table names (key) and their aliases (values) needed for the join statements.
        - gcp_project (str): Project in GCP of the tables.
        - schema (str): Schema from which the tables are selected.
        - primary_table (str): The primary table which will join other tables.
    
    Returns:
        - String of the From/Join Query.
    """

    from_join = f" FROM {gcp_project}.{schema}.{primary_table} AS fct"

    # for table, fk in join_tables.items():
    #     from_join += f" INNER JOIN {gcp_project}.{schema}.{primary_table[:9]}{table} AS {table} ON {fk} = fct.{table}"

    # TODO: Key is altijd aanwezig in de query, dus in de dict alleen Tabel Naam en Alias.
    if join_tables:
        for table, fk in join_tables.items():
            from_join += f" INNER JOIN {gcp_project}.{schema}.{primary_table[:9]}{table} AS {fk} ON {fk}.key = fct.{table}"
    
    return from_join


def qry_where_string(table_conditions):
    """Create conditions for the query, the where-part.

    Args:
        - table_conditions (list): The condition for the query as String.
    
    Returns:
        - String of the WHERE Query.
    """

    if table_conditions:
        qry_cond = f" WHERE {table_conditions[0]}"

        for condition in table_conditions[1:]:
            qry_cond += f" AND {condition}"
    
    else:
        qry_cond = ""
    
    return qry_cond + ")"


# Test function
@task
def test_query(GCP, new_table, selected_table, schema="cbs"):
    query_1 = (f"CREATE OR REPLACE TABLE `{GCP.project}.{schema}.{new_table}` "\
    "AS ("\
    "Select * "\
    f"FROM {GCP.project}.{schema}.{selected_table} "\
    f"WHERE ID < 10) ")

    print(query_1)

    return query_1


@task
def get_table_names(GCP, from_schema, table, output_schema="dso"):
    qry_data_properties = f"""SELECT
    type
    , key
    , title
    FROM `dataverbinders.{from_schema}.{table}`
    WHERE type like '%Dimension'
    """

    # print(qry_data_properties)

    bq_task = BigQueryTask(query=qry_data_properties, name="bq_task_query", project=GCP.project, location=GCP.location)
    qry_result = bq_task.run()
    
    print(type(qry_result))
    print(type(qry_result[0]))

    return qry_result


@task
def query_generator(GCP, query_result, schema="mlz"):
    table = "40060NED_DataProperties"

    pre_table = table.split("_")[0]

    test_list = [Row(('Dimension', 'Geslacht', 'Geslacht'), {'type': 0, 'key': 1, 'title': 2}),
    Row(('Dimension', 'Leeftijd', 'Leeftijd'), {'type': 0, 'key': 1, 'title': 2}),
    Row(('Dimension', 'Huishouden', 'Huishouden'), {'type': 0, 'key': 1, 'title': 2}),
    Row(('Dimension', 'TypeMaatwerkvoorziening', 'Type maatwerkvoorziening'), {'type': 0, 'key': 1, 'title': 2}),
    Row(('Dimension', 'LeveringsvormZorg', 'Leveringsvorm zorg'), {'type': 0, 'key': 1, 'title': 2}),
    Row(('GeoDimension', 'RegioS', "Regio's"), {'type': 0, 'key': 1, 'title': 2}),
    Row(('TimeDimension', 'Perioden', 'Perioden'), {'type': 0, 'key': 1, 'title': 2})]

    """ for i in test_list:
        print("Key:", i.get('key'))
        print("Title:", i.get('title')) """
    
    qry_select = qry_select_string(test_list)

    qry_from = f"""FROM `dataverbinders.{schema}.{pre_table}_TypedDataSet` AS fct
    """

    qry_where = """WHERE"""

    for i in test_list:
        qry_from += f"""INNER JOIN `dataverbinders.{schema}.{pre_table}_{i.get('key')}` AS {i.get('key')} ON {i.get('key')}.key = fct.{i.get('key')}
    """

    test = qry_select + qry_from

    print(test)


with Flow("BigQuery Task Tester") as flow:
    # Defining Parameter gcp, to retrieve GCP-project and GCP-location
    gcp = Parameter("gcp", required=True)

    # Creating BigQueryTask which will execute a query in BigQuery, have to provide the paramters only once and can be re-used.
    # bq_task = BigQueryTask(name="bq_task_query", project=GCP.project, location=GCP.location)

    
    # cbs_qry = get_table_names(GCP=gcp, from_schema="cbs", table="84639NED_DataProperties")
    # mlz_qry = get_table_names(GCP=gcp, from_schema="mlz", table="40060NED_DataProperties")
    
    test = query_generator(GCP=gcp, query_result="test")
    
    


def main(config):
    # TODO: Delete when creating pull-request, is only for testing.
    print("Pickle Rick!!\n\n")

    flow.run(
        parameters={
            "gcp": config.gcp,
            # "project": config.gcp.project,
            # "location": config.gcp.location,
        },
        
    )
    

if __name__ == "__main__":
    config = get_config("txe")
    main(config=config)
    