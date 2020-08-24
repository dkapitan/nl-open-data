from pathlib import Path
import requests
from zipfile import ZipFile


import prefect
from prefect import task, Parameter, Flow
from prefect.tasks.gcp import BigQueryTask
from prefect.engine.results import LocalResult, PrefectResult

from nl_open_data.config import get_config

# Loading 'txe' configuration.
CONFIG = get_config("txe")

# GCP configurations
GCP = CONFIG.gcp


# Dict of column_name and alias.
mlz_select_dict = {
    "RegioS.Key":"gemeente_code",
    "RegioS.Title":"gemeente_naam",
    "Geslacht.Title":"geslacht",
    "Leeftijd.Title":"leeftijd",
    "Huishouden.Key":"huishouden_code",
    "Type.Title":"huishouden_omschrijving",
    "LeveringsvormZorg.Title":"leveringsvorm",
    "fct.PersonenMetGebruikInJaar_1":"gebruik_aantal_personen",
    "fct.PersonenMetGebruikInJaar_2":"gebruik_percentage"
}


# Dict of column_name and alias.
mlz_joins_dict = {
    "Perioden":"Perioden",
    "Geslacht":"Geslacht",
    "Leeftijd":"Leeftijd",
    "Huishouden":"Huishouden",
    "TypeMaatwerkvoorziening":"Type",
    "LeveringsvormZorg":"LeveringsvormZorg",
    "RegioS":"RegioS"
}


# List of Conditions
mlz_cond_list = (
    "fct.Perioden Like '%JJ%'",
    "fct.Geslacht != 'GM%'",
    "fct.Leeftijd NOT IN ('20300', '90120', '90200')",
    "fct.Huishouden != 'T001139'",
    "fct.TypeMaatwerkvoorziening != 'T001024'",
    "fct.LeveringsvormZorg != 'T001306'"
)


# Dict of column_name and alias.
cbs_select_dict = {
    "regio.Key":"gemeente_code",
    "regio.Title":"gemeente_naam",
    "kvh.Key":"huishuiden_code",
    "SUBSTR(kvh.Title, 7)":"huishouden_omschrijving",
    "fct.ParticuliereHuishoudens_1":"particuliere_huishoudens_aantal",
    "fct.ParticuliereHuishoudensRelatief_2":"particuliere_huishoudens_relatief",
    "fct.GemiddeldGestandaardiseerdInkomen_3":"inkomen_gestandaardiseerd_gemiddeld",
    "fct.MediaanGestandaardiseerdInkomen_4":"inkomen_gestandaardiseerd_mediaan",
    "fct.GemiddeldBesteedbaarInkomen_5":"inkomen_besteedbaar_gemiddeld",
    "fct.MediaanBesteedbaarInkomen_6":"inkomen_besteedbaar_mediaan",
    "fct.GestandaardiseerdInkomen1e10Groep_7":"inkomensgroep_deciel_1",
    "fct.GestandaardiseerdInkomen2e10Groep_8":"inkomensgroep_deciel_2",
    "fct.GestandaardiseerdInkomen3e10Groep_9":"inkomensgroep_deciel_3",
    "fct.GestandaardiseerdInkomen4e10Groep_10":"inkomensgroep_deciel_4",
    "fct.GestandaardiseerdInkomen5e10Groep_11":"inkomensgroep_deciel_5",
    "fct.GestandaardiseerdInkomen6e10Groep_12":"inkomensgroep_deciel_6",
    "fct.GestandaardiseerdInkomen7e10Groep_13":"inkomensgroep_deciel_7",
    "fct.GestandaardiseerdInkomen8e10Groep_14":"inkomensgroep_deciel_8",
    "fct.GestandaardiseerdInkomen9e10Groep_15":"inkomensgroep_deciel_9",
    "fct.GestandaardiseerdInkomen10e10Groep_16":"inkomensgroep_deciel_10"
}


# Dict of table_name and alias.
cbs_joins_dict = {
    "Regio":"regio",
    "Populatie":"pop",
    "KenmerkenVanHuishoudens":"kvh"
}


# List of Conditions
cbs_cond_list = (
    "fct.Perioden LIKE '%JJ%'",
    "fct.Regio LIKE 'GM%'",
    "fct.Populatie = '1050010'",
    "kvh.Title LIKE 'Type:%'",
    "fct.KenmerkenVanHuishoudens != '1050055'",
)


# TODO: Find a workaround to returm GCP.project and GCP.location based on Prefect parameters.
# @task(result=PrefectResult())
# def bq_project(var):
#     return var


# @task(result=PrefectResult())
# def bq_location(var):
#     return var


def qry_select_string(select_tables):
    """Create select query as String.

    Args:
        - select_tables (dict): Dictionary of the columns (key) and aliases (values) which need to be selected.
    
    Returns:
        - String of the SELECT Query.
    """
    qry_select = ("SELECT "\
    "CAST(substr(fct.Perioden, 1, 4) AS INT64) AS jaar")

    for table, alias in select_tables.items():
        qry_select += (f", {table} AS {alias}")
    
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
def query_generator(GCP, from_schema, new_table, selected_tables, dict_select, dict_join, list_cond, output_schema="dso"):
    """Create complete SQL query which could be used for BigQueryTask.

    Args:
        - GCP (dataclass): configuration object with `project` and `location` attributes
        - from_schema (str): Schema from which the tables are selected.
        - new_table (str): Name of the new table.
        - selected_tables (str): The primary table from which columns are selected for the query.
        - dict_select (dict): Names and aliases of the columns which are selected for the query.
        - dict_join (dict): Column names and aliases needed for the join statements.
        - list_cond (list): The conditions for the query.
        - output_schema: Name of the schema to which the new table will loaded.
    
    Returns:
        - Complete query statement.
    """

    create_statement = (f"CREATE OR REPLACE TABLE `{GCP.project}.{output_schema}.{new_table}` "\
    "PARTITION BY RANGE_BUCKET(jaar, GENERATE_ARRAY(2011, 2020, 1)) AS (")
    
    select_statement = qry_select_string(dict_select)
    
    from_statement = qry_join_string(dict_join, GCP.project, from_schema, selected_tables)

    where_statement = qry_where_string(list_cond)
    
    qry = create_statement + select_statement + from_statement + where_statement
    
    print(qry)

    return qry


with Flow("BigQuery Task Tester") as flow:
    # Defining Parameter gcp, to retrieve GCP-project and GCP-location
    gcp = Parameter("gcp", required=True)
    # gcp_project = Parameter("project", required=True)
    # gcp_location = Parameter("location", required=True)

    # gcp_project = bq_project(var=project)
    # gcp_location = bq_location(var=location)

    # Creating BigQueryTask which will execute a query in BigQuery, have to provide the paramters only once and can be re-used.
    bq_task = BigQueryTask(name="bq_task_query", project=GCP.project, location=GCP.location)
    # bq_task = BigQueryTask(name="bq_task_query", project=gcp_project, location=gcp_location)

    # Creating the different queries as String.
    # test_qry = test_query(GCP=gcp, new_table="pickle_rick", selected_table="82339NED_TypedDataSet")
    # mlz_qry = query_generator(GCP=gcp, from_schema="mlz" new_table_name="pickle_rick", selected_tables="40060NED_TypedDataSet", dict_select=mlz_select_dict , dict_join=mlz_joins_dict, list_cond=mlz_cond_list)
    cbs_qry = query_generator(GCP=gcp, from_schema="cbs", new_table="pickle_rick_2", selected_tables="84639NED_TypedDataSet", dict_select=cbs_select_dict , dict_join=cbs_joins_dict, list_cond=cbs_cond_list)
    
    # Assigning the queries to the BigQueryTask Task.
    # bq_test_query = bq_task(query=test_qry)
    # bq_mlz_query = bq_task(query=mlz_qry)
    bq_cbs_query = bq_task(query=cbs_qry)
    
    


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
    