from pathlib import Path
import requests

from google.cloud import bigquery
import pandas as pd
from prefect import task, Parameter, Flow
from prefect.utilities.tasks import unmapped

from nl_open_data.config import get_config


CATALOGS = {
    "catalog_cbs_odatav3": "https://opendata.cbs.nl/ODataCatalog/Tables?$format=json",
    "catalog_dataderden_odatav3": "https://dataderden.cbs.nl/ODataCatalog/Tables?$format=json",
    "catalog_cbs_odatav4": "https://odata4.cbs.nl/CBS/Datasets",
}


@task(name="CBS ODATA catalogs")
def odatav3_catalog_to_gbq(catalog=None, schema="cbs", GCP=None):
    """Loads catalogs of CBS odata v3 and v4.

    Args:
        - catalog (str, str): tuple with name of table and url
    """
    bq = bigquery.Client(project=GCP.project)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    df = pd.DataFrame(requests.get(catalog[1]).json()["value"])
    job = bq.load_table_from_dataframe(
        dataframe=df,
        destination=f"{schema}.{catalog[0]}",
        project=GCP.project,
        location=GCP.location,
    )
    return job


gcp = Parameter("gcp", required=True)
with Flow("CBS catalogs") as flow:
    odatav3 = odatav3_catalog_to_gbq.map(catalog=list(CATALOGS.items()), GCP=unmapped(gcp))


def main(config):
    """Executes vektis.agb.flow in DaskExecutor.
    """
    flow.run(parameters={"gcp": config.gcp})


if __name__ == "__main__":
    config = get_config("dataverbinders")
    main(config=config)
