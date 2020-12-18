"""Dataflow for regionaal data from Statistics Netherlands (Centraal Bureau voor Statistiek, CBS).

Loads the following CBS datasets into BigQuery:

- Mapping of all postal code + housenumber to neighbourhood, district and municipalities
    (Buurt, wijk en gemeente voor postcode-huisnummer (2019)[^adres])
- Kerncijfers wijken en buurten[^kwb] 
- Regionale indelingen[^regios]
- Regionale kerncijfers uit ruim 50 CBS-statistieken[^core], uitgesplitst naar vier regionale niveaus van landsdeel tot gemeente.
- Gezondheid per wijk en buurt 2016[^rivm] 
    
[^adres]: https://www.cbs.nl/nl-nl/maatwerk/2019/42/buurt-wijk-en-gemeente-2019-voor-postcode-huisnummer
[^kwb]: https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2019
[^regios]: https://opendata.cbs.nl/statline/portal.html?_catalog=CBS&_la=nl&tableId=84721NED&_theme=232
[^core]:  https://opendata.cbs.nl/statline/portal.html?_la=nl&_catalog=CBS&tableId=70072ned&_theme=230
[^rivm]: https://statline.rivm.nl/#/RIVM/nl/dataset/50052NED/table?ts=1589622516137


"""

from pathlib import Path

# import requests
from zipfile import ZipFile


from google.cloud import bigquery
import pandas as pd

# import prefect
from prefect import task, Parameter, Flow, unmapped

from prefect.tasks.shell import ShellTask

# from prefect.engine.executors import DaskExecutor
# from prefect.triggers import all_successful

from statline_bq.tasks import cbs_odata_to_gbq, curl_cmd
from statline_bq.config import get_config

# from nl_open_data.config import get_config

URL_TABLES = "https://opendata.cbs.nl/ODataCatalog/Tables?$format=json"
URL_PC6HUISNR = (
    "https://www.cbs.nl/-/media/_excel/2019/42/2019-cbs-pc6huisnr20190801_buurt.zip"
)

ODATA_REGIONAAL = [
    # Kerncijfers wijken en buurten
    # "84583NED",  # 2019
    # "84286NED",  # 2018
    # "83765NED",  # 2017
    # "83487NED",  # 2016
    # "83220NED",  # 2015
    # "82931NED",  # 2014
    # "82339NED",  # 2013
    # Regionale indelingen
    # "84721NED",
    # Grote bevolkingstabel per pc4-leeftijd-geslacht vanaf 1999
    # "83502NED",
    # inkomensverdeling
    # "84639NED",
    # Gebruik Voorzieningen Sociaal Domein; Wijken
    "83265NED",  # 2015
    "83619NED",  # 2016
    "83817NED",  # 2017
    "84420NED",  # 2018
    "84662NED",  # 2019
]

ODATA_BEVOLKING = "03759ned"  # https://opendata.cbs.nl/statline/portal.html?_la=nl&_catalog=CBS&tableId=03759ned&_theme=259


@task(skip_on_upstream_skip=False)
def pc6huisnr_to_gbq(zipfile=None, credentials=None, GCP=None):
    """
    Loads CBS for mapping each address to buurt, wijk and gemeente from 2019.[^adres]

    Args:
        - zipfile (str): path to downloaded zipfile
        - credentials (Google credentials): GCP credentials
        - GCP (GCP config dataclass)

    Returns:
        List[google.cloud.bigquery.job.LoadJob]

    [^adres]: https://www.cbs.nl/nl-nl/maatwerk/2019/42/buurt-wijk-en-gemeente-2019-voor-postcode-huisnummer
    """
    with ZipFile(zipfile) as zipfile:
        data = {
            file.split(".")[0]: pd.read_csv(zipfile.open(file), delimiter=";",).rename(
                columns=clean_python_name
            )
            for file in zipfile.namelist()
        }

    bq = bigquery.Client(credentials=credentials, project=GCP.project)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    jobs = []
    for k, v in data.items():
        jobs.append(
            bq.load_table_from_dataframe(
                dataframe=v,
                destination=".".join(["cbs", k]),
                project=GCP.project,
                job_config=job_config,
                location=GCP.location,
            )
        )
    return jobs


config = Parameter("config", required=True)
gcp = Parameter("gcp", required=True)
filepath = Parameter("filepath", required=True)
curl_download = ShellTask(name="curl_download")


with Flow("CBS regionaal") as flow:
    # # TODO: fix UnicodeDecodeError when writing to Google Drive
    # curl_command = curl_cmd(URL_PC6HUISNR, filepath)
    # curl_download = curl_download(command=curl_command)
    # gwb = pc6huisnr_to_gbq(zipfile=filepath, GCP=gcp, upstream_tasks=[curl_download])
    regionaal = cbs_odata_to_gbq.map(
        id=ODATA_REGIONAAL,
        config=unmapped(config),
        task_args={"skip_on_upstream_skip": False},
    )
    # regionaal_column_description = column_descriptions.map(TODO: Add column descriptions to bq, probably in statline-bq
    #     table_id=ODATA_REGIONAAL, GCP=unmapped(gcp), upstream_tasks=[regionaal]
    # )


def main(config):
    """Executes cbs.regionaal.flow in DaskExecutor.
    """

    """ Trigger in Prefect, load column description first and when finished only then load the data.
    """
    # flow.set_reference_tasks([regionaal_column_description])

    # executor = DaskExecutor(n_workers=8)
    flow.run(
        # executor=executor,
        parameters={
            "config": config,
            # "filepath": Path.home()
            # / config.paths.root
            # / config.paths.cbs
            # / URL_PC6HUISNR.split("/")[-1],
            # "gcp": config.gcp.dev,
        },
    )


if __name__ == "__main__":
    config_file = Path.home() / Path("Projects/nl-open-data/nl_open_data/config.toml")
    config = get_config(config_file)
    main(config=config)
# # %%
# from statline_bq.config import get_config, Config
# from pathlib import Path

# # %%
# config_file = Path.home() / Path("Projects/nl-open-data/nl_open_data/config.toml")
# config = get_config(config_file)
# # %%
# config.paths
