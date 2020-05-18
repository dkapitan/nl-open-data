"""Dataflow for regionaal data from Statistics Netherlands (Centraal Bureau voor Statistiek, CBS).

Loads the following CBS datasets into BigQuery:
    - Mapping of all postal code + housenumber to neighbourhood, district and municipalities,
        see `Buurt, wijk en gemeente voor postcode-huisnummer (2019) <https://www.cbs.nl/nl-nl/maatwerk/2019/42/buurt-wijk-en-gemeente-2019-voor-postcode-huisnummer, wijk and gemeente>`_
    - `Kerncijfers wijken en buurten <https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2019>`_
    - `Regionale indelingen <https://opendata.cbs.nl/statline/portal.html?_catalog=CBS&_la=nl&tableId=84721NED&_theme=232>`_
    - `Regionale kerncijfers uit ruim 50 CBS-statistieken. <https://opendata.cbs.nl/statline/portal.html?_la=nl&_catalog=CBS&tableId=70072ned&_theme=230>`_ 
        Uitgesplitst naar vier regionale niveaus van landsdeel tot gemeente.
    - `Gezondheid per wijk en buurt 2016 <https://statline.rivm.nl/#/RIVM/nl/dataset/50052NED/table?ts=1589622516137>`_
    

"""

from pathlib import Path
from zipfile import ZipFile

import cbsodata
import pandas as pd
import pandas_gbq
import prefect
from prefect import task, Parameter, Flow
from prefect.tasks.shell import ShellTask
from prefect.utilities.tasks import unmapped
from prefect.engine.executors import DaskExecutor

from nimbletl.tasks import curl_cmd, cbsodata_to_gbq, excel_to_gbq, unzip
from nimbletl.utilities import clean_python_name
from nl_open_data.config import get_config


URL_PC6HUISNR = (
    "https://www.cbs.nl/-/media/_excel/2019/42/2019-cbs-pc6huisnr20190801_buurt.zip"
)

ODATA_KWB = {
    2019: "84583NED",
    2018: "84286NED",
    2017: "83765NED",
    2016: "83487NED",
    2015: "83220NED",
    2014: "82931NED",
    2013: "82339NED",
}

ODATA_RIVM: {
    2016: "50052NED",
}


@task(skip_on_upstream_skip=False)
def pc6huisnr_to_gbq(zipfile=None, credentials=None, GCP=None):
    """
    Loads CBS for mapping each address to buurt, wijk and gemeente from 2019.

    Args:
        - zipfile (str): path to downloaded zipfile
        - credentials (Google credentials): GCP credentials
        - GCP (GCP config dataclass)

    Returns:
        None

    Source: https://www.cbs.nl/nl-nl/maatwerk/2019/42/buurt-wijk-en-gemeente-2019-voor-postcode-huisnummer
    """
    with ZipFile(zipfile) as zipfile:
        data = {
            file.split(".")[0]: pd.read_csv(zipfile.open(file), delimiter=";",).rename(
                columns=clean_python_name
            )
            for file in zipfile.namelist()
        }

    for k, v in data.items():
        pandas_gbq.to_gbq(
            v,
            ".".join(["cbs", k]),
            project_id=GCP.project,
            credentials=credentials,
            if_exists="replace",
            location=GCP.location,
        )


@task(skip_on_upstream_skip=False)
def kwb_to_gbq(
    destination_table="cbs.kerncijfers_wijken_buurten", credentials=None, GCP=None
):
    """Loads kerncijfers wijken- en buurten into `cbs.kerncijfers_wijken_buurten`.
    
    Column names are converted to `clean_python_name`. Existing tables are replaced.

    Args:
        - destination_table (str): name of destination table in BigQuery in format `dataset.tablename`
        - credentials (google.auth.credentials.Credentials): credentials for project and BigQuery
        - GCP (dataclass): configuration object with `project` and `location` attributes

    Returns:
        None
  
  """
    # TODO: add kwarg jaar to add verslagjaar as partition or column to allow different versions
    dfs = [
        pd.DataFrame(cbsodata.get_data(identifier))
        .rename(columns=clean_python_name)
        .assign(jaar=jaar)
        for jaar, identifier in ODATA_KWB.items()
    ]
    pd.concat(dfs).to_gbq(
        destination_table,
        project_id=GCP.project,
        credentials=credentials,
        if_exists="replace",
        location=GCP.location,
    )


gcp = Parameter("gcp", required=True)
filepath = Parameter("filepath", required=True)
curl_download = ShellTask(name="curl_download")
regio = task(cbsodata_to_gbq, name="84721NED")
task_excel = task(excel_to_gbq)
task_unzip = task(unzip)


with Flow("CBS regionaal") as flow:
    # TODO: fix UnicodeDecodeError when writing to Google Drive
    # curl_command = curl_cmd(URL_PC6HUISNR, filepath)
    # curl_download = curl_download(command=curl_command)
    # gwb = pc6huisnr_to_gbq(zipfile=filepath, GCP=gcp, upstream_tasks=[curl_download])
    # regio = regio(
    #     identifier="84721NED", destination_table="cbs.regionale_indeling2020", GCP=gcp
    # )
    kwb = kwb_to_gbq(GCP=gcp)


def main(config):
    """Executes cbs.regionaal.flow in DaskExecutor.
    """

    executor = DaskExecutor(n_workers=8)
    flow.run(
        executor=executor,
        parameters={
            "gcp": config.gcp,
            # "filepath": config.path.root
            # / config.path.cbs
            # / URL_PC6HUISNR.split("/")[-1],
        },
    )


if __name__ == "__main__":
    config = get_config("dataverbinders")
    main(config=config)
