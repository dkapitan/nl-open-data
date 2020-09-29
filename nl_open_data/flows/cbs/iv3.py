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
import requests
from zipfile import ZipFile

from google.cloud import bigquery

import pandas as pd
import prefect
from prefect import task, Parameter, Flow, unmapped
from prefect.tasks.shell import ShellTask
from prefect.engine.executors import DaskExecutor
from prefect.engine.state import Paused
from prefect.engine.results import PrefectResult
from prefect.triggers import all_successful

from nimbletl.tasks import curl_cmd, cbsodatav3_to_gbq#, cbsodatav3_to_gcs, gcs_to_bq
from nimbletl.utilities import clean_python_name
from nl_open_data.config import get_config


ODATA_IV3 = [
    # Gemeenten onbewerkte IV3-Data
    "45050NED",  # 2020
    "45046NED",  # 2019
    # "45042NED",  # 2018
    # "45038NED",  # 2017
    # "45031NED",  # 2016
    # "45006NED",  # 2015
    # "45005NED",  # 2014
    # "45004NED",  # 2013
    # "45001NED",  # 2012
    # "45008NED",  # 2011
    # "45007NED"   # 2010
]


gcp = Parameter("gcp", required=True)
dirs = Parameter("dirs", required=True)


with Flow("Gemeente IV3 Data") as flow:
    iv3_parquet = cbsodatav3_to_gcs.map(id=ODATA_IV3, schema=unmapped("iv3"), third_party=unmapped(True), GCP=unmapped(gcp), paths=unmapped(dirs), task_args={'skip_on_upstream_skip': False})
    iv3_bq = gcs_to_bq.map(prefect_output=iv3_parquet, GCP=unmapped(gcp))
    iv3_column_description = column_descriptions.map(table_id=ODATA_IV3, third_party=unmapped(True), schema_bq=unmapped("iv3"), GCP=unmapped(gcp), upstream_tasks=[iv3_bq])
    

def main(config):
    """Executes cbs.regionaal.flow in DaskExecutor.
    """

    """ Trigger in Prefect, load column description first and when finished only then load the data.
    """
    flow.set_reference_tasks([iv3_column_description])

    # executor = DaskExecutor(n_workers=8)
    flow.run(
        # executor=executor,
        parameters={
            "gcp": config.gcp,
            "dirs": config.path,
        },
    )


if __name__ == "__main__":
    config = get_config("dataverbinders")
    main(config=config)
