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
from prefect.triggers import all_successful

from nimbletl.tasks import curl_cmd, cbsodatav3_to_gbq
from nimbletl.utilities import clean_python_name
from nl_open_data.config import get_config

ODATA_RIVM = "50052NED"  # https://statline.rivm.nl/portal.html?_la=nl&_catalog=RIVM&tableId=50052NED&_theme=72

gcp = Parameter("gcp", required=True)


with Flow("CBS regionaal") as flow:
    # # TODO: fix UnicodeDecodeError when writing to Google Drive
    rivm = cbsodatav3_to_gbq(id=ODATA_RIVM, schema="rivm", third_party=True, GCP=gcp)
    rivm_column_description = column_descriptions(table_id=ODATA_RIVM, third_party=True, schema_bq="rivm", GCP=gcp, upstream_tasks=[rivm])


def main(config):
    """Executes cbs.regionaal.flow in DaskExecutor.
    """

    """ Trigger in Prefect, load column description first and when finished only then load the data.
    """
    flow.set_reference_tasks([rivm_column_description])

    # executor = DaskExecutor(n_workers=8)
    flow.run(
        # executor=executor,
        parameters={
            "gcp": config.gcp,
        },
    )


if __name__ == "__main__":
    config = get_config("dataverbinders")
    main(config=config)
