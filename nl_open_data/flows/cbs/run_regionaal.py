"""Use statline-bq flow to upload the following datasets to GBQ:

TODO: Add docstring

[^kwb]: https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2019
[^regios]: https://opendata.cbs.nl/statline/portal.html?_catalog=CBS&_la=nl&tableId=84721NED&_theme=232

"""
from nl_open_data.config import config
from prefect import Client

VERSION_GROUP_ID = "statline_bq"

TENANT_SLUG = "dataverbinders"
ODATA_REGIONAAL = [
    # Regionale kerncijfers Nederland
    "70072NED"
    # Kerncijfers wijken en buurten
    "84583NED",  # 2019
    "84286NED",  # 2018
    "83765NED",  # 2017
    "83487NED",  # 2016
    "83220NED",  # 2015
    "82931NED",  # 2014
    "82339NED",  # 2013
    # Regionale indelingen
    "84721NED",
    # Grote bevolkingstabel per pc4-leeftijd-geslacht vanaf 1999
    "83502NED",
    # inkomensverdeling
    "84639NED",
    # Gebruik Voorzieningen Sociaal Domein; Wijken
    "83265NED",  # 2015
    "83619NED",  # 2016
    "83817NED",  # 2017
    "84420NED",  # 2018
    "84662NED",  # 2019
]
SOURCE = "cbs"  # TODO: VERIFY SOURCE??
THIRD_PARTY = False
GCP_ENV = "dev"
FORCE = False

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token
parameters = {
    "ids": ODATA_REGIONAAL,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
}
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, parameters=parameters
)
