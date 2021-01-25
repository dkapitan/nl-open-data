"""Use statline-bq flow to upload the following datasets to GBQ:

TODO: Add docstring?

[^rivm]: https://statline.rivm.nl/#/RIVM/nl/dataset/50052NED/table?ts=1589622516137
"""
# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from prefect import Client
from prefect.client import Secret

VERSION_GROUP_ID = "statline_bq"

TENANT_SLUG = "dataverbinders"
ODATA_RIVM = [
    "50052NED"
]  # https://statline.rivm.nl/portal.html?_la=nl&_catalog=RIVM&tableId=50052NED&_theme=72SOURCE = "mlz"
SOURCE = "rivm"  # TODO: VERIFY SOURCE??
THIRD_PARTY = True
GCP_ENV = "dev"
FORCE = False
SERVICE_ACCOUNT_INFO = Secret("GCP_CREDENTIALS").get()

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token
parameters = {
    "ids": ODATA_RIVM,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
    "service_account_info": SERVICE_ACCOUNT_INFO,
}
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, parameters=parameters
)
