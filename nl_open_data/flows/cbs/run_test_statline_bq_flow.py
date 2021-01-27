from nl_open_data.config import config
from prefect import Client

# Schedule flow-run on prefect cloud

STATLINE_VERSION_GROUP_ID = "statline_bq"
TENANT_SLUG = "dataverbinders"
DATA = ["83583NED"]
SOURCE = "cbs"
THIRD_PARTY = False
GCP_ENV = "dev"
FORCE = False

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

statline_parameters = {
    "ids": DATA,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
}
flow_run_id = client.create_flow_run(
    version_group_id=STATLINE_VERSION_GROUP_ID, parameters=statline_parameters
)
