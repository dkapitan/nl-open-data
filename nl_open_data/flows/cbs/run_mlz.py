"""Use statline-bq flow to upload the following datasets to GBQ:

TODO: Add docstring?

[^mlz]: https://mlzopendata.cbs.nl/#/MLZ/nl/
"""
from nl_open_data.config import config
import prefect

FLOW_ID = "5c66c7e3-ba9f-4a8c-9b7e-9dc85ea158a7"  # TODO: Automatically get latest flow_id from prefect cloud (or store output from flow.register and read from there)

TENANT_SLUG = "dataverbinders"
ODATA_MLZ = ["40061NED", "40060NED"]
SOURCE = "mlz"
THIRD_PARTY = True
GCP_ENV = "dev"
FORCE = False

client = prefect.Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token
parameters = {
    "ids": ODATA_MLZ,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
}
flow_run_id = client.create_flow_run(flow_id=FLOW_ID, parameters=parameters)
