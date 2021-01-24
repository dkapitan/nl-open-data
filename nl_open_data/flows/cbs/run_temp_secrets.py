from nl_open_data.config import config

from prefect import Client
from prefect.client import Secret

TENANT_SLUG = "dataverbinders"
JSON_ACCT_INFO = Secret("GCP_CREDENTIALS").get()

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

client.create_flow_run(
    version_group_id="test_secrets", parameters={"json_acct_info": JSON_ACCT_INFO},
)
