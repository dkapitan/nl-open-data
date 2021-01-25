from nl_open_data.config import config
from prefect import Flow, task, Parameter

from nl_open_data.utils import check_bq_dataset
from nl_open_data.tasks import get_gcp_credentials

check_bq_dataset = task(check_bq_dataset)

with Flow("test_secrets") as flow:
    service_account_info = Parameter("service_account_info")
    gcp_credentials = get_gcp_credentials(service_account_info)
    check_bq_dataset(
        dataset_id="83583NED", gcp=config.gcp.dev, credentials=gcp_credentials
    )

flow.register("nl_open_data", version_group_id="test_secrets")
