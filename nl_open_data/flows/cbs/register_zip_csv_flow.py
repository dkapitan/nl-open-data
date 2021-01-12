"""A template of a Prefect Flow downloading a zipped folder with csv files.

TODO: Docstring

"""
from nl_open_data.config import config

from pathlib import Path
from prefect import Flow, unmapped, Parameter
from prefect.tasks.shell import ShellTask
from prefect.triggers import all_finished
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

# Allow skipping unzip (if folder already locally unzipped)
nlt.unzip.skip_on_upstream_skip = False
nlt.remove_dir.trigger = all_finished

curl_download = ShellTask(name="curl_download")

with Flow("zipped_csv_folder") as zip_flow:
    """[SUMMARY]

    Parameters
    ----------
    url : str
        The url of the zip file
    local_folder : str
        The local folder to use for downloading and unzipping
    csv_delimiter : str
        The delimiter used in the zipped csv files
    gcs_folder : str
        The gcs_folder to upload the table into
    gcp_env : str
        Determines which GCP configuration to use from config.gcp
    bq_dataset_name : str
        The dataset name to use when creating in BQ
    bq_dataset_description : str
        The dataset description to use when creating in BQ
    source : str
        The source of the data, used for naming and folder placements in GCS and BQ
    """

    # config = Parameter("config")
    # filepath = Parameter("filepath", required=True)
    url = Parameter("url")
    local_folder = Parameter(
        "local_folder", default=Path(__file__).parent / config.paths.temp
    )
    csv_delimiter = Parameter("csv_delimiter", default=".")
    gcs_folder = Parameter("gcs_folder")
    gcp_env = Parameter("gcp_env", default="dev")
    bq_dataset_name = Parameter("bq_dataset_name")
    bq_dataset_description = Parameter(
        "bq_dataset_description", default=None
    )  # TODO: implement
    source = Parameter("source", required=False)

    temp = nlt.split(url, separator="/")[-1]

    filepath = local_folder / Path(temp)

    local_dir = nlt.create_dir(local_folder)
    curl_command = nlt.curl_cmd(url, filepath, limit_retries=False)
    curl_download = curl_download(command=curl_command, upstream_tasks=[local_dir])
    unzipped_folder = nlt.unzip(filepath, upstream_tasks=[curl_download])
    csv_files = nlt.list_dir(unzipped_folder, upstream_tasks=[unzipped_folder])
    pq_files = nlt.csv_to_parquet.map(
        csv_files, delimiter=unmapped(csv_delimiter), upstream_tasks=[csv_files]
    )
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=pq_files,
        gcs_folder=unmapped(gcs_folder),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[pq_files],
    )
    tables = nlt.gcs_to_bq(
        gcs_folder=gcs_folder,
        dataset_name=bq_dataset_name,
        config=config,
        gcp_env=gcp_env,
        source=source,
        upstream_tasks=[gcs_ids],
    )
    nlt.remove_dir(local_dir, upstream_tasks=[gcs_ids])

if __name__ == "__main__":
    # # Register flow
    # zip_flow.executor = DaskExecutor()
    # flow_id = zip_flow.register(
    #     project_name="nl_open_data", version_group_id="statline_bq"
    # )
    # print(f" └── Registered on: {datetime.today()}")

    # Run locally
    URL_PC6HUISNR = (
        "https://www.cbs.nl/-/media/_excel/2019/42/2019-cbs-pc6huisnr20190801_buurt.zip"
    )

    # local_folder = Path(__file__).parent / config.paths.temp / config.paths.cbs
    # filepath = local_folder / Path(URL_PC6HUISNR.split("/")[-1])
    dataset_name = "buurt_wijk_gemeente_pc"
    gcs_folder = "cbs/" + dataset_name

    state = zip_flow.run(
        parameters={
            "filepath": filepath,
            "local_folder": local_folder,
            "url": URL_PC6HUISNR,
            "csv_delimiter": ";",
            "gcs_folder": gcs_folder,
            "dataset_name": dataset_name,
            "source": "cbs",
        }
    )
