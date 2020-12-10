from statline_bq.utils import (
    check_v4,
    get_urls,
    create_named_dir,
    tables_to_parquet,
    get_dataset_description,
    write_description_to_file,
    get_file_names,
    upload_to_gcs,
    gcs_to_gbq,
)
from prefect import task, Flow, unmapped, Parameter

# Converting functions to tasks
check_v4_task = task(check_v4)
get_urls_task = task(get_urls)
create_named_dir_task = task(create_named_dir)
tables_to_parquet_task = task(tables_to_parquet)
get_dataset_description_task = task(get_dataset_description)
write_description_to_file_task = task(write_description_to_file)
get_file_names_task = task(get_file_names)
upload_to_gcs_task = task(upload_to_gcs)
gcs_to_gbq_task = task(gcs_to_gbq)


@task
def convert_bool_to_version(boolean):
    if boolean:
        return "v4"
    else:
        return "v3"


# ids = ["83583NED"]
ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]


with Flow("CBS") as flow:
    # odata_version = Parameter("odata_version")

    source = Parameter("source", default="cbs")
    config = Parameter("config")
    third_party = Parameter("third_party", default=False)

    is_v4 = check_v4_task.map(ids)
    odata_versions = convert_bool_to_version.map(is_v4)
    urls = get_urls_task.map(ids, odata_version=odata_versions)
    pq_dir = create_named_dir_task.map(
        id=ids,
        odata_version=odata_versions,
        source=unmapped(source),
        config=unmapped(config),
    )
    files_parquet = tables_to_parquet_task.map(
        id=ids,
        urls=urls,
        odata_version=odata_versions,
        source=unmapped(source),
        pq_dir=pq_dir,
    )
    descriptions = get_dataset_description_task.map(
        urls=urls, odata_version=odata_versions
    )
    write_description_to_file_task.map(
        pq_dir=pq_dir,
        source=unmapped(source),
        odata_version=odata_versions,
        id=ids,
        description_text=descriptions,
    )
    gcs_folders = upload_to_gcs_task.map(
        dir=pq_dir,
        source=unmapped(source),
        odata_version=odata_versions,
        id=ids,
        config=unmapped(config),
        upstream_tasks=[files_parquet, descriptions],
    )
    file_names = get_file_names_task.map(files_parquet)
    bq_jobs = gcs_to_gbq_task.map(
        id=ids,
        source=unmapped(source),
        odata_version=odata_versions,
        third_party=unmapped(third_party),
        config=unmapped(config),
        gcs_folder=gcs_folders,
        file_names=file_names,
        upstream_tasks=[gcs_folders],
    )

if __name__ == "__main__":
    from statline_bq.config import get_config
    from pathlib import Path

    config_file = Path.home() / Path("Projects/nl-open-data/nl_open_data/config.toml")
    config = get_config(config_file)
    flow.run(parameters={"config": config, "source": "cbs"})
