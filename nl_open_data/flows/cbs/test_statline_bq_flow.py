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
check_v4 = task(check_v4)
get_urls = task(get_urls)
create_named_dir = task(create_named_dir)
tables_to_parquet = task(tables_to_parquet)
get_dataset_description = task(get_dataset_description)
write_description_to_file = task(write_description_to_file)
get_file_names = task(get_file_names)
upload_to_gcs = task(upload_to_gcs)
gcs_to_gbq = task(gcs_to_gbq)


ids = ["83583NED"]
# ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]


with Flow("CBS") as flow:
    # odata_version = Parameter("odata_version")

    source = Parameter("source", default="cbs")
    config = Parameter("config")
    third_party = Parameter("third_party", default=False)
    gcp_env = Parameter("gcp_env", default="dev")

    odata_versions = check_v4.map(ids)
    urls = get_urls.map(ids, odata_version=odata_versions)
    pq_dir = create_named_dir.map(
        id=ids,
        odata_version=odata_versions,
        source=unmapped(source),
        config=unmapped(config),
    )
    files_parquet = tables_to_parquet.map(
        id=ids,
        urls=urls,
        odata_version=odata_versions,
        source=unmapped(source),
        pq_dir=pq_dir,
    )
    descriptions = get_dataset_description.map(urls=urls, odata_version=odata_versions)
    desc_files = write_description_to_file.map(
        description_text=descriptions,
        id=ids,
        pq_dir=pq_dir,
        source=unmapped(source),
        odata_version=odata_versions,
        upstream_tasks=[descriptions],
    )
    gcs_folders = upload_to_gcs.map(
        dir=pq_dir,
        source=unmapped(source),
        odata_version=odata_versions,
        id=ids,
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[files_parquet, desc_files],
    )
    file_names = get_file_names.map(files_parquet)
    bq_jobs = gcs_to_gbq.map(
        id=ids,
        source=unmapped(source),
        odata_version=odata_versions,
        third_party=unmapped(third_party),
        config=unmapped(config),
        gcs_folder=gcs_folders,
        file_names=file_names,
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[gcs_folders],
    )

if __name__ == "__main__":
    from statline_bq.config import get_config
    from pathlib import Path

    config_file = Path.home() / Path("Projects/nl-open-data/nl_open_data/config.toml")
    config = get_config(config_file)
    flow.run(parameters={"config": config, "source": "cbs"})
