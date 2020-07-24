from pathlib import Path
import json
import zipfile
from config import get_config
from lxml import etree, objectify
import xmltodict
# from tasks import curl_cmd, download, unzip, create_dir
from itertools import count
import prefect
from prefect import task, unmapped, Parameter, Flow
from prefect.engine.results import PrefectResult
from google.cloud import bigquery

# Loading 'txe' configuration.
CONFIG = get_config("txe")

# GCP configurations
GCP = CONFIG.gcp
DATASET = "kadaster"

# BAG Configurations
BAG_VERSION = "08062020"

# Relative paths for BAG files.
BAG = CONFIG.path.root / CONFIG.path.bag
TESTING = CONFIG.path.root / CONFIG.path.cbs
OUTPUT_DIR = CONFIG.path.root / CONFIG.path.tmp

# TEST constants of BAG files.
TEST_WPL_FILE = TESTING / (f"9999WPL{BAG_VERSION}" + ".zip")
TEST_NUM_FILE = TESTING / (f"9999NUM{BAG_VERSION}" + ".zip")

# BAG files as constants.
WPL_FILE = BAG / (f"9999WPL{BAG_VERSION}" + ".zip")
OPR_FILE = BAG / (f"9999OPR{BAG_VERSION}" + ".zip")
NUM_FILE = BAG / (f"9999NUM{BAG_VERSION}" + ".zip")
LIG_FILE = BAG / (f"9999LIG{BAG_VERSION}" + ".zip")
STA_FILE = BAG / (f"9999STA{BAG_VERSION}" + ".zip")
PND_FILE = BAG / (f"9999PND{BAG_VERSION}" + ".zip")
VBO_FILE = BAG / (f"9999VBO{BAG_VERSION}" + ".zip")

# Root tags.
WPL_ROOT = "Woonplaats"
OPR_ROOT = "OpenbareRuimte"
NUM_ROOT = "Nummeraanduiding"
LIG_ROOT = "Ligplaats"
STA_ROOT = "Standplaats"
PND_ROOT = "Pand"
VBO_ROOT = "Verblijfsobject"

XPATH = ".//"

@task(result=PrefectResult())
def create_xml_list(zip_file):
    """
    Creates new directory and list of xml files from nested_zipfile which is in main BAG zipfile.
    """

    new_dir = OUTPUT_DIR / zip_file.name.split(".")[0]
    new_dir.mkdir(exist_ok=True)
    # print(new_dir)

    with zipfile.ZipFile(zip_file) as z:
        return [f for f in z.namelist() if f.endswith(".xml")], new_dir

@task
def create_ndjson(bag_file, xml_file, ndjson_dir, root_tag):
    # Description, see Danny code

    def remove_ns_keys(dict_, root):
        # Description, see Danny code
        keys = list(dict_[root].keys())
        for key in keys:
            if key.startswith("@xmlns"):
                del dict_[root][key] # same as dict_['Woonplaats']['@xmlns']
        return dict_


    # Read zipfile.
    zip = zipfile.ZipFile(bag_file)

    
    # with zip.open(xml_file[0]) as file_:
    with zip.open(xml_file) as file_:
        xml = etree.parse(file_).getroot()

    # Removing namespace and prefix lxml:
    # https://stackoverflow.com/questions/18159221/remove-namespace-and-prefix-from-xml-in-python-using-lxml
    for elem in xml.getiterator():
        elem.tag = etree.QName(elem).localname
    
    etree.cleanup_namespaces(xml)

    # Get normal xml structure and delete (remove_ns_keys) unnecessary xml-tags.
    elements = [
        remove_ns_keys(xmltodict.parse(etree.tostring(element), xml_attribs=False), root_tag)
        for element in xml.findall(XPATH + root_tag)
    ]

    # Transform OrderedDict to json format.
    json_records = [json.dumps(element[root_tag]) for element in elements]
    # print(json_records)

    # ndjson = ndjson_dir / (xml_file[0].split(".")[0] + ".ndjson")
    ndjson = ndjson_dir / (xml_file.split(".")[0] + ".ndjson")
    # print(ndjson)
    
    with open(ndjson, "w") as file_:
        file_.write("\n".join(json_records))
    
    return ndjson


@task
def load_bq(bag_name, path_bag):
    client = bigquery.Client(project=GCP.project)

    dataset_ref = client.dataset(DATASET)
    table_ref = dataset_ref.table(bag_name.name)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    with open(path_bag, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print(f"Loaded {job.output_rows} rows into {DATASET}: {bag_name.name}.")


def main():
    # xml_wpl, dir_wpl = create_xml_list(WPL_FILE)
    # xml_opr, dir_opr = create_xml_list(OPR_FILE)
    # xml_num, dir_num = create_xml_list(NUM_FILE)
    # xml_lig, dir_lig = create_xml_list(LIG_FILE)
    # xml_sta, dir_sta = create_xml_list(STA_FILE)
    # xml_pnd, dir_pnd = create_xml_list(PND_FILE)
    # xml_vbo, dir_vbo = create_xml_list(VBO_FILE)

    # TESTING
    # xml_wpl, dir_wpl = create_xml_list(TEST_WPL_FILE)
    # xml_num, dir_num = create_xml_list(TEST_NUM_FILE)
    # print(xml_num)
    # print(dir_num)

    # print(dir_num)

    # create_ndjson(TEST_WPL_FILE, xml_wpl, dir_wpl, WPL_ROOT)
    # create_ndjson(TEST_NUM_FILE, xml_num, dir_num, NUM_ROOT)
    print("Pickle Rick")


with Flow("BAG-Extract") as flow:
    # wpl_xml, wpl_dir = create_xml_list.run(TEST_WPL_FILE)
    # wpl_mapped = create_ndjson.map(bag_file=unmapped(TEST_WPL_FILE), xml_file=wpl_xml, ndjson_dir=unmapped(wpl_dir), root_tag=unmapped(WPL_ROOT))
    # load_bq.map(bag_name=unmapped(wpl_dir), path_bag=wpl_mapped)


    num_xml, num_dir = create_xml_list.run(TEST_NUM_FILE)
    num_mapped = create_ndjson.map(bag_file=unmapped(TEST_NUM_FILE), xml_file=num_xml, ndjson_dir=unmapped(num_dir), root_tag=unmapped(NUM_ROOT))
    load_bq.map(bag_name=unmapped(num_dir), path_bag=num_mapped)

    # opr_xml, opr_dir = create_xml_list.run(OPR_FILE)
    # opr_mapped = create_ndjson.map(bag_file=unmapped(OPR_FILE), xml_file=opr_xml, ndjson_dir=unmapped(opr_dir), root_tag=unmapped(OPR_ROOT))
    # load_bq.map(bag_name=unmapped(opr_dir), path_bag=opr_mapped)


def prefect_main():
    flow.run()


if __name__ == "__main__":
    # main()
    prefect_main()