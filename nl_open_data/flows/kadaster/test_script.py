from pathlib import Path
import json
import zipfile
from config import get_config
from lxml import etree, objectify
import xmltodict
# from tasks import curl_cmd, download, unzip, create_dir
from itertools import count
from google.cloud import bigquery

# Loading 'txe' configuration.
CONFIG = get_config("txe")

# GCP configurations
GCP = CONFIG.gcp

# BAG Configurations
BAG_VERSION = "08062020"

# Relative paths for BAG files.
BAG = CONFIG.path.root / CONFIG.path.bag
TESTING = CONFIG.path.root / CONFIG.path.cbs
OUTPUT_DIR = CONFIG.path.root / CONFIG.path.tmp

# TEST constants of BAG files.
TEST_WPL_FILE = TESTING / (f"9999WPL{BAG_VERSION}" + ".zip")
TEST_NUM_FILE = TESTING / (f"9999NUM{BAG_VERSION}" + ".zip")

test_bag = [
    TESTING / (f"9999WPL{BAG_VERSION}" + ".zip"),
    TESTING / (f"9999NUM{BAG_VERSION}" + ".zip")
]

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


def create_xml_list(zip_file):
    """
    Creates list of xml files from nested_zipfile which is in main BAG zipfile.
    """

    new_dir = OUTPUT_DIR / zip_file.name.split(".")[0]
    new_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_file) as z:
        return [f for f in z.namelist() if f.endswith(".xml")], new_dir


def create_ndjson(bag_file, xmls, ndjson_dir, root_tag):
    # Description, see Danny code

    def remove_ns_keys(dict_, root):
        # Description, see Danny code
        keys = list(dict_[root].keys())
        for key in keys:
            if key.startswith("@xmlns"):
                del dict_[root][key] # same as dict_['Woonplaats']['@xmlns']
        return dict_    


    def parse_xmls(xml_file):
        xml = etree.parse(xml_file).getroot()

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

        # print(elements)

        # Transform OrderedDict to json format.
        json_records = [json.dumps(element[root_tag]) for element in elements]
        # print(json_records)

        nxt_count = next(counter)

        ndjson = ndjson_dir / (xmls[nxt_count].split(".")[0] + ".ndjson")
        # print(ndjson)
        
        with open(ndjson, "w") as file_:
            file_.write("\n".join(json_records))
        
        return ndjson
        # return "Pickle Rick!!"

    # Read zipfile.
    # zip = zipfile.ZipFile(WPL_FILE)
    zip = zipfile.ZipFile(bag_file)

    # Counter
    counter = count()

    # Iterate over xml files located in the zip-file.
    for xml in xmls:
        with zip.open(xml) as file_:
            ndjson_file = parse_xmls(file_)
    
    return ndjson_file


def load_bq(bag_id, path_bag):
    client = bigquery.Client(project=GCP.project)
    #filename = path_bag
    #print("file_location = ", type(str(path_bag)))

    dataset_ref = client.dataset("kadaster")
    table_ref = dataset_ref.table(bag_id.name)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    with open(path_bag, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, "kadaster", bag_id.name))



def parse_main():
    # wpl_xml, wpl_dir = create_xml_list(TEST_WPL_FILE)
    num_xml, num_dir = create_xml_list(TEST_NUM_FILE)

    # parse_wpl(xml_wpl)
    # parse_xml(WPL_FILE, WPL_ROOT)
    # parse_xml(NUM_FILE, NUM_ROOT)

    # create_ndjson(TEST_WPL_FILE, WPL_ROOT)
    # ndjson = create_ndjson(TEST_WPL_FILE, wpl_xml, wpl_dir, WPL_ROOT)
    ndjson = create_ndjson(TEST_NUM_FILE, num_xml, num_dir, NUM_ROOT)

    # load_bq(num_dir, ndjson)


if __name__ == "__main__":
    parse_main()