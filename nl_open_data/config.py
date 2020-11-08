from dataclasses import dataclass, field
import os
import json
from pathlib import Path


def get_gcloud_adc():
    """
    Gets gcloud application default credentials.

    There are two recommended ways to authenticate on GCP:
    - Use `gcloud auth application-default login`,
      see https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login
    - Use service accounts and setting `GOOGLE_APPLICATION_CREDENTIALS`,
      see https://cloud.google.com/docs/authentication/production

    Note prefect uses `google.oauth2.service_account.Credentials.from_service_account_info()
    for initialising clients.
    See https://google-auth.readthedocs.io/en/latest/reference/google.oauth2.service_account.html

    """
    GCLOUD_ADC_PATH = {
        "posix": ".config/gcloud/application_default_credentials.json",
        "nt": "gcloud/application_default_credentials.json",
    }
    if os.name == "posix":
        return Path.home() / GCLOUD_ADC_PATH[os.name]
    elif os.name == "nt":
        return Path(os.getenv["APPDATA"]) / GCLOUD_ADC_PATH[os.name]
    else:
        return None


@dataclass
class GCP:
    credentials_file: Path = field(default_factory=get_gcloud_adc)
    credentials_info: dict = None
    project: str = None
    bucket: str = None
    location: str = "EU"

    def __post_init__(self):
        self.credentials_info = json.load(open(self.credentials_file))


@dataclass
class Paths:
    """Data paths defined relative to root
    """

    root: Path = None
    agb: Path = None
    vektis_open_data: Path = None
    cbs: Path = None
    bag: Path = None
    tmp: Path = None


@dataclass
class Config:
    gcp: GCP = None
    path: Paths = None


def get_config(config):
    """Get configuration.

    config should be one of configs.
    """
    configs = {
        "dk": dict(
            gcp=GCP(project="nl-open-data", location="EU"),
            path=Paths(
                root=Path.home() / "nl-open-data",
                agb=Path("vektis/agb/FAGBX_All_P!Q0"),
                vektis_open_data=Path("vektis/open-data"),
                cbs=Path("cbs"),
                bag=Path("bag"),
                tmp=Path("tmp"),
            ),
        ),
        "dataverbinders": dict(
            gcp=GCP(project="dataverbinders", bucket="dataverbinders", location="EU"),
            path=Paths(
                root=Path.home() / "nl-open-data",
                agb=Path("vektis/agb/FAGBX_All_P!Q0"),
                vektis_open_data=Path("vektis/open-data"),
                cbs=Path("cbs"),
                bag=Path("bag"),
                tmp=Path("tmp"),
            ),
        ),
        "ag": dict(
            gcp=GCP(project="dataverbinders-dev", location="EU"),
            path=Paths(
                root=Path.home() / "Projects/nl-open-data",
                # agb=Path("agb"),
                # vektis_open_data=Path("vektis/open-data"),
                cbs=Path("cbs"),
                # bag=Path("bag"),
                tmp=Path("tmp"),
            ),
        )
    }
    try:
        return Config(**configs[config])

    except KeyError as err:
        print(
            f"{err}: configuration '{config}' not found. Choose from: {configs.keys()}"
        )
