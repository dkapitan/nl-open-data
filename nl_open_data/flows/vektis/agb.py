from pathlib import Path

# TODO: use dataclass
from bunch import Bunch
import google.auth
import pandas as pd
import pandas_gbq
from prefect import task, Flow

import config


AGB_FOLDER = Path(
    "/Volumes/GoogleDrive/My Drive/@kapitan/open-data/vektis-agb/FAGBX_All_P!Q0"
)

AGB = Bunch(
    zorgverlener=Bunch(
        file=AGB_FOLDER / "FAGBX_20_All_AB-en.csv",
        cols=[
            "aanduiding_oud",
            "bestandcode",
            "zorgverlenersoort",
            "zorgverlenernummer",
            "naam",
            "voorletters",
            "voorvoegsels",
            "adelijke_titel",
            "academische_titel",
            "straat",
            "huisnummer",
            "huisnummer_toev",
            "postcode",
            "plaatsnaam",
            "telefoonnummer",
            "geboortedatum",
            "geslacht",
            "datum_aanvang_beroep",
            "datum_einde_beroep",
            "verbijzondering_zvlsrt",
            "reserve",
        ],
        widths=[1, 2, 2, 6, 25, 6, 10, 2, 3, 24, 5, 5, 6, 24, 11, 8, 1, 8, 8, 2, 97],
        date_cols=["geboortedatum", "datum_aanvang_beroep", "datum_einde_beroep",],
    ),
    specialist=Bunch(
        file=AGB_FOLDER / "FAGBX_21_All_AB-en.csv",
        cols=[
            "aanduiding_oud",
            "bestandcode",
            "zorgverlenersoort",
            "zorgverlenernummer",
            "indicatie_hoogleraar",
            "reden_einde_beroep",
            "reserve",
        ],
        widths=[1, 2, 2, 6, 1, 1, 143],
    ),
    zorgverlener_praktijk=Bunch(
        file=AGB_FOLDER / "FAGBX_22_All_AB-en.csv",
        cols=[
            "aanduiding_oud",
            "bestandcode",
            "zorgverlenersoort",
            "zorgverlenernummer",
            "praktijknummer",
            "datum_toetreding_praktijk",
            "datum_uittreding_praktijk",
            "status_in_de_praktijk",
            "leeg",
            "praktijksoort",
            "reserve",
        ],
        widths=[1, 2, 2, 6, 5, 8, 8, 1, 1, 2, 220],
        date_cols=["datum_toetreding_praktijk", "datum_uittreding_praktijk",],
    ),
    praktijk=Bunch(
        file=AGB_FOLDER / "FAGBX_23_All_AB-en.csv",
        cols=[
            "aanduiding_oud",
            "bestandcode",
            "zorgverlenersoort",
            "praktijknummer",
            "naam",
            "telefoonnummer",
            "datum_aanvang_praktijk",
            "datum_einde_praktijk",
            "filler",
            "organisatievorm",
            "reserve",
        ],
        widths=[1, 2, 2, 5, 46, 11, 8, 8, 1, 1, 143,],
        date_cols=["datum_aanvang_praktijk", "datum_einde_praktijk",],
    ),
    zorgverlener_instelling=Bunch(
        file=AGB_FOLDER / "FAGBX_24_All_AB-en.csv",
        cols=[
            "aanduiding_oud",
            "bestandcode",
            "zorgverlenersoort",
            "zorgverlenernummer",
            "instellingsnummer",
            "datum_toetreding_instelling",
            "datum_uittreding_instelling",
            "status_in_de_instelling",
            "reserve",
        ],
        widths=[1, 2, 2, 6, 6, 8, 8, 1, 221],
        date_cols=["datum_toetreding_instelling", "datum_uittreding_instelling",],
    ),
    adres_praktijk=Bunch(
        file=AGB_FOLDER / "FAGBX_25_All_AB-en.csv",
        cols=[
            "aanduiding_oud",
            "bestandcode",
            "zorgverlenersoort",
            "praktijknummer",
            "praktijkadres_volgnummer",
            "straat",
            "huisnummer",
            "huisnummer_toev",
            "postcode",
            "plaatsnaam",
            "reserve",
        ],
        widths=[1, 2, 2, 5, 2, 24, 5, 5, 6, 24, 180],
    ),
)


@task
def parse_agb():
    dfs = Bunch()
    for k, v in AGB.items():
        dfs[k] = pd.read_fwf(v.file, widths=v.widths)
        dfs[k].columns = v.cols
        if "date_cols" in v:
            for date_col in v.date_cols:
                dfs[k][date_col] = pd.to_datetime(
                    dfs[k][date_col].astype(str).str.pad(8, fillchar="0"),
                    format="%d%m%Y",
                    errors="coerce",
                )
    return dfs


@task
def load_agb(dfs, credentials=None, GCP=None):
    """
    Load list of dataframes dfs into GBQ
    """
    for k, v in dfs.items():
        pandas_gbq.to_gbq(
            v,
            ".".join(["vektis", k]),
            project_id=GCP.project,
            credentials=credentials,
            if_exists="replace",
            location=GCP.location,
        )


if __name__ == "__main__":
    # geocode addressen praktijk - need to do this in batches
    # adres_praktijk_xy = pd.DataFrame.from_records(
    #     dfs.adres_praktijk.loc[:, ["postcode", "huisnummer"]].apply(
    #         lambda row: get_RDXY(row.postcode, row.huisnummer), axis=1
    #     )
    # )
    with Flow("AGB") as flow:
        credentials, _ = google.auth.default()
        e = parse_agb()
        l = load_agb(e, credentials=credentials, GCP=config.GCP_DK)

    flow.run()
