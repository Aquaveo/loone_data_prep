import sys
import os
from glob import glob
from loone_data_prep.flow_data import hydro, S65E_total


# Database keys for needed inflow data
DBKEYS = [
    "91370",
    "91371",
    "91373",
    "91377",
    "91379",
    "91401",
    "91429",
    "91473",
    "91508",
    "91510",
    "91513",
    "91599",
    "91608",
    "91656",
    "91668",
    "91675",
    "91687",
    "15627",
    "15640",
    "15626",
    "15642",
    "15638",
]


def main(workspace: str, dbkeys: list = DBKEYS) -> dict:
    # Retrieve inflow data
    for dbkey in dbkeys:
        hydro.get(workspace, dbkey)

    S65E_total.get(workspace)

    # Check if all files were downloaded
    files = glob(f"{workspace}/*FLOW*_cmd.csv")

    for file in files:
        file_dbkey = file.split("_")[-2]

        if file_dbkey in dbkeys:
            # Remove dbkey from file name
            new_file_name = file.replace(f"_{file_dbkey}", "")
            os.rename(file, new_file_name)

            # Remove dbkey from dbkeys so we know it successfully downloaded
            dbkeys.remove(file_dbkey)

    if len(dbkeys) > 0:
        return {
            "error": (
                "The data from the following dbkeys could not be "
                f"downloaded: {dbkeys}"
            )
        }
    elif not os.path.exists(f"{workspace}/S65E_total.csv"):
        return {"error": "S65E_total.csv file could not be downloaded."}

    return {"success": "Completed inflow flow data download."}


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    main(workspace)
