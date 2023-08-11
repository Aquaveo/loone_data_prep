import sys
import os
from loone_data_prep.water_level_data import hydro


D = {
    "LO_Stage": {"dbkeys": ["16022", "12509", "12519", "16265", "15611"]},
    "Stg_3ANW": {"dbkeys": ["LA369"], "date_min": "1972-01-01", "date_max": "2023-04-30"},
    "Stg_2A17": {"dbkeys": ["16531"], "date_min": "1972-01-01", "date_max": "2023-04-30"},
    "Stg_3A3": {"dbkeys": ["16532"], "date_min": "1972-01-01", "date_max": "2023-04-30"},
    "Stg_3A4": {"dbkeys": ["16537"], "date_min": "1972-01-01", "date_max": "2023-04-30"},
    "Stg_3A28": {"dbkeys": ["16538"], "date_min": "1972-01-01", "date_max": "2023-04-30"}
}


def main(workspace: str, d: dict = D) -> dict:
    missing_files = []
    for name, params in d.items():
        print(f"Getting {name}.")
        hydro.get(workspace, name, **params)
        if os.path.exists(os.path.join(workspace, f"{name}.csv")):
            print(f"{name} downloaded successfully.")
        else:
            missing_files.append(f"{name}.csv")
            print(f"{name} could not be downloaded after various tries.")

    if missing_files:
        return {"error": f"The following files could not be downloaded: {missing_files}"}

    return {"success": "Completed water level data download."}


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    main(workspace)
