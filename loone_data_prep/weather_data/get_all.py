import sys
from glob import glob
from loone_data_prep.weather_data import weather


D = {
    "RAIN": {"dbkeys": ["16021", "12515", "12524", "13081"]},
    "ETPI": {"dbkeys": ["UT736", "VM675", "UT743", "UT748"]},
    "H2OT": {"dbkeys": ["16031", "12518", "12527", "16267"]},
    "RADP": {"dbkeys": ["16025", "12516", "12525", "15649"]},
    "RADT": {"dbkeys": ["16024", "12512", "12522", "13080"]},
    "AIRT": {"dbkeys": ["16027", "12514", "12911", "13078"]},
    "WNDS": {"dbkeys": ["16023", "12510", "12520", "13076"]}
}


def main(workspace: str, d: dict = D) -> dict:
    missing_files = []
    for name, params in d.items():
        print(f"Getting {name} for the following dbkeys: {params['dbkeys']}.")
        weather.get(workspace, name, **params)
        if len(glob(f"{workspace}/*{name}*.csv")) < len(params["dbkeys"]):
            missing_files.append(True)
            print(f"After various tries, files are still missing for {name}.")

    if True in missing_files:
        return {"error": "Missing files."}

    return {"success": "Completed weather data download."}


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    main(workspace)
