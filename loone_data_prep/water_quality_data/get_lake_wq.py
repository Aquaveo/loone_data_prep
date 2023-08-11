import sys
import os
from loone_data_prep.water_quality_data import wq


D = {
    "PHOSPHATE, TOTAL AS P": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "PHOSPHATE, ORTHO AS P": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "AMMONIA-N": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "NITRATE+NITRITE-N": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "TOTAL NITROGEN": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN HILR": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN HTYR": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN LA": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN LF": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN LR": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN LW": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN LY": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN RR": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN WR": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "MICROCYSTIN YR": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "CHLOROPHYLL-A": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "CHLOROPHYLL-A(LC)": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "CHLOROPHYLL-A, CORRECTED": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]},
    "DISSOLVED OXYGEN": {"station_ids": ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]}
}


def main(workspace: str, d: dict = D) -> dict:
    missing_files = []
    for name, params in d.items():
        print(f"Getting {name} for the following station IDs: {params['station_ids']}.")
        wq.get(workspace, name, **params)
        for station in params["station_ids"]:
            if not os.path.exists(os.path.join(workspace, f"water_quality_{station}_{name}.csv")):
                missing_files.append(f"water_quality_{station}_{name}.csv")
                print(f"{name} station ID: {station} could not be downloaded after various tries.")

    if missing_files:
        return {"error": f"The following files could not be downloaded: {missing_files}"}

    return {"success": "Completed water quality data download."}


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    main(workspace)
