import sys
import os
from loone_data_prep.water_quality_data import wq


D = {
    "PHOSPHATE, TOTAL AS P": {"station_ids": ['S191', 'S65E', 'S84', 'S154', 'S71', 'S72', 'S4', 'FECSR78', 'S308C',
                                              'CULV10A', 'S133', 'S127', 'S135']},
    "AMMONIA-N": {"station_ids": ['S191', 'S65E', 'S84', 'S154', 'S71', 'S72', 'S4', 'FECSR78', 'S308C',
                                  'CULV10A', 'S133', 'S127', 'S135']},
    "NITRATE+NITRITE-N": {"station_ids": ['S191', 'S65E', 'S84', 'S154', 'S71', 'S72', 'S4', 'FECSR78', 'S308C',
                                          'CULV10A', 'S133', 'S127', 'S135']},
    "TOTAL NITROGEN": {"station_ids": ['S191', 'S65E', 'S84', 'S154', 'S71', 'S72', 'S4', 'FECSR78', 'S308C',
                                       'CULV10A', 'S133', 'S127', 'S135']},
    "CHLOROPHYLL-A": {"station_ids": ['S65E', 'S84', 'S154', 'S71', 'S72', 'S4', 'FECSR78', 'S308C', 'CULV10A', 'S133',
                                      'S127', 'S135', 'S191']},
    "CHLOROPHYLL-A(LC)": {"station_ids": ['S65E', 'S84', 'S154', 'S71', 'S72', 'S4', 'FECSR78', 'S308C', 'CULV10A',
                                          'S133', 'S127', 'S135', 'S191']},
    "CHLOROPHYLL-A, CORRECTED": {"station_ids": ['S65E', 'S84', 'S154', 'S71', 'S72', 'S4', 'FECSR78', 'S308C',
                                                 'CULV10A', 'S133', 'S127', 'S135', 'S191']}
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
