import sys
from glob import glob
import pandas as pd

def main(input_dir: str, output_dir: str, ensemble_number: str) -> None:
    #get all 16 inflow ids from geoglows
    INFLOW_IDS = [
        750059718, 750043742, 750035446, 750034865, 750055574, 750053211,
        750050248, 750065049, 750064453, 750049661, 750069195, 750051436,
        750068005, 750063868, 750069782, 750072741
    ]
    
    # Load the first inflow data file to extract the date column
    first_reach = INFLOW_IDS[0]  # Take the first reach ID
    first_inflow_data = pd.read_csv(f"{input_dir}/{first_reach}_INFLOW_cmd_geoglows.csv")

    # Ensure the date column exists and is used for geoglows_flow_df
    geoglows_flow_df = pd.DataFrame(first_inflow_data["date"], columns=["date"])

    # Loop through all reach IDs to extract the relevant ensemble column
    for reach in INFLOW_IDS:
        inflow_data = pd.read_csv(f"{input_dir}/{reach}_INFLOW_cmd_geoglows.csv")
    
        for column_name in inflow_data.columns:
            if str(ensemble_number) in column_name:
                geoglows_flow_df[reach] = inflow_data[column_name]
    
    #Calculate the netflows by summing the inflows
    geoglows_flow_df["Netflows"] = geoglows_flow_df[INFLOW_IDS].sum(axis=1)
    Netflows = pd.DataFrame(geoglows_flow_df["date"], columns=["date"])
    Netflows["Netflows_acft"] = geoglows_flow_df["Netflows"] / 1233.48  # Convert from m^3/s to ac-ft
    Netflows.to_csv(f"{output_dir}/Netflows_acft_geoglows.csv", index=False)
    
if __name__ == "__main__":
    #main(sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/"), sys.argv[3])
    main("/home/rhuber/development/RunLOONE/fixed_data_12_February_2025", "/home/rhuber/development/RunLOONE/geoglows", 1)