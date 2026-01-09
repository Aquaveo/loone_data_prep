import sys
import os
import requests
import uuid
from datetime import datetime
from loone_data_prep.water_level_data import hydro
from loone_data_prep.utils import find_last_date_in_csv, dbhydro_data_is_latest, get_stations_latitude_longitude
import pandas as pd

DATE_NOW = datetime.now().date().strftime("%Y-%m-%d")

D = {
    "LO_Stage": {"dbkeys": ["16022", "12509", "12519", "16265", "15611"], "datum": "NGVD29"},
    "LO_Stage_2": {"dbkeys": ["94832"], "date_min": "2024-04-30", "datum": "NAVD88"},
    "Stg_3ANW": {"dbkeys": ["LA369"], "date_min": "1972-01-01", "date_max": "2023-04-30", "datum": "NGVD29", "override_site_codes": {"G3ANW": "3A-NW"}},
    "Stg_2A17": {"dbkeys": ["16531"], "date_min": "1972-01-01", "date_max": "2023-04-30", "datum": "NGVD29", "override_site_codes": {"2A-17": "2-17"}},
    "Stg_3A3": {"dbkeys": ["16532"], "date_min": "1972-01-01", "date_max": "2023-04-30", "datum": "NGVD29", "override_site_codes": {"3A-3": "3-63"}},
    "Stg_3A4": {"dbkeys": ["16537"], "date_min": "1972-01-01", "date_max": "2023-04-30", "datum": "NGVD29", "override_site_codes": {"3A-4": "3-64"}},
    "Stg_3A28": {"dbkeys": ["16538"], "date_min": "1972-01-01", "date_max": "2023-04-30", "datum": "NGVD29", "override_site_codes": {"3A-28": "3-65"}},
}


def main(workspace: str, d: dict = D) -> dict:
    missing_files = []
    failed_downloads = []   # List of file names that the script failed to get the latest data for (but the files still exist)
    
    for name, params in d.items():
        # Get the date of the latest data in the csv file
        date_latest = find_last_date_in_csv(workspace, f"{name}.csv")
        
        # File with data for this dbkey does NOT already exist (or possibly some other error occurred)
        if date_latest is None:
            print(f"Getting all water level data for {name}.")
            params['date_max'] = DATE_NOW
            hydro.get(workspace, name, **params)
        else:
            # Check whether the latest data is already up to date.
            requires_data_download = False
            for dbkey in params['dbkeys']:
                if not dbhydro_data_is_latest(date_latest, dbkey):
                    requires_data_download = True
                    break
            
            # Data is already up to date
            if not requires_data_download:
                # Notify that the data is already up to date
                print(f'Downloading of new water level data skipped for {name}. Data is already up to date.')
                continue
            
            # Temporarily rename current data file so it isn't over written
            original_file_name = f"{name}.csv"
            original_file_name_temp = f"{name}_{uuid.uuid4()}.csv"
            os.rename(os.path.join(workspace, original_file_name), os.path.join(workspace, original_file_name_temp))
            
            try:
                # Download only the new data
                date_next = (datetime.strptime(date_latest, "%Y-%m-%d") + pd.DateOffset(days=1)).date().strftime("%Y-%m-%d")
                print(f'Downloading new water level data for {name} starting from date {date_next}')
                kwargs = {}
                if 'override_site_codes' in params:
                    kwargs['override_site_codes'] = params['override_site_codes']
                hydro.get(workspace, name, dbkeys=params['dbkeys'], date_min=date_next, date_max=DATE_NOW, datum=params['datum'], **kwargs)
                
                # Read in the original data and the newly downloaded data
                df_original = pd.read_csv(os.path.join(workspace, original_file_name_temp), index_col='date')
                df_new = pd.read_csv(os.path.join(workspace, original_file_name), index_col='date')
                
                # Merge the new data with the original data
                df_merged = pd.concat([df_original, df_new], ignore_index=False)
                
                # Ensure an integer index (for backwards compatibility)
                df_merged.reset_index(inplace=True)
                df_merged.drop(columns=['Unnamed: 0'], inplace=True, errors='ignore')
                
                # Write out the merged data
                df_merged.to_csv(os.path.join(workspace, original_file_name))
                
                # Remove the original renamed data file
                os.remove(os.path.join(workspace, original_file_name_temp))
            except Exception as e:
                # Notify of the error
                print(f"Error occurred while downloading new water level data: {e}")
                
                # Remove the newly downloaded data file if it exists
                if os.path.exists(os.path.join(workspace, original_file_name)):
                    os.remove(os.path.join(workspace, original_file_name))
                
                # Rename the original renamed file back to its original name
                if os.path.exists(os.path.join(workspace, original_file_name_temp)):
                    os.rename(os.path.join(workspace, original_file_name_temp), os.path.join(workspace, original_file_name))
                
                # Add the file name to the list of failed downloads
                failed_downloads.append(original_file_name)
            
        if os.path.exists(os.path.join(workspace, f"{name}.csv")):
            print(f"{name} downloaded successfully.")
        else:
            missing_files.append(f"{name}.csv")
            print(f"{name} could not be downloaded after various tries.")

    # Merge data from old and new dbkey for station "L OKEE"
    convert_failure = False
    if os.path.exists(os.path.join(workspace, "LO_Stage.csv")) and os.path.exists(os.path.join(workspace, "LO_Stage_2.csv")):
        # Output Progress
        print("\nMerging data for station 'L OKEE'...")
        
        # Get the latitude and longitude of the "L OKEE" station
        lat_long_map = get_stations_latitude_longitude(["L OKEE"])
        latitude, longitude = lat_long_map["L OKEE"]
        
        # Load the LO_Stage.csv file
        df_lo_stage = pd.read_csv(os.path.join(workspace, "LO_Stage.csv"), index_col="date")
        df_lo_stage.index = pd.to_datetime(df_lo_stage.index)
        
        # Load the LO_Stage_2.csv file
        df_lo_stage_2 = pd.read_csv(os.path.join(workspace, "LO_Stage_2.csv"), index_col="date")
        df_lo_stage_2.index = pd.to_datetime(df_lo_stage_2.index)
        
        # Output Progress
        print("Converting NAVD88 to NGVD29 for 'L OKEE's new dbkey...\n")
        
        # Use only the data that is not already in the LO_Stage.csv file and exists in the LO_Stage_2.csv file
        common_dates = df_lo_stage.index.intersection(df_lo_stage_2.index)
        
        missing_mask = (
            df_lo_stage.loc[common_dates, "L OKEE_STG_ft NGVD29"].isna() &
            df_lo_stage_2.loc[common_dates, "L OKEE_STG_ft NGVD29"].notna()
        )
        
        missing_dates: pd.DatetimeIndex = common_dates[missing_mask]
        missing_dates = missing_dates.to_list()
        
        # Convert the stage values from NAVD88 to NGVD29 for the missing dates
        converted_values = {}
        for date in missing_dates:
            try:
                navd88_value = df_lo_stage_2.at[date, "L OKEE_STG_ft NGVD29"]
                ngvd29_value = _convert_navd88_to_ngvd29(latitude, longitude, navd88_value, date.year)
                converted_values[date] = ngvd29_value
            except Exception as e:
                convert_failure = True
                print(str(e))
                break
        
        # Check for conversion failure
        if not convert_failure:        
            # Update the LO_Stage.csv file with the converted values
            for date, value in converted_values.items():
                df_lo_stage.at[date, "L OKEE_STG_ft NGVD29"] = value
            
            # Reset the index
            df_lo_stage.reset_index(inplace=True)
            
            # Drop Unnamed: 0 column that might have been added
            if "Unnamed: 0" in df_lo_stage.columns:
                df_lo_stage.drop(columns=["Unnamed: 0"], inplace=True)
            
            # Save the updated LO_Stage.csv file
            df_lo_stage.to_csv(os.path.join(workspace, "LO_Stage.csv"))
    else:
        # Conversion failed due to missing files
        convert_failure = True
        print("Error: Missing LO_Stage.csv or LO_Stage_2.csv file, cannot convert and merge.")
            
    if missing_files or convert_failure:
        error_string = ""
        
        if missing_files:
            error_string += f"The following files could not be downloaded: {missing_files}"
        
        if failed_downloads:
            error_string += f"\nFailed to download the latest data for the following files: {failed_downloads}"
        
        if convert_failure:
            error_string += "\nFailed to convert NAVD88 to NGVD29 for 'L OKEE' station."
            
        return {"error": error_string}
    
    return {"success": "Completed water level data download."}

def _convert_navd88_to_ngvd29(latitude: float, longitude: float, stage: float, year: int) -> float:
    """Converts a stage value from NAVD88 to NGVD29 using NCAT.
    
    Args:
        latitude (float): The latitude of the station (in decimal degrees format).
        longitude (float): The longitude of the station (in decimal degrees format).
        stage (float): The stage (water level) value to convert (in feet).
        year (int): The year when the stage value was recorded.
        
    Returns:
        float: The converted stage value in feet (NGVD29).
    """
    # Helper functions
    def _feet_to_meters(feet: float) -> float:
        return feet * 0.3048
    
    def _meters_to_feet(meters: float) -> float:
        return meters / 0.3048
    
    # Check for NA value
    if pd.isna(stage):
        return stage
    
    # Convert stage to meters
    stage_meters = _feet_to_meters(stage)
    
    # Make request
    base_url = "https://geodesy.noaa.gov/api/ncat/llh"
    
    params = {
        "lat": latitude,            # latitude
        "lon": longitude,           # longitude
        "orthoHt": stage_meters,    # orthometric height in NAVD88
        "year": year,               # year of observation
        "inDatum": "NAD83(1986)",   # Datum used for input latitude and longitude
        "outDatum": "NAD83(1986)",  # Datum used for output latitude and longitude
        "inVertDatum": "NAVD88",    # vertical datum of input orthometric height
        "outVertDatum": "NGVD29",   # vertical datum of output orthometric height (desired vertical datum)
    }
    
    try:
        response = requests.get(base_url, params=params)
    except Exception as e:
        raise Exception(f"Error converting NAVD88 to NGVD29: {e}")
    
    # Check for failure
    if response.status_code != 200:
        raise Exception(f"Error converting NAVD88 to NGVD29: {response.text}")
    
    # Return converted stage in feet
    try:
        value = _meters_to_feet(float(response.json()["destOrthoht"]))
    except Exception as e:
        raise Exception(f"Error converting NAVD88 to NGVD29: {e}")
    
    return value

if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    main(workspace)
