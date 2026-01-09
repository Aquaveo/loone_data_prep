import os
import sys
from datetime import datetime
from retry import retry
import pandas as pd
from loone_data_prep.utils import df_replace_missing_with_nan, get_dbhydro_api
import csv


DEFAULT_DBKEYS = ["16021", "12515", "12524", "13081"]
DATE_NOW = datetime.now().strftime("%Y-%m-%d")


@retry(Exception, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace: str,
    param: str,
    dbkeys: list = DEFAULT_DBKEYS,
    date_min: str = "2000-01-01",
    date_max: str = DATE_NOW,
    **kwargs: str | list
) -> None:
    """Fetches daily weather data from DBHYDRO for specified dbkeys and date range, and saves the data to CSV files in the specified workspace.
    
    Args:
        workspace (str): The directory where the CSV files will be saved.
        param (str): The type of weather data to fetch (e.g., "RAIN", "ETPI").
        dbkeys (list, optional): List of DBHYDRO dbkeys to fetch data for. Defaults to DEFAULT_DBKEYS.
        date_min (str, optional): The start date for data retrieval in "YYYY-MM-DD" format. Defaults to "2000-01-01".
        date_max (str, optional): The end date for data retrieval in "YYYY-MM-DD" format. Defaults to the current date.
    """
    data_type = param
    data_units_file = None
    data_units_header = None
    
    # Get the units for the file name and column header based on the type of data
    data_units_file, data_units_header = _get_file_header_data_units(data_type)
    
    # Retrieve the data
    api = get_dbhydro_api()
    response = api.get_daily_data(dbkeys, 'id', date_min, date_max, 'NGVD29', False)
    
    # Get the data as a dataframe
    df = response.to_dataframe(True)
    
    # Replace 0 values with NaN when their qualifier is either 'M' or 'N'
    df = df_replace_missing_with_nan(df)
    
    # Map each station to its own dataframe
    station_dfs = {}
    
    for site_code in response.get_site_codes():
        station_dfs[site_code] = df[df['site_code'] == site_code].copy()
    
    # Write out each station's data to its own file
    for station, station_df in station_dfs.items():
        # Get metadata for the station
        parameter_code = station_df['parameter_code'].iloc[0]
        unit_code = station_df['unit_code'].iloc[0]
        
        # Select only the desired columns
        station_df = station_df[['value']].copy()
        
        # Rename datetime index
        station_df.index.rename('date', inplace=True)
        
        # Rename the columns to the expected format
        station_df.rename(columns={'value': f'{station}_{data_type}_{data_units_header}'}, inplace=True)
        
        # Make the date index a column and use an integer index (for backwards compatibility)
        station_df = station_df.reset_index()
        
        # Get the name of the output file
        file_name = ''
        if data_type in ['RADP', 'RADT']:
            file_name = f'{station}_{data_type}.csv'
        else:
            file_name = f'{station}_{data_type}_{data_units_file}.csv'
        
        # Write out the station's data to a csv file
        station_df.to_csv(os.path.join(workspace, file_name), index=True)


def merge_data(workspace: str, data_type: str):
    """
    Merge the data files for the different stations to create either the LAKE_RAINFALL_DATA.csv or LOONE_AVERAGE_ETPI_DATA.csv file.
    
    Args:
        workspace (str): The path to the workspace directory.
        data_type (str): The type of data. Either 'RAIN' for LAKE_RAINFALL_DATA.csv or 'ETPI' for LOONE_AVERAGE_ETPI_DATA.csv.
    """
    
    # Merge the data files for the different stations (LAKE_RAINFALL_DATA.csv)
    if data_type == "RAIN":
        # Read in rain data
        l001_rain_inches = pd.read_csv(os.path.join(workspace, 'L001_RAIN_Inches.csv'), index_col=0)
        l005_rain_inches = pd.read_csv(os.path.join(workspace, 'L005_RAIN_Inches.csv'), index_col=0)
        l006_rain_inches = pd.read_csv(os.path.join(workspace, 'L006_RAIN_Inches.csv'), index_col=0)
        lz40_rain_inches = pd.read_csv(os.path.join(workspace, 'LZ40_RAIN_Inches.csv'), index_col=0)
        
        # Replace NaN values with 0
        l001_rain_inches.fillna(0, inplace=True)
        l005_rain_inches.fillna(0, inplace=True)
        l006_rain_inches.fillna(0, inplace=True)
        lz40_rain_inches.fillna(0, inplace=True)
        
        # Merge the data by the "date" column
        merged_data = pd.merge(l001_rain_inches, l005_rain_inches, on="date", how="outer")
        merged_data = pd.merge(merged_data, l006_rain_inches, on="date", how="outer")
        merged_data = pd.merge(merged_data, lz40_rain_inches, on="date", how="outer")
        
        # Calculate the average rainfall per day
        merged_data['average_rainfall'] = merged_data.iloc[:, 1:].mean(axis=1)
        
        # Make sure the integer index values are quoted in the csv file (for backwards compatibility)
        merged_data.index = merged_data.index.astype(str)
        
        # Save merged data as a CSV file
        merged_data.applymap(lambda x: round(x, 4) if isinstance(x, (float, int)) else x)
        merged_data.to_csv(os.path.join(workspace, 'LAKE_RAINFALL_DATA.csv'), index=True, quoting=csv.QUOTE_NONNUMERIC)

    # Merge the data files for the different stations (LOONE_AVERAGE_ETPI_DATA.csv)
    if data_type == "ETPI":
        # Read in ETPI data
        l001_etpi_inches = pd.read_csv(os.path.join(workspace, 'L001_ETPI_Inches.csv'), index_col=0)
        l005_etpi_inches = pd.read_csv(os.path.join(workspace, 'L005_ETPI_Inches.csv'), index_col=0)
        l006_etpi_inches = pd.read_csv(os.path.join(workspace, 'L006_ETPI_Inches.csv'), index_col=0)
        lz40_etpi_inches = pd.read_csv(os.path.join(workspace, 'LZ40_ETPI_Inches.csv'), index_col=0)
        
        # Replace NaN values with 0
        l001_etpi_inches.fillna(0, inplace=True)
        l005_etpi_inches.fillna(0, inplace=True)
        l006_etpi_inches.fillna(0, inplace=True)
        lz40_etpi_inches.fillna(0, inplace=True)
        
        # Merge the data by the "date" column
        merged_data = pd.merge(l001_etpi_inches, l005_etpi_inches, on="date", how="outer")
        merged_data = pd.merge(merged_data, l006_etpi_inches, on="date", how="outer")
        merged_data = pd.merge(merged_data, lz40_etpi_inches, on="date", how="outer")
        
        # Calculate the average ETPI per day
        merged_data['average_ETPI'] = merged_data.iloc[:, 1:].mean(axis=1)
        
        # Make sure the integer index values are quoted in the csv file (for backwards compatibility)
        merged_data.index = merged_data.index.astype(str)
        
        # Save merged data as a CSV file
        merged_data.to_csv(os.path.join(workspace, 'LOONE_AVERAGE_ETPI_DATA.csv'), index=True, quoting=csv.QUOTE_NONNUMERIC, na_rep='NA')
        
        # Rewrite the file so NA values aren't quoted (for backwards compatibility)
        file_path = os.path.join(workspace, 'LOONE_AVERAGE_ETPI_DATA.csv')
        lines = []

        with open(file_path, 'r') as file:
            lines = file.readlines()
        
        with open(file_path, 'w', newline='') as file:
            for line in lines:
                line = line.replace(',"NA"', ',NA')
                line = line.replace('"NA",', 'NA,')
                line = line.replace(',"NaN"', ',NA')
                line = line.replace('"NaN",', 'NA,')
                file.write(line)


def _get_file_header_data_units(data_type: str) -> tuple[str, str]:
    """
    Retrieves the units of measurement for a given environmental data type to be used in file names and column headers.

    This function maps a specified environmental data type to its corresponding units of measurement. 
    These units are used for naming files and for the column headers within those files. 

    Args:
        data_type (str): The type of environmental data for which units are being requested. Supported types include "RAIN", "ETPI", "H2OT", "RADP", "RADT", "AIRT", and "WNDS".

    Returns:
        tuple[str, str]: A tuple containing two strings. The first string represents the unit of measurement for the file name, and the second string represents the unit of measurement for the column header in the data file.
    """
    # Get the units for the file name and column header based on the type of data
    if data_type == "RAIN":
        data_units_file = "Inches"
        data_units_header = "Inches"
    elif data_type == "ETPI":
        data_units_file = "Inches"
        data_units_header = "Inches"
    elif data_type == "H2OT":
        data_units_file = "Degrees Celsius"
        data_units_header = "Degrees Celsius"
    elif data_type == "RADP":
        data_units_file = ""
        data_units_header = "MICROMOLE/m^2/s"
    elif data_type == "RADT":
        data_units_file = ""
        data_units_header = "kW/m^2"
    elif data_type == "AIRT":
        data_units_file = "Degrees Celsius"
        data_units_header = "Degrees Celsius"
    elif data_type == "WNDS":
        data_units_file = "MPH"
        data_units_header = "MPH"
        
    return data_units_file, data_units_header


if __name__ == "__main__":
    args = [sys.argv[1].rstrip("/"), sys.argv[2]]
    if len(sys.argv) >= 4:
        dbkeys = sys.argv[3].strip("[]").replace(" ", "").split(',')
        args.append(dbkeys)
    if len(sys.argv) >= 5:
        date_min = sys.argv[4]
        args.append(date_min)
    if len(sys.argv) >= 6:
        date_max = sys.argv[5]
        args.append(date_max)

    get(*args)
