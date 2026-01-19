import csv
import os
import sys
from datetime import datetime
from retry import retry
import pandas as pd
from loone_data_prep.utils import get_dbhydro_api

DEFAULT_STATION_IDS = ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
DATE_NOW = datetime.now().strftime("%Y-%m-%d")


@retry(Exception, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace: str,
    name: str,
    test_number: int,
    station_ids: list = DEFAULT_STATION_IDS,
    date_min: str = "1950-01-01",
    date_max: str = DATE_NOW,
    **kwargs: str | list
) -> None:
    """Fetch water quality data from DBHydro API and save it as CSV files in the specified workspace.
    
    Args:
        workspace (str): The directory where the CSV files will be saved.
        name (str): The name of the water quality parameter. Example: 'PHOSPHATE, TOTAL AS P'
        test_number (int): The DBHydro test number for the water quality parameter.
        station_ids (list, optional): List of station IDs to fetch data for. Defaults to DEFAULT_STATION_IDS.
        date_min (str, optional): The start date for fetching data in YYYY-MM-DD format. Defaults to "1950-01-01".
        date_max (str, optional): The end date for fetching data in YYYY-MM-DD format. Defaults to the current date.
        **kwargs: Additional keyword arguments.
    
    Returns:
        None
    """
    
    # Initialize the DBHydro API
    api = get_dbhydro_api()
    
    # Fetch water quality data
    response = api.get_water_quality(stations=station_ids, test_numbers=[test_number], date_start=date_min, date_end=date_max, exclude_flagged_results=False)
    df = response.to_dataframe(include_metadata=True)
    
    # Process and save data for each station
    for station in station_ids:
        # Get a copy of the data frame for this station
        df_station = df[df['station'] == station].copy()
        
        # Check if the data frame is empty
        if df_station.empty:
            print(f'No data found for station ID {station} and test number {test_number}.')
            continue
        
        # Get the units of the data
        units = df_station['units'].iloc[0] if 'units' in df_station.columns else ''
        
        # Drop unwanted columns
        df_station = df_station[['date_collected_str', 'sig_fig_value']].copy()
        
        # Convert string sig_fig_value to numeric
        df_station['sig_fig_value'] = pd.to_numeric(df_station['sig_fig_value'], errors='coerce')
        
        # Calculate daily average values
        df_station['date_collected_str'] = pd.to_datetime(df_station['date_collected_str'])
        df_station["date_only"] = df_station["date_collected_str"].dt.date
        df_station = df_station.groupby("date_only")["sig_fig_value"].mean().reset_index()
        df_station.rename(columns={"date_only": "date_collected_str"}, inplace=True)
        
        # Format dataframe to expected layout
        df_station['date_collected_str'] = pd.to_datetime(df_station['date_collected_str'])                                     # Convert date_collected_str column to datetime
        df_station.sort_values('date_collected_str', inplace=True)                                                              # Sort df by date_collected_str
        df_station.rename(columns={'date_collected_str': 'date', 'sig_fig_value': f'{station}_{name}_{units}'}, inplace=True)   # Rename columns
        
        # Calculate the days column
        df_station['days'] = (df_station['date'] - df_station['date'].min()).dt.days + df_station['date'].min().day
        
        # Make sure the integer index is written out (for backwards compatibility)
        df_station.reset_index(inplace=True, drop=True)
        
        # Start index at 1 instead of 0 (for backwards compatibility)
        df_station.index = df_station.index + 1
        
        # Make sure the integer index values are quoted in the csv file (for backwards compatibility)
        df_station.index = df_station.index.astype(str)
        
        # Make sure the date column includes time information at midnight (for backwards compatibility)
        df_station['date'] = df_station['date'].dt.strftime('%Y-%m-%d 00:00:00')
        
        # Write out the data frame to a CSV file
        df_station.to_csv(os.path.join(workspace, f'water_quality_{station}_{name}.csv'), index=True, quoting=csv.QUOTE_NONNUMERIC)
        
        # Rewrite the file so dates don't have double quotes around them (for backwards compatibility)
        rewrite_water_quality_file_without_date_quotes(workspace, f'water_quality_{station}_{name}.csv')


def _calculate_days_column(workspace: str, df: pd.DataFrame, date_min: str):
    """
    Calculates the values that should be in the "days" column of the water quality data CSV file
    based on the given date_min and writes the updated data frame back to the CSV file.
    
    Args:
        workspace (str): The path to the workspace directory.
        df (pd.DataFrame): The water quality data dataframe.
        date_min (str): The minimum date that the "days" column values should be calculated from. Should be in format "YYYY-MM-DD".
    """
    # Ensure df['date'] is a pandas datetime Series
    df['date'] = pd.to_datetime(df['date'])
    date_min_object = pd.to_datetime(date_min)

    # Calculate days column for all rows
    df['days'] = (df['date'] - date_min_object).dt.days + date_min_object.day
    
    return df


def rewrite_water_quality_file_without_date_quotes(workspace: str, file_name: str) -> None:
    """
    Rewrites the given water quality CSV file so that the dates don't have double quotes around them (for backwards compatibility).
    
    Args:
        workspace (str): The path to the workspace directory.
        file_name (str): The name of the water quality CSV file.
    """
    # Rewrite the file so dates don't have double quotes around them (for backwards compatibility)
    file_path = os.path.join(workspace, file_name)
    lines = []

    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    with open(file_path, 'w', newline='') as file:
        line_number = 0
        for line in lines:
            if line_number != 0:
                line_split = line.split(',')
                line_split[1] = line_split[1].replace('"', '')  # Remove quotes around dates (2nd column)
                line = ','.join(line_split)
            file.write(line)
            line_number += 1


if __name__ == "__main__":
    args = [sys.argv[1].rstrip("/"), sys.argv[2]]
    if len(sys.argv) >= 4:
        station_ids = sys.argv[3].strip("[]").replace(" ", "").split(',')
        args.append(station_ids)
    if len(sys.argv) >= 5:
        date_min = sys.argv[4]
        args.append(date_min)
    if len(sys.argv) >= 6:
        date_max = sys.argv[5]
        args.append(date_max)

    get(*args)
