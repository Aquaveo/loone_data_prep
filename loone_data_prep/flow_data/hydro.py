import sys
from datetime import datetime
from retry import retry
import pandas as pd
from loone_data_prep.utils import df_replace_missing_with_nan, get_dbhydro_api


DATE_NOW = datetime.now().strftime("%Y-%m-%d")


@retry(Exception, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace: str,
    dbkey: str,
    date_min: str = "1990-01-01",
    date_max: str = DATE_NOW,
    station: str | None = None
) -> None:
    """Fetches daily flow data from DBHYDRO and saves it to a CSV file.
    
    Args:
        workspace (str): Path to the workspace directory where data will be saved.
        dbkey (str): The DBHYDRO database key for the station.
        date_min (str): Minimum date for data retrieval in 'YYYY-MM-DD' format.
        date_max (str): Maximum date for data retrieval in 'YYYY-MM-DD' format.
        station (str | None): The station name. If None, the station name will be fetched from DBHYDRO.
    """
    # Get a DbHydroApi instance
    api = get_dbhydro_api()
    
    # Get the daily data from DbHydro
    response = api.get_daily_data([dbkey], 'id', date_min, date_max, 'NGVD29', False)
    
    # Check for failure
    if not response.has_data():
        return
    
    # Get the station name for _reformat_flow_df()
    if station is None:
        station = response.get_site_codes()[0]
    
    # Get the data as a dataframe
    df = response.to_dataframe(True)
    
    # Replace flagged 0 values and -99999.0 with NaN
    df = df_replace_missing_with_nan(df)
    
    # Convert flow from cfs to cmd
    df['value'] = df['value'] * (0.0283168466 * 86400)
    
    # Prepare the dataframe to be reformatted into the expected layout
    df.reset_index(inplace=True)
    df.rename(columns={'datetime': 'date', 'value': f'{station}_FLOW_cmd'}, inplace=True)
    
    # Reformat the flow df to the expected layout
    df = _reformat_flow_df(df, station)
    
    # Check if the station name contains a space
    if ' ' in station:
        # Replace space with underscore in the station name
        station_previous = station
        station = station.replace(' ', '_')

    # Write the data to a CSV file
    df.to_csv(f'{workspace}/{station}_FLOW_{dbkey}_cmd.csv', index=True)


def _reformat_flow_file(workspace:str, station: str, dbkey: str):
    '''
    Reformat the flow data file to the expected layout.
    Converts the format of the dates in the file to 'YYYY-MM-DD' then sorts the data by date.
    Reads and writes to a .CSV file.
    
    Args:
        workspace (str): The path to the workspace directory.
        station (str): The station name.
        dbkey (str): The dbkey for the station.
        
    Returns:
        None
    '''
    # Read in the data
    df = pd.read_csv(f"{workspace}/{station}_FLOW_{dbkey}_cmd.csv")
    
    # Reformat the data
    df = _reformat_flow_df(df, station)
    
    # Write the updated data back to the file
    df.to_csv(f"{workspace}/{station}_FLOW_{dbkey}_cmd.csv")


def _reformat_flow_df(df: pd.DataFrame, station: str) -> pd.DataFrame:
    '''
    Reformat the flow data file to the expected layout.
    Converts the format of the dates in the file to 'YYYY-MM-DD' then sorts the data by date.
    
    Args:
        df (pd.DataFrame): The dataframe containing the flow data.
        station (str): The station name.
        
    Returns:
        pd.DataFrame: The reformatted dataframe.
    '''
    # Grab only the columns we need
    df = df[['date', f'{station}_FLOW_cmd']].copy()
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y')
    
    # Sort the data by date
    df.sort_values('date', inplace=True)
    
    # Renumber the index
    df.reset_index(drop=True, inplace=True)
    
    # Drop rows that are missing values for both the date and value columns
    df = df.drop(df[(df['date'].isna()) & (df[f'{station}_FLOW_cmd'].isna())].index)
    
    # Return the updated dataframe
    return df


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    dbkey = sys.argv[2]
    get(workspace, dbkey)
