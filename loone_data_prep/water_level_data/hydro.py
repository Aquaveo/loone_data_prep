import sys
from datetime import datetime
from retry import retry
import pandas as pd
from loone_data_prep.utils import get_dbhydro_api

DEFAULT_DBKEYS = ["16022", "12509", "12519", "16265", "15611"]
DATE_NOW = datetime.now().strftime("%Y-%m-%d")


@retry(Exception, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace: str,
    name: str,
    dbkeys: list = DEFAULT_DBKEYS,
    date_min: str = "1950-01-01",
    date_max: str = DATE_NOW,
    datum: str = "",
    **kwargs: str | list | dict
) -> None:
    """Fetches daily water level data from DBHYDRO and saves it as a CSV file.
    
    Args:
        workspace (str): The directory where the CSV file will be saved.
        name (str): The name of the output CSV file (without extension).
        dbkeys (list): List of DBHYDRO dbkeys to fetch data for. Defaults to DEFAULT_DBKEYS.
        date_min (str): The start date for data retrieval in 'YYYY-MM-DD' format. Defaults to '1950-01-01'.
        date_max (str): The end date for data retrieval in 'YYYY-MM-DD' format. Defaults to current date.
        datum (str): The datum to use for the water level data. Defaults to an empty string. One of 'NGVD29', or 'NAVD88'.
        **kwargs: Additional keyword arguments. Can include 'override_site_codes' (dict) to rename site codes in the output.
    """
    # Get the type and units for the station
    data_type = "STG"
    units = "ft NGVD29"
    
    if name in ["Stg_3A3", "Stg_2A17", "Stg_3A4", "Stg_3A28"]:
        data_type = "GAGHT"
        units = "feet"
    
    # Retrieve the data
    api = get_dbhydro_api()
    response = api.get_daily_data(dbkeys, 'id', date_min, date_max, datum, False)
    
    # Get the data as a dataframe
    df = response.to_dataframe()
    
    # Make sure datetime exists as a column
    if 'datetime' not in df.columns:
        df.reset_index(inplace=True)
    
    # Pivot the data so that each site_code is a column
    df = df.pivot(index='datetime', columns='site_code', values='value')
    
    # Get the current column names in df and the names to rename them to
    column_names = {'datetime': 'date'}
    override_site_codes = kwargs.get("override_site_codes", None)
    for column in df.columns:
        if override_site_codes and column in override_site_codes:
            column_names[column] = f"{override_site_codes[column]}_{data_type}_{units}"
        else:
            column_names[column] = f"{column}_{data_type}_{units}"
    
    # Reset the index to turn the datetime index into a column
    df.reset_index(inplace=True)
    
    # Rename the columns
    df.rename(columns=column_names, inplace=True)
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Drop the "Unnamed: 0" column if it exists
    if 'Unnamed: 0' in df.columns:
        df.drop(columns=['Unnamed: 0'], inplace=True)
    
    # Write the data to a csv file
    df.to_csv(f"{workspace}/{name}.csv", index=False)


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
