import sys
from retry import retry
import pandas as pd
from loone_data_prep.utils import df_replace_missing_with_nan, get_dbhydro_api


@retry(Exception, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace, 
    date_min: str = "1972-01-01", 
    date_max: str = "2023-06-30"
) -> None:
    """Retrieve total flow data for S65E structure (S65E_S + S65EX1_S) and save to CSV.
    
    Args:
        workspace (str): Path to workspace where data will be downloaded.
        date_min (str): Minimum date for data retrieval in 'YYYY-MM-DD' format.
        date_max (str): Maximum date for data retrieval in 'YYYY-MM-DD' format.
    """
    # Get a DbHydroApi instance
    api = get_dbhydro_api()
    
    # S65E_S
    s65e_s = api.get_daily_data(['91656'], 'id', date_min, date_max, 'NGVD29', False)
    
    if not s65e_s.has_data():
        return
    
    df_s65e_s = s65e_s.to_dataframe(True)
    df_s65e_s = df_replace_missing_with_nan(df_s65e_s)                                              # Replace flagged 0 values and -99999.0 with NaN
    df_s65e_s.reset_index(inplace=True)                                                             # Reset index so datetime is a column
    df_s65e_s['value'] = df_s65e_s['value'] * (0.0283168466 * 86400)                                # Convert flow from cfs to cmd
    df_s65e_s = df_s65e_s[['datetime', 'value']].copy()                                             # Grab only the columns we need
    df_s65e_s.rename(columns={'datetime': 'date', 'value': f'S65E_S_FLOW_cfs'}, inplace=True)       # Rename columns to expected names
    
    # S65EX1_S
    s65ex1_s = api.get_daily_data(['AL760'], 'id', date_min, date_max, 'NGVD29', False)
    
    if not s65ex1_s.has_data():
        return
    
    df_s65ex1_s = s65ex1_s.to_dataframe(True)
    df_s65ex1_s = df_replace_missing_with_nan(df_s65ex1_s)                                          # Replace flagged 0 values and -99999.0 with NaN
    df_s65ex1_s.reset_index(inplace=True)                                                           # Reset index so datetime is a column
    df_s65ex1_s['value'] = df_s65ex1_s['value'] * (0.0283168466 * 86400)                            # Convert flow from cfs to cmd
    df_s65ex1_s = df_s65ex1_s[['datetime', 'value']].copy()                                         # Grab only the columns we need
    df_s65ex1_s.rename(columns={'datetime': 'date', 'value': f'S65EX1_S_FLOW_cfs'}, inplace=True)   # Rename columns to expected names
    
    # Combine the data from both stations into a single dataframe
    df = pd.merge(df_s65e_s, df_s65ex1_s, on='date', how='outer', suffixes=('_S65E_S', '_S65EX1_S'))
    
    # Reformat the data to the expected layout
    df = _reformat_s65e_total_df(df)
    
    # Write the data to a file
    df.to_csv(f"{workspace}/S65E_total.csv")


def _reformat_s65e_total_file(workspace: str):
    # Read in the data
    df = pd.read_csv(f"{workspace}/S65E_total.csv")
    
    # Reformat the data
    df = _reformat_s65e_total_df(df)
    
    # Write the updated data back to the file
    df.to_csv(f"{workspace}/S65E_total.csv")


def _reformat_s65e_total_df(df: pd.DataFrame) -> pd.DataFrame:
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y')
    
    # Sort the data by date
    df.sort_values('date', inplace=True)
    
    # Renumber the index
    df.reset_index(drop=True, inplace=True)
    
    # Drop rows that are missing all their values
    df.dropna(how='all', inplace=True)
    
    # Return the reformatted dataframe
    return df


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    get(workspace)
