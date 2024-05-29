import sys
from retry import retry
from rpy2.robjects import r
from rpy2.rinterface_lib.embedded import RRuntimeError
import pandas as pd


@retry(RRuntimeError, tries=5, delay=15, max_delay=60, backoff=2)
def get(workspace):
    r(
        f"""
        # Load the required libraries
        library(dbhydroR)
        library(dplyr)

        # S65E_Total
        S65E_total = get_hydro(dbkey = c("91656", "AL760"), date_min = "1972-01-01", date_max = "2023-06-30", raw = TRUE)
        
        # Give data.frame correct column names so it can be cleaned using the clean_hydro function
        colnames(S65E_total) <- c("station", "dbkey", "date", "data.value", "qualifer", "revision.date")
        
        # Add a type and units column to data so it can be cleaned using the clean_hydro function
        S65E_total$type <- "FLOW"
        S65E_total$units <- "cfs"
        
        # Clean the data.frame
        S65E_total <- clean_hydro(S65E_total)
        
        # Drop the " _FLOW_cfs" column
        S65E_total <- S65E_total %>% select(-` _FLOW_cfs`)
        
        S65E_total[, -1] <- S65E_total[, -1] * (0.0283168466 * 86400)
        write.csv(S65E_total,file ='{workspace}/S65E_total.csv')
        """
    )
    
    _reformat_s65e_total_file(workspace)

def _reformat_s65e_total_file(workspace: str):
    # Read in the data
    df = pd.read_csv(f"{workspace}/S65E_total.csv")
    
    # Drop unused columns
    df.drop('Unnamed: 0', axis=1, inplace=True)
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y')
    
    # Sort the data by date
    df.sort_values('date', inplace=True)
    
    # Renumber the index
    df.reset_index(drop=True, inplace=True)
    
    # Write the updated data back to the file
    df.to_csv(f"{workspace}/S65E_total.csv")

if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    get(workspace)
