import sys
from datetime import datetime
from glob import glob
from retry import retry
import pandas as pd
from rpy2.robjects import r
from rpy2.rinterface_lib.embedded import RRuntimeError


DATE_NOW = datetime.now().strftime("%Y-%m-%d")


@retry(RRuntimeError, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace: str,
    dbkey: str,
    date_min: str = "1990-01-01",
    date_max: str = DATE_NOW
) -> None:
    r_str = f"""
    # Load the required libraries
    library(dbhydroR)

    # Retrieve data for the dbkey
    data <- get_hydro(dbkey = "{dbkey}", date_min = "{date_min}", date_max = "{date_max}")

    # Check if data is empty or contains only the "date" column
    if (ncol(data) <= 1) {{
        cat("No data found for dbkey", "{dbkey}", "Skipping to the next dbkey.\n")
    }}

    # Multiply all columns except "date" column by 0.0283168466 * 86400 to convert Flow rate from cfs to m³/day
    data[, -1] <- data[, -1] * (0.0283168466 * 86400)

    # Extract the column names excluding the date column
    column_names <- names(data)[-1]

    # Generate the filename based on the column names
    filename <- paste0( gsub(" ", "_", sub("_[^_]*$", "", paste(column_names, collapse = "_"))), "_{dbkey}_cmd.csv")
    # Save data to a CSV file
    write.csv(data, file = paste0("{workspace}/", filename))

    # Print a message indicating the file has been saved
    cat("CSV file", filename, "has been saved.\n")

    # Add a delay between requests
    Sys.sleep(1)  # Wait for 1 second before the next iteration
    """

    r(r_str)

    # column values are converted to cmd in R. This snippet makes sure column names are updated accordingly.
    file = glob(f'{workspace}/*FLOW*{dbkey}_cmd.csv')[0]
    df = pd.read_csv(file, index_col=False)
    df.columns = df.columns.astype(str).str.replace("_cfs", "_cmd")
    df.to_csv(file, index=False)

    if __name__ == "__main__":
        workspace = sys.argv[1].rstrip("/")
        dbkey = sys.argv[2]
        get(workspace, dbkey)
