import sys
from datetime import datetime
from retry import retry
from rpy2.robjects import r
from rpy2.rinterface_lib.embedded import RRuntimeError


DEFAULT_DBKEYS = ["16021", "12515", "12524", "13081"]
DATE_NOW = datetime.now().strftime("%Y-%m-%d")


@retry(RRuntimeError, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace: str,
    param: str,
    dbkeys: list = DEFAULT_DBKEYS,
    date_min: str = "2000-01-01",
    date_max: str = DATE_NOW,
    **kwargs: str | list
) -> None:
    dbkeys_str = "\"" + "\", \"".join(dbkeys) + "\""

    r(
        f"""
        library(dbhydroR)
        library(dplyr)

        dbkeys <- c({dbkeys_str})

        for (i in dbkeys) {{
            # Retrieve data for the dbkey
            data <- get_hydro(dbkey = i, date_min = "{date_min}", date_max = "{date_max}")

            # Extract the column names excluding the date column
            column_names <- names(data)[-1]

            # Generate the filename based on the column names
            if ("{param}" %in% c("RADP", "RADT")) {{
                filename <- paste0("{workspace}/", gsub(" ", "_", sub("_[^_]*$", "", paste(column_names, collapse = "_"))), ".csv")
            }} else {{
                filename <- paste0("{workspace}/", paste(column_names, collapse = "_"), ".csv")
            }}

            # Save data to a CSV file
            write.csv(data, file = filename)

            # Print a message indicating the file has been saved
            cat("CSV file", filename, "has been saved.\n")

            # Add a delay between requests
            Sys.sleep(2) # Wait for 2 seconds before the next iteration
        }}
        """  # noqa: E501
    )

    if param == "RAIN":
        r(
            f"""
            L001_RAIN_Inches <- read.csv("{workspace}/L001_RAIN_Inches.csv", colClasses = c("NULL", "character", "numeric"))
            L005_RAIN_Inches = read.csv("{workspace}/L005_RAIN_Inches.csv", colClasses = c("NULL", "character", "numeric"))
            L006_RAIN_Inches = read.csv("{workspace}/L006_RAIN_Inches.csv", colClasses = c("NULL", "character", "numeric"))
            LZ40_RAIN_Inches = read.csv("{workspace}/LZ40_RAIN_Inches.csv", colClasses = c("NULL", "character", "numeric"))
            #Replace NA values with zero
            L001_RAIN_Inches[is.na(L001_RAIN_Inches)] <- 0
            L005_RAIN_Inches[is.na(L005_RAIN_Inches)] <- 0
            L006_RAIN_Inches[is.na(L006_RAIN_Inches)] <- 0
            LZ40_RAIN_Inches[is.na(LZ40_RAIN_Inches)] <- 0
            # Merge the files by the "date" column
            merged_data <- merge(L001_RAIN_Inches, L005_RAIN_Inches, by = "date",all = TRUE)
            merged_data <- merge(merged_data, L006_RAIN_Inches, by = "date",all = TRUE)
            merged_data <- merge(merged_data, LZ40_RAIN_Inches, by = "date",all = TRUE)
            # Calculate the average rainfall per day
            merged_data$average_rainfall <- rowMeans(merged_data[, -1],na.rm = TRUE)

            # View the updated merged data
            head(merged_data)
            # Save merged data as a CSV file
            write.csv(merged_data, "{workspace}/LAKE_RAINFALL_DATA.csv", row.names = TRUE)
            """  # noqa: E501
        )

    if param == "ETPI":
        r(
            f"""
            L001_ETPI_Inches <- read.csv("{workspace}/L001_ETPI_Inches.csv", colClasses = c("NULL", "character", "numeric"))
            L005_ETPI_Inches = read.csv("{workspace}/L005_ETPI_Inches.csv", colClasses = c("NULL", "character", "numeric"))
            L006_ETPI_Inches = read.csv("{workspace}/L006_ETPI_Inches.csv", colClasses = c("NULL", "character", "numeric"))
            LZ40_ETPI_Inches = read.csv("{workspace}/LZ40_ETPI_Inches.csv", colClasses = c("NULL", "character", "numeric"))

            # Replace NA values with zero
            L001_ETPI_Inches[is.na(L001_ETPI_Inches)] <- 0
            L005_ETPI_Inches[is.na(L005_ETPI_Inches)] <- 0
            L006_ETPI_Inches[is.na(L006_ETPI_Inches)] <- 0
            LZ40_ETPI_Inches[is.na(LZ40_ETPI_Inches)] <- 0
            # Merge the files by the "date" column
            merged_data <- merge(L001_ETPI_Inches, L005_ETPI_Inches, by = "date",all = TRUE)
            merged_data <- merge(merged_data, L006_ETPI_Inches, by = "date",all = TRUE)
            merged_data <- merge(merged_data, LZ40_ETPI_Inches, by = "date",all = TRUE)
            # Calculate the average rainfall per day
            merged_data$average_ETPI <- rowMeans(merged_data[, -1],na.rm = TRUE)

            # View the updated merged data
            head(merged_data)
            # Save merged data as a CSV file
            write.csv(merged_data, "{workspace}/LOONE_AVERAGE_ETPI_DATA.csv", row.names = TRUE)
            """  # noqa: E501
        )


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
