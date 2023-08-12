import sys
from datetime import datetime
from retry import retry
from rpy2.robjects import r
from rpy2.rinterface_lib.embedded import RRuntimeError


DEFAULT_STATION_IDS = ["L001", "L004", "L005", "L006", "L007", "L008", "LZ40"]
DATE_NOW = datetime.now().strftime("%Y-%m-%d")


@retry(RRuntimeError, tries=5, delay=15, max_delay=60, backoff=2)
def get(
    workspace: str,
    name: str,
    station_ids: list = DEFAULT_STATION_IDS,
    date_min: str = "1950-01-01",
    date_max: str = DATE_NOW,
    **kwargs: str | list
) -> None:
    station_ids_str = "\"" + "\", \"".join(station_ids) + "\""
    r(
        f"""
        # Load the required libraries
        library(rio)
        library(dbhydroR)

        # Specify the station IDs, date range, and test names
        station_ids <- c({station_ids_str})
        date_min <- "{date_min}"
        date_max <- "{date_max}"
        test_names <- c("{name}")

        # Loop over the station IDs
        for (station_id in station_ids) {{
            # Retrieve water quality data for the current station ID
            water_quality_data <- tryCatch(
                get_wq(
                    station_id = station_id,
                    date_min = date_min,
                    date_max = date_max,
                    test_name = test_names
                ),
                error = function(e) NULL
            )

            # Check if data is available for the current station ID and test name
            if (!is.null(water_quality_data) && nrow(water_quality_data) > 0) {{
                # Convert the vector to a data frame
                water_quality_data <- as.data.frame(water_quality_data)

                # Calculate the number of days from the minimum date plus 8
                water_quality_data$days <- as.integer(difftime(water_quality_data$date, min(water_quality_data$date), units = "days")) + as.integer(format(min(water_quality_data$date), "%d"))

                # Generate the filename based on the station ID
                filename <- paste0("{workspace}/water_quality_", station_id, "_", test_names, ".csv")

                # Save data to a CSV file
                write.csv(water_quality_data, file = filename)

                # Print a message indicating the file has been saved
                cat("CSV file", filename, "has been saved.\n")
            }} else {{
                # Print a message indicating no data was found for the current station ID and test name
                cat("No data found for station ID", station_id, "and test name", test_names, "\n")
            }}
            Sys.sleep(1) # Wait for 1 seconds before the next iteration
        }}
        """  # noqa: E501
    )


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
