import sys
import pandas as pd
import geoglows


SECONDS_IN_DAY = 86400


def get_bias_corrected_data(
    station_id: str,
    reach_id: str,
    observed_data_path: str,
    station_ensembles: pd.DataFrame,
    station_stats: pd.DataFrame,
) -> dict:
    # Load the observed data from a CSV file
    observed_data = pd.read_csv(
        observed_data_path,
        index_col=0,
        usecols=["date", f"{station_id}_FLOW_cmd"],
    )
    # Convert the index to datetime and localize it to UTC
    observed_data.index = pd.to_datetime(observed_data.index).tz_localize(
        "UTC"
    )
    # Transform the data by dividing it by the number of seconds in a day
    observed_data = observed_data.transform(lambda x: x / SECONDS_IN_DAY)
    # Rename the value column to "Streamflow (m3/s)"
    observed_data.rename(
        columns={f"{station_id}_FLOW_cmd": "Streamflow (m3/s)"}, inplace=True
    )

    # Prepare the observed data by filling NaN values with the 10yr average
    prepared_od = prep_observed_data(observed_data)

    # Get the historical simulation data for the given reach ID
    historical_data = geoglows.streamflow.historic_simulation(reach_id)

    # Correct the forecast bias in the station ensembles
    station_ensembles = geoglows.bias.correct_forecast(
        station_ensembles, historical_data, prepared_od
    )
    # Correct the forecast bias in the station stats
    station_stats = geoglows.bias.correct_forecast(
        station_stats, historical_data, prepared_od
    )

    # Return the bias-corrected station ensembles and station stats
    return station_ensembles, station_stats


def prep_observed_data(observed_data: pd.DataFrame) -> pd.DataFrame:
    # Group the data by month and day
    grouped_data = observed_data.groupby(
        [observed_data.index.month, observed_data.index.day]
    )

    # Calculate the rolling average of 'Streamflow (m3/s)' for each group
    daily_10yr_avg = (
        grouped_data["Streamflow (m3/s)"]
        .rolling(window=10, min_periods=1, center=True)
        .mean()
    )

    # Reset the multi-index of daily_10yr_avg and sort it by index
    fill_val = daily_10yr_avg.reset_index(level=[0, 1], drop=True).sort_index()

    # Fill NaN in 'Streamflow (m3/s)' with corresponding values from fill_val
    observed_data["Streamflow (m3/s)"] = observed_data[
        "Streamflow (m3/s)"
    ].fillna(fill_val)

    # Return the modified observed_data DataFrame
    return observed_data


if __name__ == "__main__":
    station_id = sys.argv[1]
    reach_id = sys.argv[2]
    observed_data_path = sys.argv[3].rstrip("/")
    station_ensembles = sys.argv[4]
    station_stats = sys.argv[5]

    get_bias_corrected_data(
        station_id,
        reach_id,
        observed_data_path,
        station_ensembles,
        station_stats,
    )
