import sys
import pandas as pd
import geoglows


def get_bias_corrected_data(
    station_id: str,
    reach_id: str,
    observed_data_path: str,
    station_ensembles: pd.DataFrame,
    station_stats: pd.DataFrame,
) -> dict:
    observed_data = pd.read_csv(
        observed_data_path,
        index_col=0,
        usecols=["date", f"{station_id}_FLOW_cmd"],
    )
    observed_data.rename(
        columns={f"{station_id}_FLOW_cmd": "Streamflow (m3/s)"}, inplace=True
    )
    observed_data.index = pd.to_datetime(observed_data.index).tz_localize(
        "UTC"
    )
    historical_data = geoglows.streamflow.historic_simulation(reach_id)
    station_ensembles = geoglows.bias.correct_forecast(
        station_ensembles, historical_data, observed_data
    )
    station_stats = geoglows.bias.correct_forecast(
        station_stats, historical_data, observed_data
    )

    return station_ensembles, station_stats


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    station_id = sys.argv[2]
    reach_id = sys.argv[3]
    station_ensembles = sys.argv[4]
    station_stats = sys.argv[5]

    get_bias_corrected_data(
        workspace, station_id, reach_id, station_ensembles, station_stats
    )
