from loone_data_prep.utils import get_dbkeys
import os
import sys
import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
import geoglows
import datetime

STATION_IDS = [
    "S191_S",
    "S65E_S",
    "S65EX1_S",
    "S84_S",
    "S154_C",
    "S71_S",
    "S72_S",
    "FISHP",
    "S308.DS",
    "L8.441",
    "S133_P",
    "S127_C",
    "S127_P",
    "S129_C",
    "S135_C",
    "S2_P",
    "S3_P",
    "S4_P",
    "S351_S",
    "S352_S",
    "S354_S",
    "S129 PMP_P",
    "S135 PMP_P",
    "S77_S",
    "INDUST",
    "S79_S",
    "S80_S",
    "S40_S",
    "S49_S",
]  # Added these stations. They seemed to be missing.

SECONDS_IN_HOUR = 3600
SECONDS_IN_DAY = 86400
HOURS_IN_DAY = 24

FORECAST_DATE = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y%m%d")


def get_stations_latitude_longitude(station_ids: list[str]):
    """Gets the latitudes and longitudes of the given stations.

    Args:
        station_ids (list[str]): The ids of the stations to get the latitudes/longitudes of

    Returns:
        (dict[str, tuple[numpy.float64, numpy.float64]]): A dictionary of format dict<station_id:(latitude,longitude)>

    If a station's latitude/longitude fails to download then its station_id won't be a key in the returned dictionary.
    """
    # The dict that holds the data that gets returned
    station_data = {}

    # Get the station/dbkey data
    r_dataframe = get_dbkeys(
        station_ids=station_ids, category="SW", param="", stat="", recorder="", detail_level="full"
    )

    # Convert the r dataframe to a pandas dataframe
    with (ro.default_converter + pandas2ri.converter).context():
        pd_dataframe = ro.conversion.get_conversion().rpy2py(r_dataframe)

    # Filter out extra rows for each station from the dataframe
    pd_dataframe.drop_duplicates(subset="Station", keep="first", inplace=True)

    # Get latitude/longitude of each station
    for index in pd_dataframe.index:
        station = pd_dataframe["Station"][index]
        latitude = pd_dataframe["Latitude"][index]
        longitude = pd_dataframe["Longitude"][index]

        station_data[station] = latitude, longitude

    return station_data


def get_reach_id(latitude: float, longitude: float):
    """Gets the reach id for the given latitude/longitude.

    Args:
        latitude (float): The latitude to retrieve the reach id of
        longitude (float): The longitude to retrieve the reach id of

    Returns:
        (int): The reach id of the given latitude/longitude
    """
    reach_data = geoglows.streamflow.latlon_to_reach(latitude, longitude)

    if "error" in reach_data:
        raise Exception(reach_data["error"])

    return reach_data["reach_id"]


def get_flow_forecast_ensembles(reach_id: str, forecast_date: str):
    """Gets the 52 ensemble forecasts from geoglows for the given reach_id.

    Args:
        reach_id (str): The reach_id to get the 52 ensemble forecasts for.
        forecast_date (str): A string specifying the date to request in YYYYMMDD format

    Returns:
        (pandas.core.frame.DataFrame): The 52 ensemble flow forecasts.
    """
    return geoglows.streamflow.forecast_ensembles(reach_id=reach_id, forecast_date=forecast_date)


def get_flow_forecast_stats(reach_id: str, forecast_date: str):
    """Gets the 15-day time series forecast stats (max, min, etc) from geoglows for the given reach_id.

    Args:
        reach_id (str): The reach_id to get the forecast stats for.
        forecast_date (str): A string specifying the date to request in YYYYMMDD format

    Returns:
        (pandas.core.frame.DataFrame): The forecast stats
    """
    return geoglows.streamflow.forecast_stats(reach_id=reach_id, forecast_date=forecast_date)


def ensembles_to_csv(
    workspace: str, station_id: str, ensembles: pd.core.frame.DataFrame, stats: pd.core.frame.DataFrame
):
    """Writes the ensembles and stats from the given DataFrames to a file .csv file.
        Each ensemble and stat is written in its own column.
        The name of each file is of the format: <station_id>_FLOW_cmd_geoglows.csv

    Args:
        workspace (str): The path to the directory to write the files out to.
        station_id (str): The id of the station that the data is from.
        ensembles (pandas.core.frame.DataFrame): The DataFrame that holds the flow data.
        stats (pandas.core.frame.DataFrame): The DataFrame that holds the stats data.
    """
    # Get the path to the file that will be written
    file_name = f"{station_id}_FLOW_cmd_geoglows.csv"
    file_path = os.path.join(workspace, file_name)

    # Format DataFrames for LOONE
    ensembles = _format_ensembles_DataFrame(ensembles)
    stats = _format_stats_DataFrame(stats)

    # Append columns in stats to ensembles
    for column_name in stats.columns:
        ensembles[column_name] = stats[column_name].tolist()

    # Write out the .csv file
    ensembles.to_csv(file_path)

    # Notify user of success
    print(f"File Saved: {file_path}")


def _format_ensembles_DataFrame(dataframe: pd.core.frame.DataFrame):
    """Formats, modifies, and returns the given pandas DataFrame's data to a format that LOONE expects.
       The given DataFrame should hold the ensembles retrieved from geoglows.
       Meant to be used as a helper function in ensembles_to_csv().

    Args:
        dataframe (pandas.core.frame.DataFrame):

    Returns:
        (pandas.core.frame.DataFrame): The resulting formatted/modified pandas DataFrame.
    """
    # Remove high resolution columns (ensemble 52)
    if "ensemble_52_m^3/s" in dataframe.columns:
        dataframe.drop(columns="ensemble_52_m^3/s", inplace=True)

    # Remove rows with null values
    dataframe.dropna(axis="index", inplace=True)

    # Make all times in datetimes 00:00:00+00:00 (ignore time, only use date)
    dataframe.index = dataframe.index.normalize()

    # Convert m^3/s data to m^3/h
    dataframe = dataframe.transform(lambda x: x * SECONDS_IN_HOUR)

    # Make negative values 0
    dataframe.clip(0, inplace=True)

    # Get the average m^3/d for each day
    dataframe = dataframe.groupby([dataframe.index]).mean()
    dataframe = dataframe.transform(lambda x: x * HOURS_IN_DAY)

    # Format datetimes to just dates
    dataframe.index = dataframe.index.strftime("%Y-%m-%d")

    # Rename columns to *_m^3/d from *_m^3/s
    column_names = []
    for column_name in dataframe.columns:
        column_names.append(column_name.replace("m^3/s", "m^3/d"))

    dataframe.columns = column_names

    # Rename index from datetimes to date
    dataframe.rename_axis("date", inplace=True)

    # Return resulting DataFrame
    return dataframe


def _format_stats_DataFrame(dataframe: pd.core.frame.DataFrame):
    """Formats, modifies, and returns the given pandas DataFrame's data to a format that LOONE expects.
       The given DataFrame should hold the stats retrieved from geoglows.
       Meant to be used as a helper function in ensembles_to_csv().

    Args:
        dataframe (pandas.core.frame.DataFrame):

    Returns:
        (pandas.core.frame.DataFrame): The resulting formatted/modified pandas DataFrame.
    """
    # Remove high resolution columns (ensemble 52, high_res_m^3/s)
    if "high_res_m^3/s" in dataframe.columns:
        dataframe.drop(columns="high_res_m^3/s", inplace=True)

    # Remove rows with null values
    dataframe.dropna(axis="index", inplace=True)

    # Make all times in datetimes 00:00:00+00:00 (ignore time, only use date)
    dataframe.index = dataframe.index.normalize()

    # Convert m^3/s data to m^3/h
    dataframe = dataframe.transform(lambda x: x * SECONDS_IN_HOUR)

    # Make negative values 0
    dataframe.clip(0, inplace=True)

    # Max Column (Max)
    column_max = dataframe[["flow_max_m^3/s"]].copy()
    column_max = column_max.groupby([column_max.index]).max()

    # 75th Percentile Column (Average)
    column_75percentile = dataframe[["flow_75%_m^3/s"]].copy()
    column_75percentile = column_75percentile.groupby([column_75percentile.index]).mean()

    # Average Column (Weighted Average)
    column_average = dataframe[["flow_avg_m^3/s"]].copy()
    column_average.transform(lambda x: x / 8)
    column_average = column_average.groupby([column_average.index]).sum()

    # 25th Percentile Column (Average)
    column_25percentile = dataframe[["flow_25%_m^3/s"]].copy()
    column_25percentile = column_25percentile.groupby([column_25percentile.index]).mean()

    # Min Column (Min)
    column_min = dataframe[["flow_min_m^3/s"]].copy()
    column_min = column_min.groupby([column_min.index]).min()

    # Convert values in each column from m^3/h to m^3/d
    column_max = column_max.transform(lambda x: x * HOURS_IN_DAY)
    column_75percentile = column_75percentile.transform(lambda x: x * HOURS_IN_DAY)
    column_average = column_average.transform(lambda x: x * HOURS_IN_DAY)
    column_25percentile = column_25percentile.transform(lambda x: x * HOURS_IN_DAY)
    column_min = column_min.transform(lambda x: x * HOURS_IN_DAY)

    # Append modified columns into one pandas DataFrame
    dataframe_result = pd.DataFrame()
    dataframe_result.index = dataframe.groupby([dataframe.index]).mean().index
    dataframe_result["flow_max_m^3/d"] = column_max["flow_max_m^3/s"].tolist()
    dataframe_result["flow_75%_m^3/d"] = column_75percentile["flow_75%_m^3/s"].tolist()
    dataframe_result["flow_avg_m^3/d"] = column_average["flow_avg_m^3/s"].tolist()
    dataframe_result["flow_25%_m^3/d"] = column_25percentile["flow_25%_m^3/s"].tolist()
    dataframe_result["flow_min_m^3/d"] = column_min["flow_min_m^3/s"].tolist()

    # Format datetimes to just dates
    dataframe_result.index = dataframe_result.index.strftime("%Y-%m-%d")

    # Rename index from datetimes to date
    dataframe_result.rename_axis("date", inplace=True)

    # Return resulting DataFrame
    return dataframe_result


def main(workspace: str, station_ids: list[str] = STATION_IDS, forecast_date: str = FORECAST_DATE):
    """Downloads the flow forecasts for the given station ids and writes them out as .csv files.

    Args:
        workspace (str): Where to write the .csv files to.
        station_ids (list[str]): The station ids to get the flow data for.
        forecast_date (str): A string specifying the date to request in YYYYMMDD format.
    """
    # Local Variables
    reach_ids = {}

    # Get the latitude/longitude for each station
    station_locations = get_stations_latitude_longitude(station_ids)

    # Check for any download failures
    for station_id in station_ids:
        if station_id not in station_locations.keys():
            print(f"Error: The longitude and latitude could not be downloaded for station {station_id}")

    # Get station reach ids
    for station_id in station_locations.keys():
        location = station_locations[station_id]
        try:
            reach_ids[station_id] = get_reach_id(location[0], location[1])
        except Exception as e:
            print(f"Error: Failed to get reach id for station {station_id} ({str(e)})")

    # Get the flow data for each station
    for station_id in reach_ids.keys():
        reach_id = reach_ids[station_id]
        station_ensembles = get_flow_forecast_ensembles(reach_id, forecast_date)
        station_stats = get_flow_forecast_stats(reach_id, forecast_date)
        ensembles_to_csv(workspace, station_id, station_ensembles, station_stats)


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    main(workspace)
