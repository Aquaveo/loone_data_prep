import os
import warnings
import pandas as pd
from datetime import datetime
from retry import retry
from loone_data_prep.herbie_utils import get_fast_herbie_object
from herbie import FastHerbie
import openmeteo_requests
from retry_requests import retry as retry_requests
import requests_cache

warnings.filterwarnings("ignore", message="Will not remove GRIB file because it previously existed.")

POINTS = pd.DataFrame({
    "station": ["L001", "L005", "L006", "LZ40"],
    "longitude": [-80.7934, -80.9724, -80.7828, -80.7890],
    "latitude": [27.1389, 26.9567, 26.8226, 26.9018]
})

WIND_FILE_MAP = {
    "L001": ("L001_WNDS_MPH_predicted.csv", "L001_WNDS_MPH"),
    "L005": ("L005_WNDS_MPH_predicted.csv", "L005_WNDS_MPH"),
    "L006": ("L006_WNDS_MPH_predicted.csv", "L006_WNDS_MPH"),
    "LZ40": ("LZ40_WNDS_MPH_predicted.csv", "LZ40_WNDS_MPH")
}

AIRT_FILE_MAP = {
    "L001": "L001_AIRT_Degrees Celsius_forecast.csv",
    "L005": "L005_AIRT_Degrees Celsius_forecast.csv",
    "L006": "L006_AIRT_Degrees Celsius_forecast.csv",
    "LZ40": "LZ40_AIRT_Degrees Celsius_forecast.csv"
}

AIRT_COLUMN_MAP = {
    "L001": "L001_AIRT_Degrees Celsius",
    "L005": "L005_AIRT_Degrees Celsius",
    "L006": "L006_AIRT_Degrees Celsius",
    "LZ40": "LZ40_AIRT_Degrees Celsius"
}

@retry(Exception, tries=5, delay=15, max_delay=60, backoff=2)
def download_herbie_variable(FH, variable_key, variable_name, point_df):
    """Download a Herbie variable for a given point and return a DataFrame."""
    FH.download(f":{variable_key}")
    ds = FH.xarray(f":{variable_key}", backend_kwargs={"decode_timedelta": True})
    dsi = ds.herbie.pick_points(point_df, method="nearest")

    var_name = {
        "10u": "u10",
        "10v": "v10",
        "2t": "t2m"
    }.get(variable_name, variable_name) 

    ts = dsi[var_name].squeeze()
    df = ts.to_dataframe().reset_index()
    if "valid_time" in df.columns:
        df.rename(columns={"valid_time": "datetime"}, inplace=True)
    elif "time" in df.columns:
        df.rename(columns={"time": "datetime"}, inplace=True)

    df = df[["datetime", var_name]].drop_duplicates()
    ds.close()
    dsi.close()
    del ds, dsi, ts
    return df

# Download ET from Open-Meteo
def download_hourly_et(lat, lon):
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry_requests(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "evapotranspiration",
        "forecast_days": 16,
        "models": "gfs_seamless"
    }
    responses = client.weather_api(url, params=params)
    response = responses[0]

    hourly = response.Hourly()
    hourly_evap = hourly.Variables(0).ValuesAsNumpy()
    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s"),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}
    hourly_data["evapotranspiration"] = hourly_evap
    return pd.DataFrame(hourly_data)

# Main generation function
def generate_all_outputs(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    today_str = datetime.today().strftime('%Y-%m-%d 00:00')
    FH = get_fast_herbie_object(today_str)

    # Forecasted weather data (single point)
    point_df = pd.DataFrame({"longitude": [-80.7976], "latitude": [26.9690]})
    forecast_vars = ["10u", "10v", "2t", "tp", "ssrd"]
    data = {var: download_herbie_variable(FH, var, var, point_df) for var in forecast_vars}

    merged = data["10u"].merge(data["10v"], on="datetime")
    merged = merged.merge(data["2t"], on="datetime")
    merged = merged.merge(data["tp"], on="datetime")
    merged = merged.merge(data["ssrd"], on="datetime")

    # Derived columns
    merged["wind_speed"] = (merged["u10"]**2 + merged["v10"]**2)**0.5  # wind speed in m/s
    merged["wind_speed_corrected"] = 0.4167 * merged["wind_speed"] + 4.1868
    merged["tp_inc_m"] = merged["tp"].diff().clip(lower=0)
    # Convert incremental meters → mm
    merged["tp_inc_mm"] = merged["tp_inc_m"] * 1000.0
    # Apply bias correction (in mm)
    merged["tp_corrected_mm"] = 0.7247 * merged["tp_inc_mm"] + 0.1853
    # convert to inches
    merged["tp_corrected"] = merged["tp_corrected_mm"] * 0.0393701

    merged["ssrd_kwm2"] = merged["ssrd"].diff() / merged["datetime"].diff().dt.total_seconds() / 1000
    merged["ssrd_corrected"] = (1.0530 * merged["ssrd_kwm2"] - 0.0347).clip(lower=0)
    merged = merged[[
        "datetime",
        "wind_speed_corrected",
        "tp_corrected",
        "ssrd_corrected"
    ]]

    # ET for main point
    df_et = download_hourly_et(26.9690, -80.7976)
    merged = merged.merge(df_et, left_on="datetime", right_on="date", how="left").drop(columns=["date"])
    merged.to_csv(os.path.join(output_dir, "forecasted_weather_data.csv"), index=False)

    # 4-point wind and air temp CSVs
    for idx, row in POINTS.iterrows():
        station = row["station"]
        point_df = pd.DataFrame({"longitude": [row.longitude], "latitude": [row.latitude]})

        # Wind
        df_u = download_herbie_variable(FH, "10u", "10u", point_df)
        df_v = download_herbie_variable(FH, "10v", "10v", point_df)
        merged_ws = df_u.merge(df_v, on="datetime")
        merged_ws["wind_speed"] = (merged_ws["u10"]**2 + merged_ws["v10"]**2)**0.5
        merged_ws["wind_speed_corrected"] = 0.4167 * merged_ws["wind_speed"] + 4.1868

        filename, new_col = WIND_FILE_MAP[station]
        merged_ws[["datetime", "wind_speed_corrected"]].rename(
            columns={"datetime": "date", "wind_speed_corrected": new_col}
        ).to_csv(os.path.join(output_dir, filename), index=False)

        # Air temp
        df_t = download_herbie_variable(FH, "2t", "2t", point_df)
        df_t["t2m"] = df_t["t2m"] - 273.15
        df_t.rename(columns={"datetime": "date", "t2m": AIRT_COLUMN_MAP[station]}).to_csv(
            os.path.join(output_dir, AIRT_FILE_MAP[station]), index=False
        )

    # Rainfall, ET, and SSRD 4-point CSVs
    rainfall_dfs, et_dfs, ssrd_dfs = [], [], []

    for idx, row in POINTS.iterrows():
        station = row["station"]
        point_df = pd.DataFrame({"longitude": [row.longitude], "latitude": [row.latitude]})

        # Rainfall
        df_tp = download_herbie_variable(FH, "tp", "tp", point_df)
        # Convert cumulative meters → incremental meters
        df_tp["tp_inc_m"] = df_tp["tp"].diff().clip(lower=0)
        # Convert incremental meters → millimeters
        df_tp["tp_inc_mm"] = df_tp["tp_inc_m"] * 1000.0
        df_tp["date_only"] = df_tp["datetime"].dt.date
        # Sum incremental precipitation per day
        df_daily = df_tp.groupby("date_only")["tp_inc_mm"].sum().reset_index()
        # Apply bias correction on daily totals (in mm)
        df_daily["tp_corrected_mm"] = 0.7247 * df_daily["tp_inc_mm"] + 0.1853
        # Convert corrected mm → inches
        df_daily["tp_corrected_in"] = df_daily["tp_corrected_mm"] * 0.0393701
        df_daily = df_daily.rename(columns={"date_only": "date", "tp_corrected_in": station})
        rainfall_dfs.append(df_daily[["date", station]])

        # ET
        df_et_point = download_hourly_et(row.latitude, row.longitude)
        df_et_point.rename(columns={"evapotranspiration": station}, inplace=True)
        et_dfs.append(df_et_point)

        # SSRD
        df_ssrd = download_herbie_variable(FH, "ssrd", "ssrd", point_df)
        df_ssrd["ssrd_kwm2"] = df_ssrd["ssrd"].diff() / df_ssrd["datetime"].diff().dt.total_seconds() / 1000
        df_ssrd["ssrd_corrected"] = (1.0530 * df_ssrd["ssrd_kwm2"] - 0.0347).clip(lower=0)
        df_ssrd = df_ssrd[["datetime", "ssrd_corrected"]].rename(columns={"datetime": "date", "ssrd_corrected": station})
        ssrd_dfs.append(df_ssrd)

    # Merge rainfall
    rainfall_df = pd.concat(rainfall_dfs, axis=0).groupby("date").first().reset_index()
    rainfall_df["average_rainfall"] = rainfall_df[POINTS["station"]].mean(axis=1)
    rainfall_df.to_csv(os.path.join(output_dir, "LAKE_RAINFALL_DATA_FORECAST.csv"), index=False)

    # Merge ET
    et_df_all = pd.concat(et_dfs, axis=0).groupby("date").first().reset_index()
    et_df_all["average_ETPI"] = et_df_all[POINTS["station"]].mean(axis=1)
    et_df_all.to_csv(os.path.join(output_dir, "LOONE_AVERAGE_ETPI_DATA_FORECAST.csv"), index=False)

    # Combine all SSRD DataFrames
    ssrd_df_all = pd.concat(ssrd_dfs, axis=0)
    ssrd_df_all["date"] = pd.to_datetime(ssrd_df_all["date"])

    # Compute the daily mean for each station
    daily_ssrd = (
        ssrd_df_all.groupby(ssrd_df_all["date"].dt.date)[POINTS["station"]]
        .mean()
        .reset_index()
    )

    daily_ssrd = daily_ssrd.rename(columns={"date": "date"})
    daily_ssrd["Mean_RADT"] = daily_ssrd[POINTS["station"]].mean(axis=1)
    daily_ssrd.to_csv(os.path.join(output_dir, "LO_RADT_data_forecast.csv"), index=False)

    print("All outputs generated successfully.")
