import sys
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from pvlib import solarposition
from datetime import datetime, timedelta
import pytz
import numpy as np
import os

# Function to process user-uploaded weather.csv and generate new CSV with spin-up period
def generate_weather_with_spinup(input_csv_path, output_csv_path, latitude, longitude, timezone, timestep_hour):
    # ---------- 1) Read user CSV (case-insensitive) & validate ----------
    df_user = pd.read_csv(input_csv_path)
    df_user.columns = df_user.columns.str.lower()

    required_time_cols = ["year", "month", "day", "hour", "minute"]
    missing = [c for c in required_time_cols if c not in df_user.columns]
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {missing}")

    # ---------- 2) Validate timestep_hour ----------
    if timestep_hour <= 0 or (24 % timestep_hour) != 0:
        raise ValueError(f"timestep_hour must be a positive divisor of 24; got {timestep_hour}")

    # ---------- 3) Build aware initial timestamp (DST-safe) ----------
    initial_naive = pd.to_datetime(
        f"{int(df_user['year'].iloc[0])}-{int(df_user['month'].iloc[0]):02d}-"
        f"{int(df_user['day'].iloc[0]):02d} {int(df_user['hour'].iloc[0]):02d}:"
        f"{int(df_user['minute'].iloc[0]):02d}"
    )
    initial_minute = int(df_user['minute'].iloc[0])

    local_tz = pytz.timezone(timezone)
    try:
        initial_time = local_tz.localize(initial_naive, is_dst=None)
    except pytz.NonExistentTimeError:
        # Spring forward gap: push forward 1h
        initial_time = local_tz.localize(initial_naive + timedelta(hours=1), is_dst=None)
    except pytz.AmbiguousTimeError:
        # Fall back overlap: choose standard time
        initial_time = local_tz.localize(initial_naive, is_dst=False)

    # ---------- 4) Spin-up window in local time ----------
    start_time = initial_time - timedelta(days=1)
    end_time = initial_time

    # ---------- 5) Fetch Open-Meteo (archive) ----------
    start_time_utc = (start_time - timedelta(days=1)).astimezone(pytz.UTC)  # extra day for safety
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_time_utc.strftime("%Y-%m-%d"),
        "end_date": end_time.strftime("%Y-%m-%d"),
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "wind_speed_10m",
            "shortwave_radiation",
            "diffuse_radiation",
            "direct_normal_irradiance",
            "wind_direction_10m"
        ],
        "timezone": "UTC"  # Request UTC, we convert to local below
    }

    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # ---------- 6) Make a local, minute-aligned time axis ----------
    hourly = response.Hourly()
    utc_dates = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )
    # Convert to local tz and SHIFT by the user's minute offset (e.g., +30 min)
    local_dates = utc_dates.tz_convert(timezone)
    minute_offset = pd.Timedelta(minutes=initial_minute)
    local_dates = local_dates + minute_offset

    # Build a DataFrame from API values, on the shifted local dates
    df_api = pd.DataFrame({
        "date": local_dates,
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
        "dew_point_2m": hourly.Variables(2).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(3).ValuesAsNumpy(),
        "shortwave_radiation": hourly.Variables(4).ValuesAsNumpy(),
        "diffuse_radiation": hourly.Variables(5).ValuesAsNumpy(),
        "direct_normal_irradiance": hourly.Variables(6).ValuesAsNumpy(),
        "wind_direction_10m": hourly.Variables(7).ValuesAsNumpy(),
    })

    # Keep only rows exactly in the 24h spin-up window (now minute-aligned)
    df_api = df_api[(df_api["date"] >= start_time) & (df_api["date"] < end_time)].copy()

    # ---------- 7) Build desired spin-up timestamps and align ----------
    n_steps = int(24 / timestep_hour)
    step = pd.Timedelta(hours=timestep_hour)
    desired_times = pd.DatetimeIndex([start_time + i * step for i in range(n_steps)])

    df_api = df_api.sort_values("date").set_index("date")

    # Try exact reindex first; if DST removes a time, fall back to nearest within 45 min
    df_spinup = df_api.reindex(desired_times)
    if df_spinup.isna().any().any():
        df_spinup = df_api.reindex(desired_times, method="nearest", tolerance=pd.Timedelta("45min"))
        if df_spinup.isna().any().any():
            raise ValueError("Could not align Open-Meteo data to desired spin-up times (check API coverage/DST).")

    df_spinup = df_spinup.reset_index().rename(columns={"index": "date"})

    # ---------- 8) Solar position at the EXACT desired times ----------
    solpos = solarposition.get_solarposition(desired_times, latitude, longitude).reset_index(drop=True)

    # Sanity check lengths
    if len(df_spinup) != len(solpos):
        raise ValueError(f"Mismatch: spin-up rows={len(df_spinup)} vs solpos rows={len(solpos)}")

    # ---------- 9) Build spin-up block with exact column names ----------
    df_output = pd.DataFrame({
        "year":   df_spinup["date"].dt.year,
        "month":  df_spinup["date"].dt.month,
        "day":    df_spinup["date"].dt.day,
        "hour":   df_spinup["date"].dt.hour,
        "minute": df_spinup["date"].dt.minute,
        "outTemDrb": df_spinup["temperature_2m"],               # Dry bulb
        "outTemDep": df_spinup["dew_point_2m"],                 # Dew point
        "outSolDHI": df_spinup["diffuse_radiation"],            # Diffuse horizontal irradiance
        "outSolDNI": df_spinup["direct_normal_irradiance"],     # Direct normal irradiance
        "outTSolHr": df_spinup["shortwave_radiation"],          # Total horizontal radiation
        "outRH":     df_spinup["relative_humidity_2m"],         # Relative humidity
        "outWindS":  df_spinup["wind_speed_10m"],               # Wind speed
        "outWindD":  df_spinup["wind_direction_10m"],           # Wind direction
        "outSolCZe": np.cos(np.deg2rad(solpos["zenith"])),      # Cosine(zenith)
        "outSolZe":  solpos["zenith"],                          # Zenith
        "outSolAzS": solpos["azimuth"],                         # Azimuth (south-based)
        "outSolAzN": (solpos["azimuth"] - 180) % 360,           # Azimuth (north-based)
    })

    # ---------- 10) Append user CSV (case-insensitive -> exact-cased) ----------
    # Coerce user numeric columns where relevant (robust to strings)
    numeric_cols = [
        "outtemdrb","outtemdep","outsoldhi","outsoldni","outtsolhr",
        "outrh","outwinds","outwindd","outsolcze","outsolze","outsolazs","outsolazn"
    ]
    for c in numeric_cols:
        if c in df_user.columns:
            df_user[c] = pd.to_numeric(df_user[c], errors="coerce")

    # Rename user columns to your exact casing
    rename_map = {
        "outtemdrb": "outTemDrb",
        "outtemdep": "outTemDep",
        "outsoldhi": "outSolDHI",
        "outsoldni": "outSolDNI",
        "outtsolhr": "outTSolHr",
        "outrh":     "outRH",
        "outwinds":  "outWindS",
        "outwindd":  "outWindD",
        "outsolcze": "outSolCZe",
        "outsolze":  "outSolZe",
        "outsolazs": "outSolAzS",
        "outsolazn": "outSolAzN",
    }
    df_user = df_user.rename(columns=rename_map)

    # Reorder/keep only final columns for user part too (drop extras silently)
    final_columns = [
        "year","month","day","hour","minute",
        "outTemDrb","outTemDep","outSolDHI","outSolDNI","outTSolHr",
        "outRH","outWindS","outWindD","outSolCZe","outSolZe","outSolAzS","outSolAzN"
    ]
    df_user = df_user[[c for c in final_columns if c in df_user.columns]]

    # Dedupe seam if spin-up last timestamp == user first timestamp
    same_stamp = False
    if not df_user.empty:
        seam_user = tuple(int(df_user.iloc[0][x]) for x in ["year","month","day","hour","minute"])
        seam_spin = tuple(int(df_output.iloc[-1][x]) for x in ["year","month","day","hour","minute"])
        same_stamp = (seam_user == seam_spin)

    if same_stamp:
        df_final = pd.concat([df_output.iloc[:-1], df_user], ignore_index=True)
    else:
        df_final = pd.concat([df_output, df_user], ignore_index=True)

    # Coerce and round numerics in the final output
    for c in ["outTemDrb","outTemDep","outSolDHI","outSolDNI","outTSolHr",
              "outRH","outWindS","outWindD","outSolCZe","outSolZe","outSolAzS","outSolAzN"]:
        if c in df_final.columns:
            df_final[c] = pd.to_numeric(df_final[c], errors="coerce").round(2)

    # Ensure exact column order and write
    df_final = df_final[final_columns]
    df_final.to_csv(output_csv_path, index=False)
    return df_final


# Example usage
if __name__ == "__main__":
    scenario_id = sys.argv[1]
    project_id  = sys.argv[2]
    latitude    = float(sys.argv[3])
    longitude   = float(sys.argv[4])
    timezone    = sys.argv[5]
    timestep_hour = int(sys.argv[6])

    input_csv = f"/home/ec2-user/platform/projects/{project_id}/weather.csv"
    output_dir = f"/home/ec2-user/platform/projects/{project_id}/{scenario_id}"
    os.makedirs(output_dir, exist_ok=True)
    output_csv = os.path.join(output_dir, "weather_spinup.csv")

    generate_weather_with_spinup(input_csv, output_csv, latitude, longitude, timezone, timestep_hour)
