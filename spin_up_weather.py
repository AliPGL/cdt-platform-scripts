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
    # Read the user-uploaded weather.csv
    df_user = pd.read_csv(input_csv_path)
    
    # Get the initial timestamp from the first row (assumed to be in local time)
    initial_time = pd.to_datetime(f"{int(df_user['year'][0])}-{int(df_user['month'][0]):02d}-{int(df_user['day'][0]):02d} {int(df_user['hour'][0]):02d}:{int(df_user['minute'][0]):02d}")
    
    # Localize initial_time to the specified timezone
    local_tz = pytz.timezone(timezone)
    initial_time = initial_time.tz_localize(local_tz)
    
    # Calculate start and end times for the 24-hour spin-up period in local time
    start_time = initial_time - timedelta(days=1)  # e.g., 2023-01-01 00:00
    end_time = initial_time  # e.g., 2023-01-02 00:00
    
    # Convert start_time to UTC for Open-Meteo API, fetching an extra day to ensure coverage
    start_time_utc = (start_time - timedelta(days=1)).astimezone(pytz.UTC)
    
    # Setup Open-Meteo API client
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    
    # Prepare Open-Meteo API parameters
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
        "timezone": "UTC"  # Force UTC to avoid API timezone issues
    }
    
    # Fetch weather data
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    
    # Process hourly data
    hourly = response.Hourly()
    # Create date range in UTC
    utc_dates = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    # Convert to local timezone
    local_dates = utc_dates.tz_convert(timezone)
    
    hourly_data = {
        "date": local_dates,
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
        "dew_point_2m": hourly.Variables(2).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(3).ValuesAsNumpy(),
        "shortwave_radiation": hourly.Variables(4).ValuesAsNumpy(),
        "diffuse_radiation": hourly.Variables(5).ValuesAsNumpy(),
        "direct_normal_irradiance": hourly.Variables(6).ValuesAsNumpy(),
        "wind_direction_10m": hourly.Variables(7).ValuesAsNumpy()
    }
    
    # Create DataFrame
    df_spinup = pd.DataFrame(data=hourly_data)
    
    # Filter to exact 24-hour period in local time
    df_spinup = df_spinup[(df_spinup['date'] >= start_time) & (df_spinup['date'] < end_time)]

    # Select rows at timestep_hour intervals (e.g., 00:00, 04:00, 08:00 if timestep_hour=4)
    target_hours = list(range(0, 24, timestep_hour))
    df_spinup = df_spinup[df_spinup['date'].dt.hour.isin(target_hours)]
    
    # Reset index to ensure alignment
    df_spinup = df_spinup.reset_index(drop=True)
    
    # Save df_spinup for debugging
    #df_spinup.to_csv("debug_df_spinup.csv", index=False)
        
    # Calculate solar zenith and azimuth using the same timestamps
    times = pd.date_range(start=start_time, periods=len(df_spinup), freq=f'{timestep_hour}h', tz=timezone)
    solpos = solarposition.get_solarposition(times, latitude, longitude)
    
    # Reset solpos index to align with df_spinup
    solpos = solpos.reset_index(drop=True)
    
    # Save solpos for debugging
    #solpos.to_csv("debug_solpos.csv", index=False)
        
    # Verify lengths match
    if len(df_spinup) != len(solpos):
        print(f"Error: df_spinup length ({len(df_spinup)}) does not match solpos length ({len(solpos)})")
        return None
    
    # Create output DataFrame with required columns
    output_data = {
        "year": df_spinup['date'].dt.year,
        "month": df_spinup['date'].dt.month,
        "day": df_spinup['date'].dt.day,
        "hour": df_spinup['date'].dt.hour,
        "minute": df_spinup['date'].dt.minute,
        "outTemDrb": df_spinup['temperature_2m'],  # Dry bulb temperature
        "outTemDep": df_spinup['dew_point_2m'],   # Dew point temperature
        "outSolDHI": df_spinup['diffuse_radiation'],  # Diffuse horizontal irradiance
        "outSolDNI": df_spinup['direct_normal_irradiance'],  # Direct normal irradiance
        "outTSolHr": df_spinup['shortwave_radiation'],  # Total horizontal radiation
        "outRH": df_spinup['relative_humidity_2m'],  # Relative humidity
        "outWindS": df_spinup['wind_speed_10m'],     # Wind speed
        "outWindD": df_spinup['wind_direction_10m'], # Wind direction
        "outSolCZe": np.cos(np.deg2rad(solpos['zenith'])),  # Cosine of zenith angle
        "outSolZe": solpos['zenith'],                       # Zenith angle
        "outSolAzS": solpos['azimuth'],                     # Azimuth angle (south-based)
        "outSolAzN": (solpos['azimuth'] - 180) % 360        # Azimuth angle (north-based)
    }
    
    df_output = pd.DataFrame(output_data)
    
    # Save df_output for debugging
    #df_output.to_csv("debug_df_output.csv", index=False)
        
    # Append user data to spin-up data
    df_final = pd.concat([df_output, df_user], ignore_index=True)
    
    # Ensure numeric columns are properly formatted
    numeric_columns = [
        'outTemDrb', 'outTemDep', 'outSolDHI', 'outSolDNI', 'outTSolHr',
        'outRH', 'outWindS', 'outWindD', 'outSolCZe', 'outSolZe', 'outSolAzS', 'outSolAzN'
    ]
    for col in numeric_columns:
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce').round(2)
    
    # Save to CSV
    df_final.to_csv(output_csv_path, index=False)
    
    return df_final

# Example usage
if __name__ == "__main__":
    scenario_id = sys.argv[1]
    project_id = sys.argv[2]
    latitude = float(sys.argv[3])
    longitude = float(sys.argv[4])
    timezone = sys.argv[5]
    timestep_hour = int(sys.argv[6])  # new arg

    input_csv = f"/home/ec2-user/platform/projects/{project_id}/weather.csv"

    # Define and create output directory
    output_dir = f"/home/ec2-user/platform/projects/{project_id}/{scenario_id}"
    os.makedirs(output_dir, exist_ok=True)

    # Define full output path
    output_csv = os.path.join(output_dir, "weather_spinup.csv")

    generate_weather_with_spinup(input_csv, output_csv, latitude, longitude, timezone, timestep_hour)
