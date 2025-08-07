import os
import boto3
import sys
import csv
import json
from datetime import datetime

def download_from_s3(bucket, object_name, local_file_name):
    """
    Download a file from S3 to the local filesystem.
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket, object_name, local_file_name)
        print(f"File {local_file_name} was downloaded successfully")
    except Exception as e:
        print(f"An error occurred while downloading: {e}")

def upload_to_s3(file_name, bucket, object_name):
    """
    Upload a file to S3.
    """
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_name, bucket, object_name)

def parse_weather_csv(file_path):
    """
    Parse a weather CSV file to extract headers, start/end date-time, timestep, and row count.
    """
    with open(file_path, "r", encoding="utf-8-sig") as file:
        csv_reader = csv.DictReader(file)
        headers = csv_reader.fieldnames  # Extract headers
        rows = list(csv_reader)  # Read all rows into a list

        row_count = len(rows)  # Count the number of data rows (excluding header)

        if row_count == 0:
            return {
                "row_count": 0,
                "headers": headers,
                "start_date_time": None,
                "end_date_time": None,
                "timestep_minutes": None,
                "error": "Weather CSV is empty"
            }
        
        def parse_datetime(row):
            return datetime(
                year=int(row["year"]),
                month=int(row["month"]),
                day=int(row["day"]),
                hour=int(row["hour"]),
                minute=int(row["minute"])
            )

        # Extract start date and time from the first row
        start_date_time = parse_datetime(rows[0])
        end_date_time = parse_datetime(rows[-1])
        start_date_time_str = start_date_time.strftime("%Y-%m-%d %H:%M:%S")
        end_date_time_str = end_date_time.strftime("%Y-%m-%d %H:%M:%S")

        # Determine expected interval
        if row_count == 1:
            return {
                "row_count": row_count,
                "headers": headers,
                "start_date_time": start_date_time_str,
                "end_date_time": end_date_time_str,
                "timestep_minutes": 60,
                "error": None
            }

        expected_delta = (parse_datetime(rows[1]) - start_date_time).total_seconds() / 60

        if expected_delta > 360:
            return {
                "row_count": row_count,
                "headers": headers,
                "start_date_time": start_date_time_str,
                "end_date_time": end_date_time_str,
                "timestep_minutes": expected_delta,
                "error": "Time interval of the weather CSV data is too large. Use ≤ 6 hours or split into smaller projects."
            }
        

                # Check all intervals
        for i in range(1, len(rows)):
            delta = (parse_datetime(rows[i]) - parse_datetime(rows[i - 1])).total_seconds() / 60
            if delta != expected_delta:
                return {
                    "row_count": row_count,
                    "headers": headers,
                    "start_date_time": start_date_time_str,
                    "end_date_time": end_date_time_str,
                    "timestep_minutes": expected_delta,
                    "error": f"Inconsistent time interval in weatehr CSV at row {i + 1}."
                }

        return {
            "row_count": row_count,
            "headers": headers,
            "start_date_time": start_date_time_str,
            "end_date_time": end_date_time_str,
            "timestep_minutes": expected_delta,
            "error": None
        }

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python parse_weather.py <s3_bucket> <s3_key> <project_id>")
        sys.exit(1)

    bucket = sys.argv[1]
    s3_key = sys.argv[2]
    project_id = sys.argv[3]

    # Ensure project folder exists
    project_dir = f"/home/ec2-user/platform/projects/{project_id}"
    if not os.path.exists(project_dir):
        os.makedirs(project_dir)
        print(f"Created directory: {project_dir}")

    local_file = f"{project_dir}/weather.csv"
    result_file = f"{project_dir}/weather_info.json"

    # Download the weather CSV file
    download_from_s3(bucket, s3_key, local_file)

    # Parse the weather CSV file
    result = parse_weather_csv(local_file)

    # Save results to a JSON file
    with open(result_file, "w") as f:
        json.dump(result, f)

    # Also return non-zero code if error is present
    if result["error"]:
        print(f"Error: {result['error']}")
        sys.exit(2)

    # Upload results back to S3
    result_key = f"{project_id}/weather_info.json"
    upload_to_s3(result_file, bucket, result_key)

    print(f"Weather info uploaded to {result_key}")