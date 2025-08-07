#!/usr/bin/env python3
import pandas as pd
import os
import sys
import boto3
from botocore.exceptions import NoCredentialsError

def download_from_s3(bucket, object_name, local_file_name):
    """
    Download a file from an S3 bucket to the local file system.
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket, object_name, local_file_name)
        print(f"File {local_file_name} was downloaded successfully")
    except Exception as e:
        print(f"An error occurred while downloading: {e}")

def upload_to_s3(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    """
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print("File was uploaded successfully")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"An error occurred: {e}")

def filter_data(file_path, input_argument, scenario_id, layer_id):
    # Define a dictionary to map abbreviations to full variable names
    variable_mapping = {
        'EC': 'Power_Consumption',
        'IAT': 'Indoor_Air_Temperature',
        'ST': 'Surface_Temperature',
        'PPG': 'PV_Power_Generation'
    }
    
    # Parse the input argument
    abbreviation, start_time, end_time = input_argument.split(', ')
    var_type = variable_mapping[abbreviation.strip()]
    start_time = pd.to_datetime(start_time.strip(), format='%Y-%m-%dT%H:%M')
    end_time = pd.to_datetime(end_time.strip(), format='%Y-%m-%dT%H:%M')

    # S3 paths
    bucket_name = 'citydigitaltwin-projects-bucket'
    s3_file_path = f'Scenarios/{scenario_id}/output_City_scale_result.txt'
    
    # Download the data file from S3
    download_from_s3(bucket_name, s3_file_path, file_path)

    # Read the data file
    data = pd.read_csv(file_path, sep='\t')
    data['Time'] = pd.to_datetime(data['Time'], format='%m/%d/%Y %H:%M')

    # Filter based on variable type and time range
    filtered_data = data[(data['Variable'] == var_type) &
                         (data['Time'] >= start_time) &
                         (data['Time'] <= end_time)]

    # Drop the 'Variable' column if it is not needed in the output
    filtered_data = filtered_data.drop('Variable', axis=1)

    # Save the filtered data to a CSV file
    output_filename = f'{abbreviation}_{start_time.strftime("%Y%m%dT%H%M")}_{end_time.strftime("%Y%m%dT%H%M")}.csv'
    filtered_data.to_csv(output_filename, index=False)
    print(f'Data saved to {output_filename}')
    
    s3_output_path = f'Layers/{layer_id}/{output_filename}'
    upload_to_s3(output_filename, bucket_name, s3_output_path)
    os.remove(output_filename)

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python script.py <file_path> <input_argument>")
        sys.exit(1)

    file_path = sys.argv[1]
    input_argument = sys.argv[2]
    scenario_id = sys.argv[3]
    layer_id = sys.argv[4]
    filter_data(file_path, input_argument, scenario_id, layer_id)
