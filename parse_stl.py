import os
import boto3
import sys
from stl import Mesh
import json
import re

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

def parse_stl(file_path):
    """
    Parse an ASCII STL file and compute min/max coordinates and solid names for all solids.
    """
    min_coords = [float("inf"), float("inf"), float("inf")]
    max_coords = [-float("inf"), -float("inf"), -float("inf")]
    solid_counts = {
        "building": 0,
        "highway": 0,
        "grass": 0,
        "ground": 0,
        "waterway": 0,
        "tree": 0
    }
    solid_names = {
        "building": [],
        "highway": [],
        "grass": [],
        "ground": [],
        "waterway": [],
        "tree": []
    }

    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()

            # Process solid names
            if line.startswith("solid"):
                solid_name = line.split()[1]  # Get the solid name after "solid"
                # Check for solid type using prefix (case-insensitive)
                for solid_type in solid_counts.keys():
                    if re.match(f"^{solid_type}", solid_name, re.IGNORECASE):
                        solid_counts[solid_type] += 1
                        solid_names[solid_type].append(solid_name)
                        break

            # Process vertex lines
            if line.startswith("vertex"):
                _, x, y, z = line.split()
                x, y, z = float(x), float(y), float(z)

                # Update min and max coordinates
                min_coords[0] = min(min_coords[0], x)
                min_coords[1] = min(min_coords[1], y)
                min_coords[2] = min(min_coords[2], z)
                max_coords[0] = max(max_coords[0], x)
                max_coords[1] = max(max_coords[1], y)
                max_coords[2] = max(max_coords[2], z)

    return {
        "bounds": {
            "x": {"min": min_coords[0], "max": max_coords[0]},
            "y": {"min": min_coords[1], "max": max_coords[1]},
            "z": {"min": min_coords[2], "max": max_coords[2]},
        },
        "solid_counts": solid_counts,
        "solid_names": solid_names
    }

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python parse_stl.py <s3_bucket> <s3_key> <project_id>")
        sys.exit(1)

    bucket = sys.argv[1]
    s3_key = sys.argv[2]
    project_id = sys.argv[3]

    # Ensure project folder exists
    project_dir = f"/home/ec2-user/platform/projects/{project_id}"
    if not os.path.exists(project_dir):
        os.makedirs(project_dir)
        print(f"Created directory: {project_dir}")

    local_file = f"{project_dir}/geometry.stl"
    result_file = f"{project_dir}/stl_bounds.json"

    # Download the STL file
    download_from_s3(bucket, s3_key, local_file)

    # Parse the STL file
    result = parse_stl(local_file)

    # Save results to a JSON file
    with open(result_file, "w") as f:
        json.dump(result, f)

    # Upload results back to S3
    result_key = f"{project_id}/stl_bounds.json"
    upload_to_s3(result_file, bucket, result_key)

    print(f"STL bounds, solid counts, and solid names uploaded to S3")