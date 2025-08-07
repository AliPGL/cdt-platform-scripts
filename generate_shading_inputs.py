import os
import boto3
import sys
import json
import shutil

def download_json(bucket, key, local_path):
    print(f"Starting download of JSON from s3://{bucket}/{key} to {local_path}")
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, local_path)
    print(f"Successfully downloaded JSON to {local_path}")

def upload_file(bucket, local_path, key):
    print(f"Starting upload of {local_path} to s3://{bucket}/{key}")
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    print(f"Successfully uploaded {local_path} to s3://{bucket}/{key}")

    
def generate_grid_info(shading, project_id, output_dir):
    print(f"Starting generate_grid_info with project_id={project_id}, output_dir={output_dir}")
    
    xf_min = float(shading['xf_min'])
    xf_max = float(shading['xf_max'])
    yf_min = float(shading['yf_min'])
    yf_max = float(shading['yf_max'])
    zf_min = float(shading['zf_min'])
    zf_max = float(shading['zf_max'])
    grid_size = float(shading['gridSize'])
    output_3D = shading['output_3D']

    # Compute nx, ny, nz
    nx = (xf_max - xf_min) / grid_size
    ny = (yf_max - yf_min) / grid_size
    nz = (zf_max - zf_min) / grid_size

    # Adjust max bounds so nx, ny, nz are integers
    if not nx.is_integer():
        xf_max += (round(nx) - nx) * grid_size
        nx = round(nx)
    else:
        nx = int(nx)

    if not ny.is_integer():
        yf_max += (round(ny) - ny) * grid_size
        ny = round(ny)
    else:
        ny = int(ny)

    if not nz.is_integer():
        zf_max += (round(nz) - nz) * grid_size
        nz = round(nz)
    else:
        nz = int(nz)

    # Extend final domain limits for boundary conditions
    x_min = xf_min - 5 * grid_size
    x_max = xf_max + 5 * grid_size
    y_min = yf_min - 5 * grid_size
    y_max = yf_max + 5 * grid_size
    z_min = zf_min
    z_max = zf_max + 5 * grid_size

    file_path = os.path.join(output_dir, "grid_info_shading.txt")

    with open(file_path, "w") as f:
        f.write("!!!!!!!!!!!Mesh info data!!!!!!!!!!!\n")
        f.write(f"xf_min\t{x_min}\n")
        f.write(f"xf_max\t{x_max}\n")
        f.write(f"yf_min\t{y_min}\n")
        f.write(f"yf_max\t{y_max}\n")
        f.write(f"zf_min\t{z_min}\n")
        f.write(f"zf_max\t{z_max}\n")
        f.write(f"grid_size\t{grid_size}\n")
        f.write(f"3D_output\t{output_3D}\n")

    print(f"Successfully wrote grid info to {file_path}")
    return file_path

if __name__ == "__main__":
    print("Starting generate_shading_inputs.py")
    if len(sys.argv) != 4:
        print("Error: Incorrect number of arguments provided")
        print("Usage: python generate_shading_inputs.py <bucket> <scenario_id> <project_id>")
        sys.exit(1)

    bucket = sys.argv[1]
    scenario_id = sys.argv[2]
    project_id = sys.argv[3]
    print(f"Script arguments: bucket={bucket}, scenario_id={scenario_id}, project_id={project_id}")

    baseDir = "/home/ec2-user/platform/projects"
    working_dir = os.path.join(baseDir, project_id, scenario_id)

    os.makedirs(working_dir, exist_ok=True)
    shading_dir = os.path.join(working_dir, "shading_inputs")
    os.makedirs(shading_dir, exist_ok=True)

    local_json = os.path.join(working_dir, "shading.json")
    json_key = f"Scenarios/{scenario_id}/shading.json"
    print(f"Preparing to download shading JSON: {json_key}")

    download_json(bucket, json_key, local_json)

    with open(local_json, "r") as f:
        shading = json.load(f)
    print(f"Loaded shading data: {shading}")

    grid_info_path = generate_grid_info(shading, project_id, shading_dir)
    print(f"Grid info generated at {grid_info_path}")

    upload_file(bucket, grid_info_path, f"Scenarios/{scenario_id}/shading_inputs/grid_info_shading.txt")

    #copy geometry_split.stl to shading_inputs and change the name to geometry.stl
    geometry_stl_path = os.path.join(baseDir, project_id, "geometry_split.stl")
    if not os.path.isfile(geometry_stl_path):
        print(f"Error: geometry_split.stl not found at {geometry_stl_path}")
        sys.exit(1)
    target_geometry_stl_path = os.path.join(shading_dir, "geometry.stl")
    shutil.copyfile(geometry_stl_path, target_geometry_stl_path)
    print(f"Copied geometry_split.stl to {target_geometry_stl_path}")

    # Define EC2 paths
    source_weather_path = os.path.join(baseDir, project_id, scenario_id, "weather_spinup.csv")
    target_weather_path = os.path.join(baseDir, project_id, scenario_id, "shading_inputs", "weather.csv")
    
    shutil.copyfile(source_weather_path, target_weather_path)
    print(f"Copied weather_spinup.csv from {source_weather_path} to {target_weather_path}")

    # Upload to S3
    upload_file(bucket, target_weather_path, f"Scenarios/{scenario_id}/shading_inputs/weather.csv")


    print("Shading input generation complete.")