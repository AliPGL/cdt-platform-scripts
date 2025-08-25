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

def find_r(dx_min, n, delta, initial_r=1.1, tolerance=1e-6, max_iterations=1000):
    """
    Solve for r in the equation: dx_min * (1 - r^n) / (1 - r) - delta = 0
    using Newton-Raphson iteration.
    
    Parameters:
    - dx_min (float): Minimum dx value
    - n (int): Number of terms in the geometric series
    - delta (float): Delta value (d)
    - initial_r (float): Initial guess for r (default: 1.1)
    - tolerance (float): Convergence tolerance (default: 1e-6)
    - max_iterations (int): Maximum number of iterations (default: 1000)
    
    Returns:
    - float: The value of r that satisfies the equation
    """
    print(f"Finding r with dx_min={dx_min}, n={n}, delta={delta}, initial_r={initial_r}")
    r = initial_r
    
    def f(r):
        if abs(r - 1.0) < 1e-10:
            print(f"r is too close to 1: {r}, returning infinity to avoid division by zero")
            return float('inf')
        return dx_min * (1 - r**n) / (1 - r) - delta
    
    def f_prime(r):
        if abs(r - 1.0) < 1e-10:
            print(f"r is too close to 1 in derivative: {r}, returning infinity")
            return float('inf')
        term1 = -dx_min * n * r**(n-1) / (1 - r)
        term2 = dx_min * (1 - r**n) / (1 - r)**2
        return term1 + term2
    
    for i in range(max_iterations):
        fr = f(r)
        if abs(fr) < tolerance:
            print(f"Converged after {i+1} iterations with r={r}")
            return r
        
        fpr = f_prime(r)
        if abs(fpr) < 1e-10:
            raise ValueError("Derivative too small, iteration may not converge.")
        
        r = r - fr / fpr
        
        if abs(r - 1.0) < 1e-10:
            print(f"r is too close to 1: {r}, nudging to {1.0 + 1e-5}")
            r = 1.0 + 1e-5
    
    raise ValueError("Failed to converge within the maximum number of iterations.")

def generate_grid_with_two_buffers(buffer_grids, dx_min, buffer_length, n_urban_cells):
    print(f"Generating grid with two buffers: buffer_grids={buffer_grids}, dx_min={dx_min}, buffer_length={buffer_length}, n_urban_cells={n_urban_cells}")
    r = find_r(dx_min, buffer_grids, buffer_length)

    grids = []

    # Pre-urban buffer
    grids_temp = []
    dx = dx_min
    for i in range(buffer_grids):
        grids_temp.append(dx)
        dx *= r
    grids += grids_temp[::-1]

    # Urban region
    for i in range(n_urban_cells):
        grids.append(dx_min)

    # Post-urban buffer
    dx = dx_min
    for i in range(buffer_grids):
        grids.append(dx)
        dx *= r

    return grids

def generate_grid_with_one_buffer(buffer_grids, dx_min, buffer_length, n_urban_cells):
    print(f"Generating grid with one buffer: buffer_grids={buffer_grids}, dx_min={dx_min}, buffer_length={buffer_length}, n_urban_cells={n_urban_cells}")
    r = find_r(dx_min, buffer_grids, buffer_length)

    grids = []

    # Urban region
    for i in range(n_urban_cells):
        grids.append(dx_min)

    # Post-urban buffer
    dx = dx_min
    for i in range(buffer_grids):
        grids.append(dx)
        dx *= r

    print(f"Final grid with one buffer")
    return grids

def generate_grid_info(microclimate, project_id, output_dir, bounds_json_path):
    print(f"Starting generate_grid_info with project_id={project_id}, output_dir={output_dir}")
    
    xf_min = float(microclimate['xf_min'])
    xf_max = float(microclimate['xf_max'])
    yf_min = float(microclimate['yf_min'])
    yf_max = float(microclimate['yf_max'])
    zf_min = float(microclimate['zf_min'])
    zf_max = float(microclimate['zf_max'])
    grid_size = float(microclimate['gridSize'])
    delta = float(microclimate['delta'])
    buffer_grids = int(microclimate['bufferGrids'])

    # Calculate nx, ny, nz
    nx = (xf_max - xf_min) / grid_size
    ny = (yf_max - yf_min) / grid_size
    nz = (zf_max - zf_min) / grid_size

    # Adjust xf_max, yf_max, zf_max if needed
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

    buffer_length = delta * yf_max

    # Adjust buffer_grids to ensure right stretching
    while buffer_grids > 0 and (buffer_grids * grid_size) > buffer_length - 5.0*grid_size:
        buffer_grids -= 1    

    x_min = xf_min - buffer_length
    x_max = xf_max + buffer_length
    y_min = yf_min
    y_max = yf_max + buffer_length
    z_min = zf_min - buffer_length
    z_max = zf_max + buffer_length

    n_x_grid = nx + 2 * buffer_grids
    n_y_grid = ny + buffer_grids
    n_z_grid = nz + 2 * buffer_grids

    x_grid = generate_grid_with_two_buffers(buffer_grids, grid_size, buffer_length, nx)
    y_grid = generate_grid_with_one_buffer(buffer_grids, grid_size, buffer_length, ny)
    z_grid = generate_grid_with_two_buffers(buffer_grids, grid_size, buffer_length, nz)

    # Download stl_bounds.json to get solid counts
    with open(bounds_json_path, "r") as f:
        solid_counts = json.load(f)

    # Ensure all expected keys are present (set missing ones to 0)
    default_keys = ["building", "highway", "grass", "ground", "waterway", "tree"]
    for key in default_keys:
        solid_counts.setdefault(key, 0)

    print(f"Solid counts: {solid_counts}")

    file_path = os.path.join(output_dir, "grid_info.txt")

    with open(file_path, "w") as f:
        f.write("!!!!!!!!!!!Mesh info data (Urban region)!!!!!!!!!!!\n")
        f.write(f"xf_min\t{xf_min}\n")
        f.write(f"xf_max\t{xf_max}\n")
        f.write(f"yf_min\t{yf_min}\n")
        f.write(f"yf_max\t{yf_max}\n")
        f.write(f"zf_min\t{zf_min}\n")
        f.write(f"zf_max\t{zf_max}\n")
        f.write(f"grid_size\t{grid_size}\n")
        f.write("!!!!!!!!!!!Mesh info data (Buffer zone)!!!!!!!!!!!\n")
        f.write(f"x_min\t{x_min}\n")
        f.write(f"x_max\t{x_max}\n")
        f.write(f"y_min\t{y_min}\n")
        f.write(f"y_max\t{y_max}\n")
        f.write(f"z_min\t{z_min}\n")
        f.write(f"z_max\t{z_max}\n")
        f.write(f"delta\t{delta}\n")
        f.write(f"n_grid\t{buffer_grids}\n")
        f.write("!!!!!!!!!!!Mesh size!!!!!!!!!!!\n")
        f.write(f"n_x_grid\t{n_x_grid}\n")
        f.write(f"n_y_grid\t{n_y_grid}\n")
        f.write(f"n_z_grid\t{n_z_grid}\n")

        f.write("x_grid\t" + "\t".join(map(str, x_grid)) + "\n")
        f.write("y_grid\t" + "\t".join(map(str, y_grid)) + "\n")
        f.write("z_grid\t" + "\t".join(map(str, z_grid)) + "\n")

        f.write("\n!!!!!!!!!!!Geometry data!!!!!!!!!!!\n")
        f.write(f"num_buildings\t{solid_counts['building']}\n")
        f.write(f"num_trees\t{solid_counts['tree']}\n")
        f.write(f"num_grasses\t{solid_counts['grass']}\n")
        f.write(f"num_highways\t{solid_counts['highway']}\n")
        f.write(f"num_waterways\t{solid_counts['waterway']}\n")
        f.write(f"num_ground\t{solid_counts['ground']}\n")

    print(f"Successfully wrote grid info to {file_path}")
    return file_path

def get_weather_row_count(bucket, project_id, output_dir):
    """
    Downloads weather_info.json and returns the row_count.
    """
    weather_json_path = os.path.join(output_dir, "weather_info.json")
    download_json(bucket, f"{project_id}/weather_info.json", weather_json_path)
    with open(weather_json_path, "r") as f:
        weather_data = json.load(f)
    row_count = weather_data.get("row_count", None)
    print(f"Weather row_count: {row_count}")
    return row_count

def generate_domain_info(microclimate, output_dir, row_count):
    print(f"Starting generate_domain_info with output_dir={output_dir}, row_count={row_count}")
    output_step = microclimate['outputFrequency']
    num_simulation = row_count if row_count is not None else 1
    num_iteration = microclimate['iterations']
    BEM_coupling = microclimate['BEM_coupling']
    dt = microclimate['timeStep']
    l_ref = microclimate['L_ref']
    pow_u = microclimate['Pow_u']
    cs = microclimate['Cs']
    flow_mode = microclimate['Flow_mode']
    intrp = microclimate['Interpolation']
    ave_needed = microclimate['Ave_needed']
    boussinesq = microclimate['Boussinesq']

    # Extract and process error location coordinates
    xf_min = float(microclimate['xf_min'])
    xf_max = float(microclimate['xf_max'])
    yf_min = float(microclimate['yf_min'])
    yf_max = float(microclimate['yf_max'])
    zf_min = float(microclimate['zf_min'])
    zf_max = float(microclimate['zf_max'])

    xyz_print_1 = microclimate.get('errorLocationX', '').strip()
    xyz_print_2 = microclimate.get('errorLocationY', '').strip()
    xyz_print_3 = microclimate.get('errorLocationZ', '').strip()

    if not xyz_print_1:
        xyz_print_1 = (xf_min + xf_max) / 2
    else:
        xyz_print_1 = float(xyz_print_1)

    if not xyz_print_2:
        xyz_print_2 = yf_max * 1.15 # (yf_min + yf_max) / 2
    else:
        xyz_print_2 = float(xyz_print_2)

    if not xyz_print_3:
        xyz_print_3 = (zf_min + zf_max) / 2
    else:
        xyz_print_3 = float(xyz_print_3)
    
    weather_file = "available"

    file_path = os.path.join(output_dir, "domain_info.txt")

    with open(file_path, "w") as f:
        f.write("!!!!!!!!!!!Setup info!!!!!!!!!!!\n")
        f.write(f"output_step\t{output_step}\n")
        f.write(f"num_Simulation\t{num_simulation}\n")
        f.write(f"num_Iteration\t{num_iteration}\n")
        f.write(f"BEM_coupling\t{BEM_coupling}\n")
        f.write(f"DT\t{dt}\n")
        f.write(f"L_ref\t{l_ref}\n")
        f.write(f"pow_u\t{pow_u}\n")
        f.write(f"Cs\t{cs}\n")
        f.write(f"Flow_Mode\t{flow_mode}\n")
        f.write(f"intrp\t{intrp}\n")
        f.write(f"ave_needed\t{ave_needed}\n")
        f.write(f"Boussinesq\t{boussinesq}\n")
        f.write(f"xyzPrint_1\t{xyz_print_1}\n")
        f.write(f"xyzPrint_2\t{xyz_print_2}\n")
        f.write(f"xyzPrint_3\t{xyz_print_3}\n")
        f.write(f"Weather_file\t{weather_file}\n")

    print(f"Successfully wrote domain info to {file_path}")
    return file_path

if __name__ == "__main__":
    print("Starting generate_microclimate_inputs.py")
    if len(sys.argv) != 4:
        print("Error: Incorrect number of arguments provided")
        print("Usage: python generate_microclimate_inputs.py <bucket> <scenario_id> <project_id>")
        sys.exit(1)

    bucket = sys.argv[1]
    scenario_id = sys.argv[2]
    project_id = sys.argv[3]
    print(f"Script arguments: bucket={bucket}, scenario_id={scenario_id}, project_id={project_id}")

    baseDir = "/home/ec2-user/platform/projects"
    project_dir = os.path.join(baseDir, project_id)
    working_dir = os.path.join(baseDir, project_id, scenario_id)

    os.makedirs(working_dir, exist_ok=True)
    microclimate_dir = os.path.join(working_dir, "microclimate_inputs")
    os.makedirs(microclimate_dir, exist_ok=True)

    local_json = os.path.join(working_dir, "microclimate.json")
    json_key = f"Scenarios/{scenario_id}/microclimate.json"
    print(f"Preparing to download microclimate JSON: {json_key}")

    download_json(bucket, json_key, local_json)

    with open(local_json, "r") as f:
        microclimate = json.load(f)
    print(f"Loaded microclimate data: {microclimate}")

    bounds_json_path = os.path.join(project_dir, "geometry_split_summary.json")
    grid_info_path = generate_grid_info(microclimate, project_id, microclimate_dir, bounds_json_path)
    row_count = get_weather_row_count(bucket, project_id, working_dir)
    print(f"Grid info generated at {grid_info_path}, row_count={row_count}")

    upload_file(bucket, grid_info_path, f"Scenarios/{scenario_id}/microclimate_inputs/grid_info.txt")

    domain_info_path = generate_domain_info(microclimate, microclimate_dir, row_count)
    print(f"Domain info generated at {domain_info_path}")

    upload_file(bucket, domain_info_path, f"Scenarios/{scenario_id}/microclimate_inputs/domain_info.txt")

    # copy geometry_split.stl to microclimate_inputs and change the name to geometry.stl
    geometry_stl_path = os.path.join(baseDir, project_id, "geometry_split.stl")
    if not os.path.isfile(geometry_stl_path):
        print(f"Error: geometry.stl not found at {geometry_stl_path}")
        sys.exit(1)
    target_geometry_stl_path = os.path.join(microclimate_dir, "geometry.stl")
    shutil.copyfile(geometry_stl_path, target_geometry_stl_path)
    print(f"Copied geometry_split.stl to {target_geometry_stl_path}")

    # Define EC2 paths
    source_weather_path = os.path.join(baseDir, project_id, "weather.csv")
    target_weather_path = os.path.join(baseDir, project_id, scenario_id, "microclimate_inputs", "weather.csv")
    
    shutil.copyfile(source_weather_path, target_weather_path)
    print(f"Copied weather.csv from {source_weather_path} to {target_weather_path}")

    # Upload to S3
    upload_file(bucket, target_weather_path, f"Scenarios/{scenario_id}/microclimate_inputs/weather.csv")
    print("Uploaded copied weather.csv to S3 successfully.")

    print("Microclimate input generation complete.")