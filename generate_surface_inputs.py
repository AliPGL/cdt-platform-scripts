import os
import sys
import boto3
import json
import csv
import shutil
from datetime import datetime, timedelta

def format_datetime(dt_str):
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%m/%d/%YT%H:%M")
    except Exception as e:
        print(f"Warning: could not parse datetime '{dt_str}': {e}")
        return dt_str
    
def download_json(bucket, key, local_path):
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, local_path)

def upload_file(bucket, local_path, key):
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)

def write_txt_settings(surface, path):

    start_time_str = surface.get('start_time')
    end_time_str = surface.get('end_time')

    with open(path, "w") as f:
        f.write("debug_mode=" + surface.get("debug_mode") + "\n")
        f.write("problem_type=" + surface.get("problem_type") + "\n")
        f.write("geometry_type=" + surface.get("geometry_type") + "\n")
        f.write("weather_type=" + surface.get("weather_type") + "\n")
        f.write("weather_interpolation=" + surface.get("weather_interpolation") + "\n")
        
        f.write("\n##### Location settings ######\n")
        f.write(f"city_name={surface.get('city_name')}\n")
        f.write(f"city_latitude={surface.get('city_latitude')}\n")
        f.write(f"city_longitude={surface.get('city_longitude',)}\n")
        f.write(f"city_altitude={surface.get('city_altitude', '0')}\n")
        f.write(f"UTC_offset={surface.get('UTC_offset', '0')}\n")
        
        f.write("\n###### Date and time and timesteps (date format: month/day/year) ######\n")
        try:
            spinup_hours = int(surface.get("surface_spinup_time_hour", 0))
            original_start_dt = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
            adjusted_start_dt = original_start_dt - timedelta(hours=spinup_hours)
            f.write(f"start_time={adjusted_start_dt.strftime('%m/%d/%YT%H:%M')}\n")
        except Exception as e:
            print(f"Warning: could not adjust start_time for spin-up: {e}")
            f.write(f"start_time={format_datetime(start_time_str)}\n")

        f.write(f"end_time={format_datetime(end_time_str)}\n")
        f.write(f"weather_timestep={surface.get('weather_timestep', '60')}\n")
        f.write(f"simulation_timestep={surface.get('simulation_timestep')}\n")
        
        f.write("\n###### Soil Parameters ######\n")
        f.write(f"number_of_soil_layers={surface.get('number_of_soil_layers')}\n")
        f.write(f"number_of_road_layers={surface.get('number_of_road_layers')}\n")
        f.write(f"DZ_soil_target={surface.get('DZ_soil_target')}\n")
        f.write(f"DZ_soil_ref={surface.get('DZ_soil_ref')}\n")
        
        f.write("\n###### Solver Parameters ######\n")
        f.write(f"soil_solver_type={surface.get('soil_solver_type')}\n")
        f.write(f"soil_solver_scheme={surface.get('soil_solver_scheme')}\n")
        f.write(f"building_wall_solver_type={surface.get('building_wall_solver_type')}\n")
        f.write(f"building_wall_solver_scheme={surface.get('building_wall_solver_scheme')}\n")
        f.write(f"exterior_surface_solver_type={surface.get('exterior_surface_solver_type')}\n")

        f.write("\n##### if new method is used for exterior surface solver ####\n")
        f.write(f"exterior_surface_solver_scheme={surface.get('exterior_surface_solver_scheme')}\n")
        f.write(f"sensible_heat_coefficient_method={surface.get('sensible_heat_coefficient_method')}\n")
        f.write(f"aerodynamic_resistance_formula={surface.get('aerodynamic_resistance_formula')}\n")
        f.write(f"surface_resistance_method={surface.get('surface_resistance_method')}\n")
        
        f.write("\n###### Surface spinup settings to get resonable initial state ######\n")
        f.write(f"surface_spinup_status={surface.get('surface_spinup_status')}\n")
        f.write(f"surface_spinup_time_hour={surface.get('surface_spinup_time_hour')}\n")
        
        f.write("\n###### Solar Radiation Related Parameters ######\n")
        f.write(f"default_ground_reflectance={surface.get('default_ground_reflectance')}\n")
        f.write(f"solarTimeIndex={surface.get('solarTimeIndex')}\n")
        
        f.write("\n###### Building Solver Settings ######\n")
        f.write(f"number_of_wall_roof_layers={surface.get('number_of_wall_roof_layers')}\n")
        
        f.write("\n###### Greenroof settings ######\n")
        f.write(f"green_roof={surface.get('green_roof')}\n")
        f.write(f"greenroof_type={surface.get('greenroof_type')}\n")
        
        f.write("\n##### Exterior shading settings ####\n")
        f.write(f"exterior_shading={surface.get('exterior_shading')}\n")
        
        f.write("\n###### Building energy model settings ######\n")
        f.write("# (1). Maximum height for low rise buildings classification\n")
        f.write(f"building_lowRise_maxHeight=15\n")
        f.write("# (2). Maximum height for mid rise buildings classification\n")
        f.write(f"building_midRise_maxHeight=30\n")
        f.write("# (3). Whether to model all surfaces (e.g., grass, etc.) with buildings\n")
        f.write(f"model_all_surface_types_alongside_building=Yes\n")

        f.write("\n##### Inputs to model ground temperature for building energy modeling ####\n")
        f.write("# (1). Average site annual outdoor air temperature (degree C)\n")
        f.write(f"ave_annual_outdoor_air_temperature=8\n")
        f.write("# (2). Surface temperature amplitude [K]. Normal range: 5–20 K amplitude\n")
        f.write(f"surface_temperature_amplitude=15\n")
        f.write("# (3). Soil damping depth [m]. Normal range: 0.5–5.0 m amping depth\n")
        f.write(f"soil_damping_depth=2\n")
        f.write("# (4). Phase offset (time of max surface temperature) [days]. It happens in summer time in the North Hemisphere\n")
        f.write(f"day_of_max_surface_temperature=200\n")
        f.write("# (5). Ground thermal resistance [m2⋅K/W]. Normal range: 0.5 to 3.5 m2⋅K/W\n")
        f.write(f"ground_thermal_resistance=1.5\n")

        f.write("\n###### Settings for printing outputs when the building energy modeling is enabled #########\n")
        f.write("# (1). Building general property\n")
        f.write(f"building_property_general=OFF\n")
        f.write("# (2). Building envelope property\n")
        f.write(f"building_property_envelope=OFF\n")
        f.write("# (3). Building HVAC-related property\n")
        f.write(f"building_HVAC_related_property=OFF\n")
        f.write("# (4). Building average indoor thermal zone data (the average of thermal zones will be printed)\n")
        f.write(f"building_indoor_average_thermal_data=OFF\n")

        f.write("\n###### Settings for printing outputs for debugging #########\n")
        f.write(f"transient_IHG_buildings=OFF\n")
        f.write(f"building_surface_debugging_files=OFF\n")

def write_csv(path, headers, rows):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        if headers:
            writer.writerow(headers)
        writer.writerows(rows)

def generate_surface_files(surface, dir_path):
    os.makedirs(dir_path, exist_ok=True)

    write_txt_settings(surface, os.path.join(dir_path, "CDT_UBEM_settings.txt"))

    # Vegetation Types
    veg_list = surface.get("vegetationTypes", [])
    veg_path = os.path.join(dir_path, "vegetationTypes.csv")

    # Default values to add if missing
    default_vegs = [
        [
            "grass_default", 0.35, 0.38, 0.23, 0.04, 110, 2, 0, 0.03,
            3.00E-04, 10, 10, 0.05, 0.18, 0.95, 1000, 1600
        ],
        [
            "tree_default", 0.26, 0.39, 0.29, 0.06, 500, 5, 0.03, 2,
            2, 20, 15, 0.03, 0.12, 0.97, 1200, 2000
        ]
    ]

    veg_headers = [
        "vegetation_type", "root_fraction_layer_1", "root_fraction_layer_2", "root_fraction_layer_3",
        "root_fraction_layer_4", "minCanopyRes", "leafAreaIndex", "tallVegCorrFac", "momentumRoughLength",
        "heatRoughLength", "thermalCondStable", "thermalCondUnstable", "F_sw_in", "albedo", "emissivity",
        "density", "heatCapacity"
    ]
    if veg_list:
        veg_rows = [
            [v.get("vegetation_type", ""), v.get("root_fraction_layer_1", ""), v.get("root_fraction_layer_2", ""),
             v.get("root_fraction_layer_3", ""), v.get("root_fraction_layer_4", ""), v.get("minCanopyRes", ""),
             v.get("leafAreaIndex", ""), v.get("tallVegCorrFac", ""), v.get("momentumRoughLength", ""),
             v.get("heatRoughLength", ""), v.get("thermalCondStable", ""), v.get("thermalCondUnstable", ""),
             v.get("F_sw_in", ""), v.get("albedo", ""), v.get("emissivity", ""), v.get("density", ""),
             v.get("heatCapacity", "")]
            for v in veg_list
        ]
        veg_types_in_input = set(row[0] for row in veg_rows)
        # Add defaults if missing
        for default_veg in default_vegs:
            if default_veg[0] not in veg_types_in_input:
                veg_rows.append(default_veg)
        write_csv(veg_path, veg_headers, veg_rows)
    else:
        shutil.copy("/home/ec2-user/platform/surface_default_files/vegetationTypes.csv", veg_path)

    # Soil Types
    soil_list = surface.get("soilTypes", [])
    soil_path = os.path.join(dir_path, "soilTypes.csv")

    default_soil = [
        "default", 3.14, -2.342, 1.28, 1.16E-06, 0.439, 0.347, 0.151, 0.01
    ]

    soil_headers = [
        "id", "alpha_vg", "l_vg", "n_vg", "hydraulicConductivity", "satVolSoilMois",
        "fieldCapaVolSoilMois", "wiltPointVolSoilMois", "resVolSoilMois"
    ]

    if soil_list:
        soil_rows = [
            [
                s.get("soilType", ""), s.get("alpha_vg", ""), s.get("l_vg", ""), s.get("n_vg", ""),
                s.get("hydraulicConductivity", ""), s.get("satVolSoilMois", ""), s.get("fieldCapaVolSoilMois", ""),
                s.get("wiltPointVolSoilMois", ""), s.get("resVolSoilMois", "")
            ]
            for s in soil_list
        ]
        soil_ids_in_input = set(row[0] for row in soil_rows)
        if "default" not in soil_ids_in_input:
            soil_rows.append(default_soil)
        write_csv(soil_path, soil_headers, soil_rows)
    else:
        shutil.copy("/home/ec2-user/platform/surface_default_files/soilTypes.csv", soil_path)

    # vegetation and soil assignments
    vsa_rows = [[v.get("SolidName", ""), v.get("VegetationType", ""), v.get("SoilType", "")]
                for v in surface.get("vegSoilAssignments", [])]
    write_csv(os.path.join(dir_path, "solidVegetationSoilSubcategories.csv"),
              ["solid_name", "vegetation_id", "soil_id"], vsa_rows)


    # Road Types and Assignments
    road_list = surface.get("roadTypes", [])
    road_path = os.path.join(dir_path, "roadTypes.csv")

    default_road = [
        "default", 0.1, 0.95, 1.2, 2300, 870
    ]

    road_headers = [
        "id", "albedo", "emissivity", "thermalConductivity", "density", "specificHeatCapacity"
    ]

    if road_list:
        road_rows = [
            [
                r.get("roadType", ""), r.get("albedo", ""), r.get("emissivity", ""), r.get("thermalConductivity", ""),
                r.get("density", ""), r.get("specificHeatCapacity", "")
            ]
            for r in road_list
        ]
        road_ids_in_input = set(row[0] for row in road_rows)
        if "default" not in road_ids_in_input:
            road_rows.append(default_road)
        write_csv(road_path, road_headers, road_rows)
    else:
        shutil.copy("/home/ec2-user/platform/surface_default_files/roadTypes.csv", road_path)


    road_assignments = [[r.get("SolidName", ""), r.get("RoadType", "")]
                        for r in surface.get("roadAssignments", [])]
    write_csv(os.path.join(dir_path, "solidRoadSubcategories.csv"), ["solid_name", "road_id"], road_assignments)


    # water Types and Assignments
    water_list = surface.get("waterTypes", [])
    water_path = os.path.join(dir_path, "waterTypes.csv")

    default_water = [
        "default", 0.06, 0.96, 4190, 1000, 50
    ]

    water_headers = [
        "id", "albedo", "emissivity", "specificHeatCapacity", "density", "depth"
    ]

    if water_list:
        water_rows = [
            [
                w.get("waterType", ""), w.get("albedo", ""), w.get("emissivity", ""), w.get("specificHeatCapacity", ""),
                w.get("density", ""), w.get("depth", "")
            ]
            for w in water_list
        ]
        water_ids_in_input = set(row[0] for row in water_rows)
        if "default" not in water_ids_in_input:
            water_rows.append(default_water)
        write_csv(water_path, water_headers, water_rows)
    else:
        shutil.copy("/home/ec2-user/platform/surface_default_files/waterTypes.csv", water_path)


    water_assignments = [[w.get("SolidName", ""), w.get("WaterType", "")]
                         for w in surface.get("waterAssignments", [])]
    write_csv(os.path.join(dir_path, "solidWaterSubcategories.csv"), ["solid_name", "water_id"], water_assignments)

    # Building Envelope Types and Assignments
    env_path = os.path.join(dir_path, "building_archetype_envelope_property.csv")
    envelope_list = surface.get("envelopeTypes", [])

    # Define the "default" envelope block
    default_envelope = [
        ["envelope_property_name", "default"],
        ["building_usage_type", "default"],
        ["number_of_wall_layers", 1],
        ["number_of_roof_layers", 1],
        ["number_of_floor_layers", 1],
        [
            "startPeriod", "endPeriod", "Uvalue_window(W/m2/K)", "windowSHGC(-)", "windowEmissivity(-)",
            "ThermalConductivity_wall[Wm-1K-1]", "ThermalConductivity_roof[Wm-1K-1]", "ThermalConductivity_floor[Wm-1K-1]",
            "SpecificHeat_wall[Jkg-1K-1]", "SpecificHeat_roof[Jkg-1K-1]", "SpecificHeat_floor[Jkg-1K-1]",
            "Density_wall[kgm-3]", "Density_roof[kgm-3]", "Density_floor[kgm-3]", "Thickness_wall[m]", "Thickness_roof[m]",
            "Thickness_floor[m]", "wallAlbedo(-)", "roofAlbedo(-)", "floorAlbedo(-)", "wallEmissivity(-)",
            "roofEmissivity(-)", "floorEmissivity(-)"
        ],
        [
            1500, 2100, 3.12, 0.8, 0.84, 2.4, 2.4, 2.4, 840, 840, 840, 2400, 2400, 2400,
            0.2, 0.2, 0.2, 0.3, 0.2, 0.7, 0.8, 0.8, 0.8
        ]
    ]

    def envelope_name(et):
        # Handles both new and legacy keys for envelope property name
        return et.get("name", et.get("envelope_property_name", ""))

    if envelope_list:
        found_default = any(envelope_name(et) == "default" for et in envelope_list)
        with open(env_path, "w", newline="") as f:
            writer = csv.writer(f)
            for et in envelope_list:
                writer.writerow(["envelope_property_name", et.get("name", "")])
                writer.writerow(["building_usage_type", et.get("usageType", "")])
                writer.writerow(["number_of_wall_layers", et.get("wallLayers", "")])
                writer.writerow(["number_of_roof_layers", et.get("roofLayers", "")])
                writer.writerow(["number_of_floor_layers", et.get("floorLayers", "")])
                writer.writerow(et.get("headers", []))
                writer.writerows(et.get("rows", []))
                # Add the default only if missing
            if not found_default:
                for row in default_envelope:
                    writer.writerow(row)
    else:
        shutil.copy("/home/ec2-user/platform/surface_default_files/building_archetype_envelope_property.csv", env_path)


    # Define the headers
    bldg_headers = [
        "buildingID",
        "isUsageTypeKnown",
        "usageType",
        "isHeightClassificationKnown",
        "heightClassification",
        "isConstructionYearKnown",
        "constructionYear",
        "scheduleType",
        "envelopePropertyType",
        "thermalZoneModel",
        "hasHVAC",
        "isHVACSystemTypeKnown",
        "heatingSystemType",
        "coolingSystemType",
        "isHotwaterSystemTypeKnown",
        "hotwaterSystemType"
    ]

    bldg_rows = []
    for b in surface.get("envelopeAssignments", []):
        building_id = b.get("buildingID", "")
        usage_type = b.get("usage_type", "")
        construction_year = b.get("construction_year", "")
        envelope_property_type = b.get("envelope_property_type", "")
        envelope_group_name = b.get("envelope_group_name", "")
        envelope_property = envelope_property_type or envelope_group_name  # Use whichever is set


        bldg_rows.append([
        building_id,                       # buildingID
        "yes",                             # isUsageTypeKnown (default)
        usage_type,                        # usageType
        "no",                              # isHeightClassificationKnown (default)
        "lowRise",                         # heightClassification (default)
        "yes",                             # isConstructionYearKnown (default)
        construction_year,                 # constructionYear
        "no",                               # scheduleType (default)
        envelope_property,                 # envelopePropertyType
        "singleZone",                      # thermalZoneModel (default)
        "yes",                             # hasHVAC (default)
        "yes",                             # isHVACSystemTypeKnown (default)
        "electric_baseboard",              # heatingSystemType (default)
        "splitCooler",                     # coolingSystemType (default)
        "yes",                             # isHotwaterSystemTypeKnown (default)
        "electric_boiler"                  # hotwaterSystemType (default)
    ])
        
    write_csv(
        os.path.join(dir_path, "building_info_basic.csv"),
        bldg_headers,
        bldg_rows
    )
    
    # copy empty solids_other_types.csv
    solids_other_types_path = os.path.join(dir_path, "solids_other_types.csv")
    shutil.copy("/home/ec2-user/platform/surface_default_files/solids_other_types.csv", solids_other_types_path)

    # copy building_archetype_general_by_usageType.csv
    general_usage_path = os.path.join(dir_path, "building_archetype_general_by_usageType.csv")
    shutil.copy("/home/ec2-user/platform/surface_default_files/building_archetype_general_by_usageType.csv", general_usage_path)

    # copy building_info_advanced_others.csv
    advanced_others_path = os.path.join(dir_path, "building_info_advanced_others.csv")
    shutil.copy("/home/ec2-user/platform/surface_default_files/building_info_advanced_others.csv", advanced_others_path)

    # copy building_info_advanced_HVAC.csv
    advanced_hvac_path = os.path.join(dir_path, "building_info_advanced_HVAC.csv")
    shutil.copy("/home/ec2-user/platform/surface_default_files/building_info_advanced_HVAC.csv", advanced_hvac_path)

    # copy building_schedules.csv
    schedules_path = os.path.join(dir_path, "building_schedules.csv")
    shutil.copy("/home/ec2-user/platform/surface_default_files/building_schedules.csv", schedules_path)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python generate_surface_inputs.py <bucket> <scenario_id> <project_id>")
        sys.exit(1)

    bucket = sys.argv[1]
    scenario_id = sys.argv[2]
    project_id = sys.argv[3]

    baseDir = "/home/ec2-user/platform/projects"
    work_dir = os.path.join(baseDir, project_id, scenario_id)
    surface_dir = os.path.join(work_dir, "surface_inputs")
    
    os.makedirs(work_dir, exist_ok=True)
    os.makedirs(surface_dir, exist_ok=True)

    local_path = os.path.join(work_dir, "surface.json")
    s3_key = f"Scenarios/{scenario_id}/surface.json"
    download_json(bucket, s3_key, local_path)

    with open(local_path, "r") as f:
        surface = json.load(f)

    generate_surface_files(surface, surface_dir)


    # Copy default config files
    default_config_dir = "/home/ec2-user/platform/surface_default_files/config"
    target_config_dir = os.path.join(work_dir, "config")

    if os.path.exists(default_config_dir):
        shutil.copytree(default_config_dir, target_config_dir, dirs_exist_ok=True)
        print(f"Copied config folder from {default_config_dir} to {target_config_dir}")
    else:
        print(f"Warning: Default config folder not found at {default_config_dir}")

    for filename in os.listdir(surface_dir):
        if filename.endswith(".csv") or filename.endswith(".txt"):
            upload_file(bucket, os.path.join(surface_dir, filename), f"Scenarios/{scenario_id}/surface_inputs/{filename}")


    # Define EC2 paths
    source_weather_path = os.path.join(baseDir, project_id, scenario_id, "weather_spinup.csv")
    target_weather_path = os.path.join(baseDir, project_id, scenario_id, "surface_inputs", "weather.csv")
    
    shutil.copyfile(source_weather_path, target_weather_path)
    print(f"Copied weather_spinup.csv from {source_weather_path} to {target_weather_path}")

    # Copy geometry.stl from project folder to surface_dir
    source_geometry_path = os.path.join(baseDir, project_id, "geometry.stl")
    target_geometry_path = os.path.join(surface_dir, "geometry.stl")

    if os.path.exists(source_geometry_path):
        shutil.copyfile(source_geometry_path, target_geometry_path)
        print(f"Copied geometry.stl from {source_geometry_path} to {target_geometry_path}")
    else:
        print(f"Warning: geometry.stl not found at {source_geometry_path}")

    # Upload to S3
    upload_file(bucket, target_weather_path, f"Scenarios/{scenario_id}/surface_inputs/weather.csv")
    print("Uploaded copied weather.csv to S3 successfully.")
