import os
import sys
import json
import re

def parse_stl_solid_counts(file_path):
    solid_counts = {
        "building": 0,
        "highway": 0,
        "grass": 0,
        "ground": 0,
        "waterway": 0,
        "tree": 0
    }

    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if line.startswith("solid"):
                solid_name = line.split(" ", 1)[1]
                for solid_type in solid_counts.keys():
                    if re.match(f"^{solid_type}", solid_name, re.IGNORECASE):
                        solid_counts[solid_type] += 1
                        break

    return solid_counts

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parse_split_stl_summary.py <project_id>")
        sys.exit(1)

    project_id = sys.argv[1]
    base_dir = "/home/ec2-user/platform/projects"
    geometry_file = os.path.join(base_dir, project_id, "geometry_split.stl")
    output_file = os.path.join(base_dir, project_id, "geometry_split_summary.json")

    if not os.path.isfile(geometry_file):
        print(f"Error: geometry_split.stl not found at {geometry_file}")
        sys.exit(1)

    summary = parse_stl_solid_counts(geometry_file)

    with open(output_file, "w") as f:
        json.dump(summary, f)

    print(f"✅ Summary saved to: {output_file}")
