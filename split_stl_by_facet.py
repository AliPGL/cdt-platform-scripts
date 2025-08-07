import sys
import os

def split_stl_by_facet(input_file_path, output_file_path):
    with open(input_file_path, 'r') as file:
        lines = file.readlines()

    output_lines = []
    current_solid = ""
    solid_index = 0
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        if line.startswith("solid"):
            current_solid = line.split(" ", 1)[1]
            solid_index = 0  # ✅ Reset index for new solid
            i += 1
            continue

        elif line.startswith("facet normal"):
            facet_lines = [f"solid {current_solid}_{solid_index}\n"]

            while not lines[i].strip().startswith("endfacet"):
                facet_lines.append(lines[i])
                i += 1
            facet_lines.append(lines[i])  # endfacet
            i += 1

            facet_lines.append(f"endsolid {current_solid}_{solid_index}\n")
            output_lines.extend(facet_lines)
            solid_index += 1

        elif line.startswith("endsolid"):
            i += 1
        else:
            i += 1

    with open(output_file_path, 'w') as out_file:
        out_file.writelines(output_lines)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python split_stl_by_facet.py <project_id>")
        sys.exit(1)

    project_id = sys.argv[1]
    base_dir = "/home/ec2-user/platform/projects"
    input_path = os.path.join(base_dir, project_id, "geometry.stl")
    output_path = os.path.join(base_dir, project_id, "geometry_split.stl")

    if not os.path.isfile(input_path):
        print(f"Error: Input file '{input_path}' does not exist.")
        sys.exit(1)

    split_stl_by_facet(input_path, output_path)
    print(f"✅ geometry_split.stl written to: {output_path}")
