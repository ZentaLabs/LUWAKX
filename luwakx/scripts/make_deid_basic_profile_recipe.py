import csv
import os
from pydicom.datadict import get_entry

def get_vr_for_tag(tag_str):
    """Get the VR (Value Representation) for a DICOM tag."""
    try:
        # Remove parentheses and split by comma
        tag_str = tag_str.strip('()')
        group_str, elem_str = tag_str.split(',')
            
        # Convert to integers
        group = int(group_str, 16)
        elem = int(elem_str, 16)
            
        # Get VR from pydicom dictionary
        entry = get_entry((group, elem))
        if entry:
            return entry[0]  # VR is the first element in the tuple
    except Exception as e:
        print(f"Could not determine VR for tag {tag_str}: {e}")
    return None

def generate_basic_profile_recipe(input_csv, output_file):
    """
    Generate a deid recipe file from DICOM standard tags CSV.
    
    Args:
        input_csv: Path to the dicom_standard_tags.csv file
        output_file: Output filename for the recipe
    """
    if not os.path.exists(input_csv):
        print(f"Error: Input file {input_csv} not found")
        return
    
    with open(input_csv, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Check if required columns exist
        if 'Tag' not in reader.fieldnames or 'Basic Prof.' not in reader.fieldnames:
            print(f"Error: Required columns 'Tag' and 'Basic Prof.' not found in {input_csv}")
            print(f"Available columns: {reader.fieldnames}")
            return
        
        with open(output_file, 'w') as outfile:
            outfile.write("FORMAT dicom\n\n%header\n\n")
            outfile.write("ADD PatientIdentityRemoved YES\n\n")

            processed_count = {'X': 0, 'K': 0, 'U': 0, 'D': 0, 'Z': 0, 'Z/D': 0, 'X/Z': 0, 'X/D': 0, 'X/Z/D': 0, 'X/Z/U*': 0, 'other_D': 0, 'other_Z': 0}

            for row in reader:
                tag = row['Tag'].strip()
                basic_profile = row['Basic Prof.'].strip()
                
                # Skip empty tags
                if not tag:
                    continue
                
                if basic_profile == 'X':
                    # REMOVE for X values
                    line = f"REMOVE {tag}\n"
                    outfile.write(line)
                    processed_count['X'] += 1
                elif basic_profile == 'K':
                    # KEEP for K values
                    line = f"KEEP {tag}\n"
                    outfile.write(line)
                    processed_count['K'] += 1
                elif basic_profile == 'U':
                    # Check if VR is UI for UID replacement
                    vr = get_vr_for_tag(tag)
                    if vr == 'UI':
                        line = f"REPLACE {tag} func:generate_uid\n"
                        outfile.write(line)
                        processed_count['U'] += 1
                elif basic_profile == 'D':
                    # Check if VR is date/time related for date replacement
                    vr = get_vr_for_tag(tag)
                    if vr in ['DA', 'DT', 'TM']:
                        line = f"REPLACE {tag} func:generate_dummy_datetime\n"
                        outfile.write(line)
                        processed_count['D'] += 1
                    elif vr in ['UI']:
                        # D but UI VR
                        line = f"REPLACE {tag} func:generate_uid\n"
                        outfile.write(line)
                        processed_count['D'] += 1
                    else:
                        # line = f"# TODO: Handle 'D' (non-date/time) for {tag} (VR: {vr})\n"
                        # outfile.write(line)
                        processed_count['other_D'] += 1
                elif basic_profile == 'Z':
                    # Check if VR is date/time related for date replacement
                    vr = get_vr_for_tag(tag)
                    if vr in ['DA', 'DT', 'TM']:
                        line = f"REPLACE {tag} func:generate_dummy_datetime\n"
                        outfile.write(line)
                        processed_count['Z'] += 1
                    else:
                        # Z but not date/time VR
                        line = f"BLANK {tag}\n"
                        outfile.write(line)
                        processed_count['other_Z'] += 1
                elif basic_profile == 'Z/D':
                    # Handle Z/D combination - clean or replace with dummy value
                    vr = get_vr_for_tag(tag)
                    if vr in ['DA', 'DT', 'TM']:
                        # line = f"# TODO: Handle 'Z/D' (date/time) for {tag} (VR: {vr})\n"
                        # outfile.write(line)
                        processed_count['Z/D'] += 1
                    else:
                        # Z/D but not date/time VR
                        # line = f"# TODO: Handle 'Z/D' (non-date/time) for {tag} (VR: {vr})\n"
                        # outfile.write(line)
                        processed_count['Z/D'] += 1
                elif basic_profile == 'X/Z':
                    # Handle X/Z combination - remove or clean
                    line = f"REMOVE {tag}\n"
                    outfile.write(line)
                    processed_count['X/Z'] += 1
                elif basic_profile == 'X/D':
                    # Handle X/D combination - remove or clean
                    line = f"REMOVE {tag}\n"
                    outfile.write(line)
                    processed_count['X/D'] += 1
                elif basic_profile == 'X/Z/D':
                    # Handle X/Z/D combination - remove or clean
                    line = f"REMOVE {tag}\n"
                    outfile.write(line)
                    processed_count['X/Z/D'] += 1
                elif basic_profile == 'X/Z/U*':
                    # Handle X/Z/U* combination - remove or replace UID
                    vr = get_vr_for_tag(tag)
                    if vr == 'UI':
                        line = f"REPLACE {tag} func:generate_uid\n"
                        outfile.write(line)
                        processed_count['X/Z/U*'] += 1
                    else:
                        # Not UI VR, treat as remove
                        line = f"REMOVE {tag}\n"
                        outfile.write(line)
                        processed_count['X/Z/U*'] += 1
    
    print(f"Recipe generated: {output_file}")
    print(f"Statistics:")
    print(f"  REMOVE (X): {processed_count['X']} tags")
    print(f"  KEEP (K): {processed_count['K']} tags")
    print(f"  REPLACE UID (U with VR=UI): {processed_count['U']} tags")
    print(f"  Date/Time D (VR=DA/DT/TM): {processed_count['D']} tags")
    print(f"  Date/Time Z (VR=DA/DT/TM): {processed_count['Z']} tags")
    print(f"  Z/D combination: {processed_count['Z/D']} tags")
    print(f"  X/Z combination (REMOVE): {processed_count['X/Z']} tags")
    print(f"  X/D combination (REMOVE): {processed_count['X/D']} tags")
    print(f"  X/Z/D combination (REMOVE): {processed_count['X/Z/D']} tags")
    print(f"  X/Z/U* combination: {processed_count['X/Z/U*']} tags")
    print(f"  Other D values: {processed_count['other_D']} tags (marked as TODO)")
    print(f"  Other Z values: {processed_count['other_Z']} tags")

if __name__ == "__main__":
    input_csv = "/home/simona/Downloads/dicom_standard_tags.csv"
    output_file = "anonymization_recipes/deid.dicom.basic-profile-2"
    
    print(f"Processing: {input_csv}")
    print(f"Output: {output_file}")
    
    generate_basic_profile_recipe(input_csv, output_file)
