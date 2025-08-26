import csv
import argparse
import os

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

def get_keyword_for_tag(tag_str):
    """Get the keyword for a DICOM tag."""
    try:
        # Remove parentheses and split by comma
        tag_str = tag_str.strip('()')
        group_str, elem_str = tag_str.split(',')
        group = int(group_str, 16)
        elem = int(elem_str, 16)
        entry = get_entry((group, elem))
        if entry:
            return entry[1]  # Keyword is the second element in the tuple
    except Exception as e:
        print(f"Could not determine keyword for tag {tag_str}: {e}")
    return None

def generate_retain_uid_profile_recipe(input_csv, output_path):
    """
    Generate a deid recipe file from DICOM standard tags CSV.
    
    Args:
        input_csv: Path to the dicom_standard_tags.csv file
        output_path: Output path for the recipe
    """
    output_file = output_path+"deid.dicom.retain-uid"
    with open(output_file, 'w') as outfile:
        outfile.write("FORMAT dicom\n\n%header\n\n")

        processed_count = {'others': 0, 'P': 0}

        for row in reader:
            tag = row['TCIA element_sig_pattern'].strip()
            profile = row['Rtn. UIDs Opt.'].strip()
                
            # Set empty tags
            if not tag:
                tag = '(' + row['Group'] + ',' + row['Element'] + ')'
                
            name = row['Name']
            comment = f" # {name}" if name else ""
            if profile == 'K':
                line = f"KEEP {tag}{comment}\n"
                outfile.write(line)
                processed_count['P'] += 1
            else:
                processed_count['others'] += 1
                continue
                    
def generate_retain_patient_characteristics_profile_recipe(input_csv, output_path):
    """
    Generate a deid recipe file from DICOM standard tags CSV.
    
    Args:
        input_csv: Path to the dicom_standard_tags.csv file
        output_path: Output path for the recipe
    """
    output_file = output_path+"deid.dicom.retain-pat-chars"
    with open(output_file, 'w') as outfile:
        outfile.write("FORMAT dicom\n\n%header\n\n")

        processed_count = {'others': 0, 'P': 0}

        for row in reader:
            tag = row['TCIA element_sig_pattern'].strip()
            profile = row['Rtn. Pat. Chars. Opt.'].strip()
                
            # Set empty tags
            if not tag:
                tag = '(' + row['Group'] + ',' + row['Element'] + ')'
                
            name = row['Name']
            comment = f" # {name}" if name else ""
            if profile == 'K':
                line = f"KEEP {tag}{comment}\n"
                outfile.write(line)
                processed_count['P'] += 1
            else:
                processed_count['others'] += 1
                continue

def generate_retain_long_full_dates_profile_recipe(input_csv, output_path):
    """
    Generate a deid recipe file from DICOM standard tags CSV.
    
    Args:
        input_csv: Path to the dicom_standard_tags.csv file
        output_path: Output path for the recipe
    """
    output_file = output_path+"deid.dicom.retain-long-full-dates"
    with open(output_file, 'w') as outfile:
        outfile.write("FORMAT dicom\n\n%header\n\n")

        processed_count = {'others': 0, 'P': 0}

        for row in reader:
            tag = row['TCIA element_sig_pattern'].strip()
            profile = row['Rtn. Long. Full Dates Opt.'].strip()
                
            # Set empty tags
            if not tag:
                tag = '(' + row['Group'] + ',' + row['Element'] + ')'
                
            name = row['Name']
            comment = f" # {name}" if name else ""
            if profile == 'K':
                line = f"KEEP {tag}{comment}\n"
                outfile.write(line)
                processed_count['P'] += 1
            else:
                processed_count['others'] += 1
                continue

def generate_retain_long_modified_dates_profile_recipe(input_csv, output_path):
    """
    Generate a deid recipe file from DICOM standard tags CSV.
    
    Args:
        input_csv: Path to the dicom_standard_tags.csv file
        output_path: Output path for the recipe
    """
    output_file = output_path+"deid.dicom.retain-long-modified-dates"
    with open(output_file, 'w') as outfile:
        outfile.write("FORMAT dicom\n\n%header\n\n")

        processed_count = {'others': 0, 'P': 0}

        for row in reader:
            tag = row['TCIA element_sig_pattern'].strip()
            profile = row['Rtn. Long. Modif. Dates Opt.'].strip()
                
            # Set empty tags
            if not tag:
                tag = '(' + row['Group'] + ',' + row['Element'] + ')'
                
            name = row['Name']
            comment = f" # {name}" if name else ""
            if profile == 'C':
                line = f"JITTER {tag} func:hash_increment_date\n"
                outfile.write(line)
                processed_count['P'] += 1
            else:
                processed_count['others'] += 1
                continue
                        

def generate_other_profile_recipe(input_csv, output_file, profile='retain-long-modified-dates'):
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

        if profile == 'retain-uid':
            generate_retain_uid_profile_recipe(input_csv, output_path)
            return # Exit after generating retain-uid profile
        elif profile == 'retain-pat-chars':
            generate_retain_patient_characteristics_profile_recipe(input_csv, output_path)
            return # Exit after generating retain-pat-chars profile
        elif profile == 'retain-long-full-dates':
            generate_retain_long_full_dates_profile_recipe(input_csv, output_path)
            return # Exit after generating retain-long-full-dates profile
        elif profile == 'retain-long-modified-dates':
            generate_retain_long_modified_dates_profile_recipe(input_csv, output_path)
            return # Exit after generating retain-long-modified-dates profile

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate deid basic profile recipe from CSV.")
    parser.add_argument('--input', type=str, default="path_to_file/merged_standard_tags.csv", help='Input CSV file (default: path_to_file/merged_standard_tags.csv)')
    parser.add_argument('--output', type=str, default="anonymization_recipes/", help='Output path for recipe file (default: anonymization_recipes/)')
    parser.add_argument('--profile', type=str, default="retain-long-modified-dates", help='Profile type (default: retain-long-modified-dates)')
    args = parser.parse_args()

    print(f"Processing: {args.input}")
    print(f"Output: {args.output}")
    generate_other_profile_recipe(args.input, args.output, args.profile)
