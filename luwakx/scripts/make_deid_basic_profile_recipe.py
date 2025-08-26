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
        if 'TCIA element_sig_pattern' not in reader.fieldnames or 'Basic Prof.' not in reader.fieldnames or 'Final CTP Script' not in reader.fieldnames:
            print(f"Error: Required columns 'TCIA element_sig_pattern', 'Basic Prof.', and 'Final CTP Script' not found in {input_csv}")
            print(f"Available columns: {reader.fieldnames}")
            return
        
        with open(output_file, 'w') as outfile:
            outfile.write("FORMAT dicom\n\n%header\n\n")
            outfile.write("ADD PatientIdentityRemoved YES\n\n")

            # processed_count = {'others': 0, 'X': 0, 'K': 0, 'U': 0, 'other_U': 0, 'other_X': 0, 'D': 0, 'Z': 0, 'other_D': 0, 'other_Z': 0}
            # All processed_count usage is commented out below for clarity.

            for row in reader:
                tag = row['TCIA element_sig_pattern'].strip()
                basic_profile = row['Basic Prof.'].strip()
                
                # Set empty tags
                if not tag:
                    tag = '(' + row['Group'] + ',' + row['Element'] + ')'
                
                name = row['Name']
                comment = f" # {name}" if name else ""
                # if basic_profile == '':
                #     processed_count['others'] += 1
                #     continue
                if basic_profile == '':
                    continue
                if basic_profile == 'X':
                    if name == "Curve Data":
                        keyword = 'CurveData'
                        line = f"REMOVE {keyword}{comment}\n"
                    elif name == "Overlay Data":
                        keyword = 'OverlayData'
                        line = f"REMOVE {keyword}{comment}\n"
                    elif name == "Overlay Comments":
                        keyword = 'OverlayComments'
                        line = f"REMOVE {keyword}{comment}\n"
                    elif name == "Digital Signatures Sequence":
                        keyword = 'DigitalSignaturesSequence'
                        line = f"REMOVE {keyword}{comment}\n"
                    elif name == "Data Set Trailing Padding":
                        keyword = 'DataSetTrailingPadding'
                        line = f"REMOVE {keyword}{comment}\n"
                    elif name == "Private Attributes":
                        line = f"REMOVE ALL func:is_tag_private\n"
                    else:
                        line = f"REMOVE {tag}{comment}\n"
                    outfile.write(line)
                    # processed_count['X'] += 1
                elif basic_profile == 'K':
                    # KEEP for K values
                    line = f"KEEP {tag}{comment}\n"
                    outfile.write(line)
                    # processed_count['K'] += 1
                elif basic_profile == 'U':
                    # Check if VR is UI for UID replacement
                    vr = get_vr_for_tag(tag)
                    if vr == 'UI':
                        line = f"REPLACE {tag} func:generate_hashuid\n"
                        outfile.write(line)
                        # processed_count['U'] += 1
                    else:
                        # processed_count['other_U'] += 1
                        pass
                elif basic_profile == 'D':
                    # Check if VR is date/time related for date replacement
                    vr = get_vr_for_tag(tag)
                    if vr in ['DA', 'DT', 'TM']:
                        line = f"REPLACE {tag} func:set_fixed_datetime\n"
                        outfile.write(line)
                        # processed_count['D'] += 1
                    elif vr in ['UI']:
                        # D but UI VR
                        line = f"REPLACE {tag} func:generate_hashuid\n"
                        outfile.write(line)
                        # processed_count['U'] += 1
                    elif vr in ["AE", "LO", "LT", "SH", "PN", "CS", "ST", "UT", "UC", "UR"]:
                        line = f"REPLACE {tag} ANONYMIZED{comment}\n"
                        outfile.write(line)
                        # processed_count['D'] += 1
                    elif vr == "UN":
                        line = f"REPLACE {tag} b'Anonymized'{comment}\n"
                        outfile.write(line)
                        # processed_count['D'] += 1
                    elif vr in ["DS", "IS", "FD", "FL", "SS", "US", "SL", "UL"]:
                        line = f"REPLACE {tag} 0{comment} NEED to BE REVIEWED\n"
                        outfile.write(line)
                        # processed_count['D'] += 1
                    elif vr in ['OD', 'OF', 'OL', 'OV', 'SV', 'UV']:
                        line = f"BLANK {tag}{comment} NEED to BE REVIEWED\n"
                        outfile.write(line)
                        # processed_count['other_Z'] += 1
                    elif vr == 'AS':
                        line = f"REPLACE {tag} 030Y{comment} NEED to BE REVIEWED\n"
                        outfile.write(line)
                        # processed_count['D'] += 1
                    elif vr in ['SQ', 'OB']:
                        # processed_count['other_D'] += 1
                        pass
                    else:
                        # processed_count['other_D'] += 1
                        print(f"Tag {tag} with Basic Profile 'D' has unhandled VR '{vr}'. Marked as TODO.")
                elif basic_profile == 'Z':
                    # Check if VR is date/time related for date replacement
                    vr = get_vr_for_tag(tag)
                    if vr in ['DA', 'DT', 'TM']:
                        line = f"REPLACE {tag} func:set_fixed_datetime\n"
                        outfile.write(line)
                        # processed_count['Z'] += 1
                    else:
                        # Z but not date/time VR
                        line = f"BLANK {tag}{comment}\n"
                        outfile.write(line)
                        # processed_count['other_Z'] += 1
                elif basic_profile in ['Z/D','X/Z','X/D','X/Z/D','X/Z/U*']:
                    if row['Final CTP Script']== "@keep()":
                        if basic_profile == 'Z/D':
                            line = f"BLANK {tag}{comment} retained by TCIA profile, arbitrarily chose to have Z. MUST BE REVIEWED \n"
                            outfile.write(line)
                            # processed_count['other_Z'] += 1
                        else:
                            line = f"REMOVE {tag}{comment} retained by TCIA profile, arbitrarily chose to have X. MUST BE REVIEWED \n"
                            outfile.write(line)
                            # processed_count['other_X'] += 1
                    else:
                        if row['Final CTP Script']=="@hashuid(@UIDROOT,this)":
                            line = f"REPLACE {tag} func:generate_hashuid\n"
                            outfile.write(line)
                            # processed_count['other_U'] += 1
                        elif row['Final CTP Script']=="@incrementdate(this,@DATEINC)":
                            line = f"JITTER {tag} func:hash_increment_date\n"
                            outfile.write(line)
                            # processed_count['other_D'] += 1
                        elif row['Final CTP Script']=="@empty()":
                            line = f"BLANK {tag}{comment}\n"
                            outfile.write(line)
                            # processed_count['other_Z'] += 1
                        elif row['Final CTP Script']=="@remove()":
                            line = f"REMOVE {tag}{comment}\n"
                            outfile.write(line)
                            # processed_count['other_X'] += 1
                        elif row['Final CTP Script']=="@process()":
                            if vr == 'UI':
                                line = f"REPLACE {tag} func:generate_hashuid\n"
                                outfile.write(line)
                                # processed_count['other_U'] += 1
                                print(f"Tag {tag} with Basic Profile '{basic_profile}' and Final CTP Script '@process()' treated as REPLACE UID since VR is 'UI'.")
                            else:
                                # Not UI VR, treat as remove
                                line = f"REMOVE {tag}{comment}\n"
                                outfile.write(line)
                                # processed_count['other_X'] += 1
                                print(f"Tag {tag} with Basic Profile '{basic_profile}' and Final CTP Script '@process()' treated as REMOVE since VR is '{vr}'.")
                        else:
                            # processed_count['others'] += 1
                            pass
                else:
                    # processed_count['others'] += 1
                    pass
    
    print(f"Recipe generated: {output_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate deid basic profile recipe from CSV.")
    parser.add_argument('--input', type=str, default="path_to_file/merged_standard_tags.csv", help='Input CSV file (default: path_to_file/merged_standard_tags.csv)')
    parser.add_argument('--output', type=str, default="anonymization_recipes/deid.dicom.basic-profile", help='Output recipe file (default: anonymization_recipes/deid.dicom.basic-profile)')
    args = parser.parse_args()

    print(f"Processing: {args.input}")
    print(f"Output: {args.output}")
    generate_basic_profile_recipe(args.input, args.output)
