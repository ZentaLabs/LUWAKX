"""Recipe builder for DICOM anonymization.

This module provides functionality to generate DEID recipe files based on 
anonymization requirements and configurations. It extracts the original
recipe building logic from anonymize.py to maintain high cohesion.

Key Components:
- make_recipe_file: Main function to generate DEID recipe files from templates
- _collect_actions_for_row: Helper function to process template rows (simplified)
"""

import os
import csv
import struct
from typing import List, Optional

from luwak_logger import get_logger


def make_recipe_file(recipes_to_process: List[str], recipe_folder: str, config: Optional[dict] = None) -> Optional[str]:
    """Generate a deid recipe file from standard_tags_template.csv and private_tags_template.csv 
    based on selected recipes.

    Args:
        recipes_to_process: List of recipe names to process (e.g., ['basic_profile', 'retain_uid'])
        recipe_folder: Path to the folder where the recipe file will be saved
        config: Optional configuration dictionary containing manuallyRevisedTags paths
    
    Returns:
        str: Path to the generated recipe file, or None if generation fails
    """
    logger = get_logger("make_recipe_file")
    logger.info(f"Generating recipe file for profiles: {recipes_to_process}")
    logger.debug(f"Recipe output folder: {recipe_folder}")
    
    # Default template paths
    input_standard_template = os.path.join(os.path.dirname(__file__), "data", "TagsArchive", "standard_tags_template.csv")
    input_private_template = os.path.join(os.path.dirname(__file__), "data", "TagsArchive", "private_tags_template.csv")

    if config and 'manuallyRevisedTags' in config:
        manually_revised = config['manuallyRevisedTags']
        
        # Use custom standard tags path if provided
        if 'standard' in manually_revised and manually_revised['standard']:
            custom_standard_path = manually_revised['standard']
            if os.path.exists(custom_standard_path):
                input_standard_template = custom_standard_path
                logger.info(f"Using manually revised standard tags from: {custom_standard_path}")
            else:
                logger.warning(f"Manually revised standard tags file not found: {custom_standard_path}, using default")
        
        # Use custom private tags path if provided
        if 'private' in manually_revised and manually_revised['private']:
            custom_private_path = manually_revised['private']
            if os.path.exists(custom_private_path):
                input_private_template = custom_private_path
                logger.info(f"Using manually revised private tags from: {custom_private_path}")
            else:
                logger.warning(f"Manually revised private tags file not found: {custom_private_path}, using default")

    # Map recipe names to column names in the CSV (original mapping)
    recipe_column_map = {
        'basic_profile': 'Basic Prof.',
        'retain_uid': 'Rtn. UIDs Opt.',
        'retain_device_id': 'Rtn. Dev. Id. Opt.',
        'retain_institution_id': 'Rtn. Inst. Id. Opt.',
        'retain_patient_chars': 'Rtn. Pat. Chars. Opt.',
        'retain_long_full_dates': 'Rtn. Long. Full Dates Opt.',
        'retain_long_modified_dates': 'Rtn. Long. Modif. Dates Opt.',
        'clean_descriptors': 'Clean Desc. Opt.',
        'clean_structured_content': 'Clean Struct. Cont. Opt.',
        'clean_graphics': 'Clean Graph. Opt.'
    }

    if not os.path.exists(input_standard_template):
        logger.error(f"Input file {input_standard_template} not found")
        return None

    if not os.path.exists(input_private_template):
        logger.error(f"Input file {input_private_template} not found")
        return None

    # Create recipe folder if it doesn't exist
    os.makedirs(recipe_folder, exist_ok=True)
    
    # Output recipe file path
    output_file = os.path.join(recipe_folder, "deid.dicom.recipe")

    with open(output_file, 'w') as outfile:
        outfile.write("FORMAT dicom\n\n%header\n\n")
        with open(input_standard_template, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Support nested sequence tag syntax in standard tag CSV only
                group_val = row['Group']
                elem_val = row['Element']
                if '__' in group_val and '__' in elem_val:
                    # Parse nested tag syntax: aaaa__0__cccc, bbbb__0__dddd
                    group_parts = group_val.split('__')
                    elem_parts = elem_val.split('__')
                    if len(group_parts) == len(elem_parts):
                        # Build tag string: (aaaa,bbbb)__0__(cccc,dddd)...
                        tag_segments = []
                        for g, e in zip(group_parts, elem_parts):
                            g_clean = g.strip().upper()
                            e_clean = e.strip().upper()
                            # If both are a single integer (e.g., '0'), output as just the number
                            if len(g_clean) == 1 and g_clean.isdigit() and len(e_clean) == 1 and e_clean.isdigit() and g_clean == e_clean:
                                tag_segments.append(g_clean)
                            else:
                                tag_segments.append(f"({g_clean},{e_clean})")
                        tag = '__'.join(tag_segments)
                    else:
                        logger.warning(f"Malformed nested tag syntax: Group={group_val}, Element={elem_val}, it will be skipped.")
                else:
                    # Fallback to default if mismatch
                    tag = (f"({group_val},{elem_val})").upper()
                name = row['Name']
                comment = f" # {name}" if name else ""
                vr = row['VR']
                # Collect actions from only the requested recipe columns
                actions = _collect_actions_for_row(row, recipes_to_process, recipe_column_map)
                
                # Skip if no actions found
                if not actions:
                    continue
                
                # Determine final action based on priority rules (original logic)
                final_action = _determine_final_action(actions, vr)
                
                # Write action based on the final determined action (original logic)
                line = f"{comment}\n"
                outfile.write(line)
                if final_action == 'keep':
                    line = f"KEEP {tag}\n"
                elif final_action == 'remove':
                    line = f"REMOVE {tag}\n"
                elif final_action == 'blank':
                    if vr in ['OD', 'OL', 'OV', 'SV', 'UV', 'DS', 'IS', 'FD', 
                                'FL', 'SS', 'US', 'SL', 'UL']:
                        line = f"REPLACE {tag} {set_empty_value(vr)}\n"
                    else:
                        line = f"BLANK {tag}\n"
                elif final_action == 'replace':
                    if  vr in ["AE", "LO", "LT", "SH", "CS", "ST", "UT", "UC", "UR"]:
                        line = f"REPLACE {tag} ANONYMIZED\n"
                    elif vr == "PN":
                        line = f"REPLACE {tag} Anonymized^Anonymized\n"
                    elif vr in ['OD', 'OL', 'OV', 'SV', 'UV', 'DS', 'IS', 'FD', 
                                'FL', 'SS', 'US', 'SL', 'UL']:
                        line = f"REPLACE {tag} {set_values_to_zero(vr)}\n"
                    elif vr == 'AS':
                        line = f"REPLACE {tag} 000D\n"
                    elif vr in ['SQ']:
                        line = f"#REPLACE {tag} NEED to BE REVIEWED\n"
                elif final_action == 'func:generate_hashuid':
                    line = f"REPLACE {tag} func:generate_hashuid\n"
                elif final_action == 'func:set_fixed_datetime':
                    line = f"REPLACE {tag} func:set_fixed_datetime\n"
                elif final_action == 'func:hash_increment_date':
                    line = f"JITTER {tag} func:hash_increment_date\n"
                elif final_action == 'func:clean_descriptors_with_llm':
                    line = f"REPLACE {tag} func:clean_descriptors_with_llm\n"
                elif final_action == 'clean_manually':
                    line = f"# REPLACE {tag} CLEANED NEEDS MANUAL REVIEW\n"
                elif final_action == 'manual_review':
                    line = f"# REPLACE {tag} MANUAL REVIEW NEEDED\n"
                outfile.write(line)
        
        # Add PatientIdentityRemoved if basic_profile is in the recipe list (original logic)
        if 'basic_profile' in recipes_to_process:
            outfile.write("ADD PatientIdentityRemoved YES\n")
            # Remove all curve data/overlay data/overlay comments tags
            # TOCHECK : are these tags supposed to be removed always and
            # not only if the basic profile is requested? if yes move the line below outside if block
            outfile.write(f"REMOVE ALL func:is_curve_or_overlay_tag\n")
            # Set DeidentificationMethod based on examples from RSNA anonymizer:
            # ds.DeidentificationMethod = "RSNA DICOM ANONYMIZER"  # (0012,0063)
            outfile.write("ADD DeidentificationMethod LUWAK_ANONYMIZER\n")
            if 'retain_long_full_dates' not in recipes_to_process and 'retain_long_modified_dates' not in recipes_to_process:
                outfile.write("ADD LongitudinalTemporalInformationModified REMOVED\n")
        if 'retain_long_full_dates' in recipes_to_process:
            outfile.write("ADD LongitudinalTemporalInformationModified UNMODIFIED\n")
        elif 'retain_long_modified_dates' in recipes_to_process:
            outfile.write("ADD LongitudinalTemporalInformationModified MODIFIED\n")
        if 'clean_recognizable_visual_features' in recipes_to_process:
            outfile.write("ADD RecognizableVisualFeatures NO\n")
        
        # Handle private tags
        if 'retain_safe_private_tags' in recipes_to_process:
            with open(input_private_template, 'r') as privfile:
                privreader = csv.DictReader(privfile)
                for row in privreader:
                    private_creator = row['Private Creator']
                    group = row['Group'].upper()
                    element = row['Element'][-2:].upper()  # Last two hex digits
                    name = row['Meaning']
                    comment = f" # {name}" if name else ""
                    line = f"{comment}\n"
                    outfile.write(line)
                    action = row['Rtn. Safe Priv. Opt.']
                    if action.lower() == 'keep': 
                        # For safe private tags, we keep them
                        line = f"KEEP ({group},\"{private_creator}\",{element})\n"
                    elif action.lower() == 'func:generate_hashuid':
                        line = f"REPLACE ({group},\"{private_creator}\",{element}) func:generate_hashuid\n"
                    elif action.lower() == 'func:hash_increment_date':
                        # TODO: Check if it is better to remove these in case the retain long modified dates is not selected
                        line = f"JITTER ({group},\"{private_creator}\",{element}) func:hash_increment_date\n"
                    outfile.write(line)

        # Add the final line to remove all other private tags
        line = f"REMOVE ALL func:is_tag_private\n"
        outfile.write(line)

    logger.info(f"Recipe generated: {output_file}")
    return output_file


def _determine_final_action(actions, vr):
    """Determine the final action based on priority rules.
    
    Args:
        actions: List of actions from CSV columns
        vr: DICOM Value Representation (VR) of the tag
        
    Returns:
        str: The final action to apply based on priority
    """
    # If any action is 'keep', final action is 'keep'
    if 'keep' in actions:
        return 'keep'
    elif 'func:hash_increment_date' in actions:
        return 'func:hash_increment_date'
    elif 'func:generate_hashuid' in actions:
        return 'func:generate_hashuid'
    elif 'func:clean_descriptors_with_llm' in actions:
        if vr == 'SQ':
            # For sequences, we need manual review
            return 'manual_review'
        elif vr in ['OB', 'OW', 'OF', 'UN']:
            return 'remove'
        else:
            return 'func:clean_descriptors_with_llm'
    elif 'replace' in actions:
        return 'replace'
    elif 'func:set_fixed_datetime' in actions:
        return 'func:set_fixed_datetime'
    elif 'blank' in actions:
        return 'blank'
    elif 'remove' in actions:
        return 'remove'
    # Otherwise, take the first non-empty action from the priority order
    else:
        return actions[0]


def _collect_actions_for_row(row, recipes_to_process, recipe_column_map):
    """Helper function to collect actions from recipe columns for a given row.
    
    Args:
        row: CSV row dictionary
        recipes_to_process: List of recipe names to process
        recipe_column_map: Dictionary mapping recipe names to CSV column names
    
    Returns:
        list: List of non-empty actions from the requested recipe columns
    """
    actions = []
    for recipe in recipes_to_process:
        if recipe not in recipe_column_map:
            continue
            
        column_name = recipe_column_map[recipe]
        action = row[column_name].strip() if row[column_name] else ""
        
        if action:  # Only add non-empty actions
            actions.append(action)
    
    return actions

def set_values_to_zero(vr):
    """
    Return the correct zero value for the given DICOM VR type.
    Based on DICOM PS3.5 Section 6.2, Table 6.2-1.
    """
    # Text VRs - return string zeros
    if vr in ['DS', 'IS']:  # Decimal String
        return "0"
    # Binary VRs - return binary zeros
    elif vr in ['OD', 'FD']:  # 64-bit IEEE 754 double
        return struct.pack('<d', 0.0)
    elif vr == 'FL':  # 32-bit IEEE 754 float
        return struct.pack('<f', 0.0)
    elif vr in ['OL', 'UL']:  # 32-bit unsigned int
        return struct.pack('<L', 0)
    elif vr in ['OV', 'UV']:  # 64-bit unsigned int
        return struct.pack('<Q', 0)
    elif vr == 'SV':  # 64-bit signed int
        return struct.pack('<q', 0)
    elif vr == 'SL':  # 32-bit signed int
        return struct.pack('<l', 0)
    elif vr == 'SS':  # 16-bit signed int
        return struct.pack('<h', 0)
    elif vr == 'US':  # 16-bit unsigned int
        return struct.pack('<H', 0)
    else:
        return b''  # fallback: empty bytes
    
def set_empty_value(vr):
    # Text VRs
    if vr in ['DS', 'IS']:
        return ""
    # Binary VRs
    elif vr in ['OD', 'OL', 'OV', 'SV', 'UV', 'FD', 'FL', 'SS', 'US', 'SL', 'UL']:
        return b''
