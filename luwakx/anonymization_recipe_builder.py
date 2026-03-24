"""Recipe builder for DICOM anonymization.

This module provides functionality to generate DEID recipe files based on 
anonymization requirements and configurations. It extracts the original
recipe building logic from anonymize.py to maintain high cohesion.

Key Components:
- make_recipe_file: Main function to generate DEID recipe files from templates
- _collect_actions_for_row: Helper function to process template rows (simplified)

See conformance documentation for details:
https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#6-deidentification-recipe-creation-pipeline-stage-4---5
"""

import os
import csv
import re
import struct
from typing import List, Optional

from luwak_logger import get_logger


# Map recipe names to their label prefix in the "Documentation References" column
_RECIPE_TO_RATIONALE_LABEL = {
    'basic_profile': 'Basic',
    'retain_uid': 'Retain UIDs',
    'retain_device_id': 'Retain Device ID',
    'retain_institution_id': 'Retain Institution ID',
    'retain_patient_chars': 'Retain Patient Characteristics',
    'retain_long_full_dates': 'Retain Full Dates',
    'retain_long_modified_dates': 'Retain Modified Dates',
    'clean_descriptors': 'Clean Descriptors',
    'clean_structured_content': 'Clean Structured Content',
    'clean_graphics': 'Clean Graphics',
}


def _find_contributing_recipe(row, final_action, recipes_to_process, recipe_column_map):
    """Return the first recipe in recipes_to_process whose column value equals final_action."""
    for recipe in recipes_to_process:
        if recipe not in recipe_column_map:
            continue
        col = recipe_column_map[recipe]
        if row.get(col, '').strip() == final_action:
            return recipe
    return None


def _extract_rationale_for_label(doc_refs, label):
    """Return the text after 'label:' in the pipe-separated Documentation References string."""
    for part in doc_refs.split('|'):
        part = part.strip()
        if part.lower().startswith(label.lower() + ':'):
            return part[len(label) + 1:].strip()
    return ''


def _lookup_tag_by_keyword(keyword):
    """Resolve a DICOM keyword to (tag_str, tag_name) using pydicom.

    Returns ('(GGGG,EEEE)', tag_name) when the keyword is known, or
    ('*', keyword) when it is not found or pydicom is unavailable.
    """
    try:
        from pydicom.datadict import tag_for_keyword, get_entry
        tag_int = tag_for_keyword(keyword)
        if tag_int is not None:
            entry = get_entry(tag_int)
            tag_str = f"({(tag_int >> 16):04X},{(tag_int & 0xFFFF):04X})"
            tag_name = entry[2]  # human-readable name
            return tag_str, tag_name
    except Exception:
        pass
    return '*', keyword


def _parse_action_from_recipe_line(line):
    """Extract the recipe action keyword (KEEP, REMOVE, REPLACE, BLANK, JITTER) from a recipe line.
    
    For commented-out lines (clean_manually / manual_review) the leading '#' is stripped
    before matching so the intended directive is still returned.
    """
    stripped = line.strip().lstrip('#').strip()
    m = re.match(r'^(KEEP|REMOVE|REPLACE|BLANK|JITTER)\b', stripped)
    return m.group(1) if m else ''


def _parse_replacement_from_recipe_line(line):
    """Extract replacement value from a REPLACE or JITTER recipe line.

    The tag field may contain spaces when a private creator string is used,
    e.g. (0009,"FDMS 1.0",05).  Using \\S+ to skip the tag would stop at
    the first space inside the creator, corrupting the captured replacement
    value.  Instead we match the tag as everything inside the outermost
    (...) pair (non-greedy, stops at first ')'), followed by optional
    nested __N__(...) segments, before consuming the replacement value.
    """
    m = re.match(
        r'(?:REPLACE|JITTER)\s+(?:\(.*?\)(?:__\w+__\(.*?\))*|ALL)\s+(.*)',
        line.strip(),
    )
    return m.group(1).strip() if m else ''


def make_recipe_file(recipes_to_process: List[str], recipe_folder: str, config: Optional[dict] = None) -> Optional[str]:
    """Generate a deid recipe file from standard_tags_template.csv and private_tags_template.csv 
    based on selected recipes.

    Args:
        recipes_to_process: List of recipe names to process (e.g., ['basic_profile', 'retain_uid'])
        recipe_folder: Path to the folder where the recipe file will be saved
        config: Optional configuration dictionary containing customTags paths
    
    Returns:
        str: Path to the generated recipe file, or None if generation fails
    
    See conformance documentation:
    - Recipe generation: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#61-recipe-builder-overview
    - Action translation: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
    """
    logger = get_logger("make_recipe_file")
    logger.info(f"Generating recipe file for profiles: {recipes_to_process}")
    logger.debug(f"Recipe output folder: {recipe_folder}")
    
    # Default template paths
    input_standard_template = os.path.join(os.path.dirname(__file__), "data", "TagsArchive", "standard_tags_template.csv")
    input_private_template = os.path.join(os.path.dirname(__file__), "data", "TagsArchive", "private_tags_template.csv")

    # Custom tag templates can override defaults
    # See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#55-custom-tag-templates
    # See configuration documentation: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#912-optional-configuration-options
    if config and 'customTags' in config:
        manually_revised = config['customTags']
        
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
    # See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#62-deidentification-profiles
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
    output_csv = os.path.join(recipe_folder, "deid.dicom.recipe.csv")

    # Set remove_private variable for later, it will be true if the basic profile is requested
    remove_private = False 

    with open(output_file, 'w') as outfile, open(output_csv, 'w', newline='', encoding='utf-8') as csv_out:
        csv_writer = csv.DictWriter(csv_out, fieldnames=['Tag', 'Tag Name', 'Action', 'Replacement Value', 'Rationale'])
        csv_writer.writeheader()
        outfile.write("FORMAT dicom\n\n%header\n\n")
        with open(input_standard_template, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Support nested sequence tag syntax in standard tag CSV only
                # See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#514-nested-sequence-support
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
                
                # Determine final action based on priority rules (original logic).
                # source_action is the raw CSV value that won the priority check;
                # it may differ from final_action when func:clean_descriptors_with_llm
                # is translated to 'remove' or 'manual_review' based on VR.
                final_action, source_action = _determine_final_action(actions, vr)
                
                # Write action based on the final determined action (original logic)
                line = f"{comment}\n"
                outfile.write(line)
                if final_action == 'keep':
                    # See keep action: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    line = f"KEEP {tag}\n"
                elif final_action == 'remove':
                    # See remove action: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    if name.lower() == 'private attributes':
                        remove_private = True
                    else:
                        line = f"REMOVE {tag}\n"
                elif final_action == 'blank':
                    # See blank action: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    # See blank implementation: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#632-blank-action---empty-value-generation
                    if vr in ['OD', 'OL', 'OV', 'SV', 'UV', 'DS', 'IS', 'FD', 
                                'FL', 'SS', 'US', 'SL', 'UL']:
                        line = f"REPLACE {tag} {set_empty_value(vr)}\n"
                    else:
                        line = f"BLANK {tag}\n"
                elif final_action == 'replace':
                    # See replace action: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    # See replace implementation: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#631-replace-action---dummy-value-generation
                    if  vr in ["AE", "LO", "LT", "SH", "CS", "ST", "UT", "UC", "UR"]:
                        line = f"REPLACE {tag} ANONYMIZED\n"
                    elif vr == "PN":
                        line = f"REPLACE {tag} Anonymized^Anonymized\n"
                    elif vr in ['OD', 'OL', 'OV', 'SV', 'UV', 'DS', 'IS', 'FD', 
                                'FL', 'SS', 'US', 'SL', 'UL']:
                        line = f"REPLACE {tag} {set_values_to_zero(vr)}\n"
                    elif vr == 'AS':
                        line = f"REPLACE {tag} 000D\n"
                    else:
                        logger.warning(f"Tag {tag} with VR={vr} requires custom template/manual review for replacement.")
                elif final_action == 'func:sq_keep_original_with_review':
                    # See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    line = f"REPLACE {tag} func:sq_keep_original_with_review\n"
                elif final_action == 'func:generate_hmacuid':
                    # See UID generation: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    line = f"REPLACE {tag} func:generate_hmacuid\n"
                elif final_action == 'func:set_fixed_datetime':
                    # See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    line = f"REPLACE {tag} func:set_fixed_datetime\n"
                elif final_action == 'func:generate_hmacdate_shift':
                    # See date shifting: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    line = f"JITTER {tag} func:generate_hmacdate_shift\n"
                elif final_action == 'func:clean_descriptors_with_llm':
                    # See LLM cleaning: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    line = f"REPLACE {tag} func:clean_descriptors_with_llm\n"
                elif final_action == 'func:generate_patient_id':
                    # See patient ID generation: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    line = f"REPLACE {tag} func:generate_patient_id\n"
                elif final_action == 'func:check_patient_age':
                    # See check patient age: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    line = f"REPLACE {tag} func:check_patient_age\n"
                elif final_action == 'clean_manually':
                    # See clean_manually action: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                    # Note: clean_manually is used for unsupported profiles - see: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#31-overview
                    line = f"# REPLACE {tag} CLEANED NEEDS MANUAL REVIEW\n"
                    logger.warning(f"Tag {tag} with VR={vr} requires manual cleaning.")
                elif final_action == 'manual_review':
                    line = f"# REPLACE {tag}  with VR={vr} MANUAL REVIEW NEEDED\n"
                    logger.warning(f"Tag {tag} with VR={vr} requires manual review.")
                else:
                    logger.error(f"Unrecognized final action '{final_action}' for tag {tag} with VR={vr}")
                    continue  # Skip unrecognized actions
                outfile.write(line)
                # The "Private Attributes" row has no real tag address and is
                # written as REMOVE ALL func:is_tag_private.  Record it in the
                # CSV as a wildcard entry only when that directive was actually
                # written (i.e. final_action is 'remove' for that row), then
                # skip the normal rationale lookup.
                if name.lower() == 'private attributes' and final_action == 'remove':
                    contributing_recipe = _find_contributing_recipe(
                        row, source_action, recipes_to_process, recipe_column_map
                    )
                    _pa_rationale = ''
                    if contributing_recipe:
                        _pa_label = _RECIPE_TO_RATIONALE_LABEL.get(contributing_recipe, '')
                        if _pa_label:
                            _pa_rationale = _extract_rationale_for_label(
                                row.get('Documentation References', ''), _pa_label
                            )
                    csv_writer.writerow({
                        'Tag': '*',
                        'Tag Name': 'Private tags (wildcard)',
                        'Action': 'REMOVE',
                        'Replacement Value': 'ALL func:is_tag_private',
                        'Rationale': _pa_rationale,
                    })
                    continue
                # Write corresponding row to summary CSV.
                # Use source_action (the CSV column value that won priority) rather
                # than final_action so that derived actions like 'remove' coming from
                # func:clean_descriptors_with_llm are attributed to the correct profile.
                contributing_recipe = _find_contributing_recipe(
                    row, source_action, recipes_to_process, recipe_column_map
                )
                rationale = ''
                if contributing_recipe:
                    label = _RECIPE_TO_RATIONALE_LABEL.get(contributing_recipe, '')
                    if label:
                        doc_refs = row.get('Documentation References', '')
                        rationale = _extract_rationale_for_label(doc_refs, label)
                csv_writer.writerow({
                    'Tag': tag,
                    'Tag Name': name,
                    'Action': _parse_action_from_recipe_line(line),
                    'Replacement Value': _parse_replacement_from_recipe_line(line),
                    'Rationale': rationale,
                })
        
        _directives_ref = 'https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#642-additional-recipe-directives'

        # Add PatientIdentityRemoved if basic_profile is in the recipe list (original logic)
        if 'basic_profile' in recipes_to_process:
            outfile.write("ADD PatientIdentityRemoved YES\n")
            _tag, _name = _lookup_tag_by_keyword('PatientIdentityRemoved')
            csv_writer.writerow({'Tag': _tag, 'Tag Name': _name, 'Action': 'ADD', 'Replacement Value': 'YES', 'Rationale': _directives_ref})

            # Remove all curve data/overlay data/overlay comments tags
            # See "If basic_profile is selected" paragraph in: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#642-additional-recipe-directives
            # TOCHECK : are these tags supposed to be removed always and
            # not only if the basic profile is requested? if yes move the line below outside if block
            outfile.write(f"REMOVE ALL func:is_curve_or_overlay_tag\n")
            csv_writer.writerow({'Tag': '*', 'Tag Name': 'Curve and Overlay tags (wildcard)', 'Action': 'REMOVE', 'Replacement Value': 'ALL func:is_curve_or_overlay_tag', 'Rationale': _directives_ref})

            # Set DeidentificationMethod based on examples from RSNA anonymizer:
            # ds.DeidentificationMethod = "RSNA DICOM ANONYMIZER"  # (0012,0063)
            outfile.write("ADD DeidentificationMethod LUWAK_ANONYMIZER\n")
            _tag, _name = _lookup_tag_by_keyword('DeidentificationMethod')
            csv_writer.writerow({'Tag': _tag, 'Tag Name': _name, 'Action': 'ADD', 'Replacement Value': 'LUWAK_ANONYMIZER', 'Rationale': _directives_ref})

            if 'retain_long_full_dates' not in recipes_to_process and 'retain_long_modified_dates' not in recipes_to_process:
                outfile.write("ADD LongitudinalTemporalInformationModified REMOVED\n")
                _tag, _name = _lookup_tag_by_keyword('LongitudinalTemporalInformationModified')
                csv_writer.writerow({'Tag': _tag, 'Tag Name': _name, 'Action': 'ADD', 'Replacement Value': 'REMOVED', 'Rationale': _directives_ref})

        if 'retain_long_full_dates' in recipes_to_process:
            outfile.write("ADD LongitudinalTemporalInformationModified UNMODIFIED\n")
            _tag, _name = _lookup_tag_by_keyword('LongitudinalTemporalInformationModified')
            csv_writer.writerow({'Tag': _tag, 'Tag Name': _name, 'Action': 'ADD', 'Replacement Value': 'UNMODIFIED', 'Rationale': _directives_ref})
        elif 'retain_long_modified_dates' in recipes_to_process:
            outfile.write("ADD LongitudinalTemporalInformationModified MODIFIED\n")
            _tag, _name = _lookup_tag_by_keyword('LongitudinalTemporalInformationModified')
            csv_writer.writerow({'Tag': _tag, 'Tag Name': _name, 'Action': 'ADD', 'Replacement Value': 'MODIFIED', 'Rationale': _directives_ref})

        # Remove DeidentificationMethodCodeSequence if exists from previous runs. It will be added 
        # again later at the end of the series deidentification.
        # See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-6
        outfile.write(f"# DeidentificationMethodCodeSequence\n")
        outfile.write(f"REMOVE (0012,0064)\n")
        _, _dcs_name = _lookup_tag_by_keyword('DeidentificationMethodCodeSequence')
        csv_writer.writerow({'Tag': '(0012,0064)', 'Tag Name': _dcs_name, 'Action': 'REMOVE', 'Replacement Value': '', 'Rationale': 'https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-6'})

        # Handle private tags
        # See "Private tag handling" paragraph in: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
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
                    elif action.lower() == 'func:generate_hmacuid':
                        line = f"REPLACE ({group},\"{private_creator}\",{element}) func:generate_hmacuid\n"
                    elif action.lower() == 'func:generate_hmacdate_shift' and 'retain_long_modified_dates' in recipes_to_process:
                        # Jitter is only applied if retain_long_modified_dates is selected
                        # See "func:generate_hmacdate_shift" conditional logic in "Private tag handling" paragraph: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                        line = f"JITTER ({group},\"{private_creator}\",{element}) func:generate_hmacdate_shift\n"
                    elif action.lower() == 'func:generate_hmacdate_shift' and not 'retain_long_modified_dates' in recipes_to_process:
                        # Skip private date tags if retain_long_modified_dates is not selected
                        # See "func:generate_hmacdate_shift" conditional logic in "Private tag handling" paragraph: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#641-translation-logic-by-action
                        continue
                    else:
                        logger.warning(f"Unrecognized action '{action}' for private tag ({group},\"{private_creator}\",{element}), skipping.")
                        continue  # Skip unrecognized actions
                    outfile.write(line)
                    csv_writer.writerow({
                        'Tag': f"({group},\"{private_creator}\",{element})",
                        'Tag Name': name,
                        'Action': _parse_action_from_recipe_line(line),
                        'Replacement Value': _parse_replacement_from_recipe_line(line),
                        'Rationale': 'TCIA (The Cancer Imaging Archive) Private Tag Knowledge Base (https://wiki.cancerimagingarchive.net/download/attachments/3539047/TCIAPrivateTagKB-02-01-2024-formatted.csv?version=2&modificationDate=1707174689263&api=v2)',
                    })

        if remove_private:
            # Add line to remove all private tags
            # See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#642-additional-recipe-directives
            line = f"REMOVE ALL func:is_tag_private\n"
            outfile.write(line)


    logger.info(f"Recipe generated: {output_file}")
    logger.info(f"Summary CSV generated: {output_csv}")
    return output_file


def _determine_final_action(actions, vr):
    """Determine the final action based on priority rules.
    
    Args:
        actions: List of actions from CSV columns
        vr: DICOM Value Representation (VR) of the tag
        
    Returns:
        tuple[str, str]: (final_action, source_action) where
          - final_action is the recipe directive to write
          - source_action is the raw CSV column value that drove this decision,
            used to look up the contributing profile for rationale attribution.
            These differ only when func:clean_descriptors_with_llm is translated
            to 'remove' or 'manual_review' based on VR.
    
    See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#65-action-priority-rules
    """
    # If any action is 'keep', final action is 'keep'
    if 'keep' in actions:
        return 'keep', 'keep'
    elif 'func:generate_hmacdate_shift' in actions:
        return 'func:generate_hmacdate_shift', 'func:generate_hmacdate_shift'
    elif 'func:generate_hmacuid' in actions:
        return 'func:generate_hmacuid', 'func:generate_hmacuid'
    elif 'func:clean_descriptors_with_llm' in actions:
        # source_action stays 'func:clean_descriptors_with_llm' even when the
        # final directive is derived as 'manual_review' or 'remove', so the
        # rationale is attributed to the clean_descriptors profile, not to
        # whichever other profile happens to carry 'remove'.
        if vr == 'SQ':
            # For sequences, we need manual review
            return 'manual_review', 'func:clean_descriptors_with_llm'
        elif vr in ['OB', 'OW', 'OF', 'UN']:
            return 'remove', 'func:clean_descriptors_with_llm'
        else:
            return 'func:clean_descriptors_with_llm', 'func:clean_descriptors_with_llm'
    elif 'replace' in actions:
        return 'replace', 'replace'
    elif 'func:check_patient_age' in actions:
        return 'func:check_patient_age', 'func:check_patient_age'
    elif 'func:set_fixed_datetime' in actions:
        return 'func:set_fixed_datetime', 'func:set_fixed_datetime'
    elif 'clean_manually' in actions:
        return 'clean_manually', 'clean_manually'
    elif 'blank' in actions:
        return 'blank', 'blank'
    elif 'func:sq_keep_original_with_review' in actions:
        return 'func:sq_keep_original_with_review', 'func:sq_keep_original_with_review'
    elif 'remove' in actions:
        return 'remove', 'remove'
    # Otherwise, take the first non-empty action from the priority order
    else:
        return actions[0], actions[0]


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
    Return the correct zero value for the given DICOM VR type 
    defined at DICOM PS3.5 2025d Section 6.2, Table 6.2-1.
    https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html
    If more numerical VRs need to be supported in the future, they can be added here.
    Make sure that this stays updated if more tags are addedd to DICOM PS3.15 Table E.1-1.
    
    See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#631-replace-action---dummy-value-generation
    """
    logger = get_logger("set_values_to_zero")

    # Text VRs - return string zeros
    if vr in ['DS', 'IS']:  # Decimal/Integer String
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
        logger.warning(f"VR {vr} not recognized for zero value, returning empty bytes")
        return b''  # fallback: empty bytes
    
def set_empty_value(vr):
    """
    Return the correct empty value for the given DICOM VR type 
    defined at DICOM PS3.5 2025d Section 6.2, Table 6.2-1.
    https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html
    If more numerical VRs need to be supported in the future, they can be added here.
    Make sure that this stays updated if more tags are addedd to DICOM PS3.15 Table E.1-1.
    
    See: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#632-blank-action---empty-value-generation
    """
    logger = get_logger("set_empty_value")
    # Text VRs
    if vr in ['DS', 'IS']:
        return ""
    # Binary VRs
    elif vr in ['OD', 'OL', 'OV', 'SV', 'UV', 'FD', 'FL', 'SS', 'US', 'SL', 'UL']:
        return b''
    else:
        logger.warning(f"VR {vr} not recognized for empty value, returning empty string")
        return ""  # fallback: empty string
