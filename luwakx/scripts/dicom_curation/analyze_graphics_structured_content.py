#!/usr/bin/env python3
"""
Analyze DICOM data for graphics and structured content sequences.
Excludes non-clinical series and checks for specific DICOM tags.

Exclusion Criteria:
- Series with descriptions matching patterns in 'excluded_series_descriptions' config
- Series with image types matching patterns in 'excluded_image_types' config
- Series with fewer slices than 'min_slices_threshold' config
- Files with extensions in 'excluded_extensions' config
- Entire series containing any SOP Class UID from 'excluded_sop_class_uids' config
- Individual reference image slices with different ImageOrientationPatient values
- Individual files without ImageOrientationPatient tag
"""

import json
import logging
from pathlib import Path
import pydicom
from pydicom.errors import InvalidDicomError
import sys


# Global configuration variables (loaded from config file)
EXCLUDED_SERIES_DESCRIPTIONS = []
EXCLUDED_IMAGE_TYPES = []
MIN_SLICES_THRESHOLD = 3
EXCLUDED_EXTENSIONS = set()
EXCLUDED_SOP_CLASS_UIDS = set()
TAGS_TO_CHECK = {}


def orientations_equal(orient1, orient2, tolerance=1e-5):
    """
    Compare two ImageOrientationPatient values with tolerance.
    
    Args:
        orient1: First orientation tuple (6 values)
        orient2: Second orientation tuple (6 values)
        tolerance: Maximum allowed difference per component (default: 1e-5)
    
    Returns:
        bool: True if orientations are equal within tolerance
    """
    if orient1 is None or orient2 is None:
        return orient1 == orient2
    if len(orient1) != len(orient2):
        return False
    return all(abs(a - b) < tolerance for a, b in zip(orient1, orient2))


def load_config(config_path='analyze_config.json'):
    """
    Load configuration from JSON file.
    All relative paths in the config are resolved relative to the config file location.
    
    Args:
        config_path: Path to configuration JSON file
    
    Returns:
        dict: Configuration dictionary with resolved absolute paths
    """
    global EXCLUDED_SERIES_DESCRIPTIONS, EXCLUDED_IMAGE_TYPES, MIN_SLICES_THRESHOLD, EXCLUDED_EXTENSIONS, EXCLUDED_SOP_CLASS_UIDS, TAGS_TO_CHECK
    
    try:
        config_path = Path(config_path).resolve()
        config_dir = config_path.parent
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Load configuration values
        EXCLUDED_SERIES_DESCRIPTIONS = config.get('excluded_series_descriptions', [])
        EXCLUDED_IMAGE_TYPES = config.get('excluded_image_types', [])
        MIN_SLICES_THRESHOLD = config.get('min_slices_threshold', 3)
        EXCLUDED_EXTENSIONS = set(config.get('excluded_extensions', []))
        EXCLUDED_SOP_CLASS_UIDS = set(config.get('excluded_sop_class_uids', []))
        
        # Convert tags_to_check from list format to tuple format
        # Handle both hex strings (e.g., "0x0070") and integers
        # Preserve the third element (filter/path) if present
        tags_dict = config.get('tags_to_check', {})
        TAGS_TO_CHECK = {}
        for name, tag in tags_dict.items():
            if isinstance(tag[0], str):
                # Convert hex strings to integers
                if len(tag) > 2:
                    TAGS_TO_CHECK[name] = (int(tag[0], 16), int(tag[1], 16), tag[2])
                else:
                    TAGS_TO_CHECK[name] = (int(tag[0], 16), int(tag[1], 16))
            else:
                # Already integers
                TAGS_TO_CHECK[name] = tuple(tag)
        
        # Resolve paths relative to config file location
        def resolve_path(path_str):
            """Resolve path relative to config file if not absolute."""
            if not path_str:
                return path_str
            path = Path(path_str)
            if path.is_absolute():
                return str(path)
            return str(config_dir / path)
        
        # Resolve input/output paths
        if 'input_folder' in config:
            config['input_folder'] = resolve_path(config['input_folder'])
        if 'output_folder' in config:
            config['output_folder'] = resolve_path(config['output_folder'])
        
        return config
    
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found!")
        print(f"Please create '{config_path}' with the required settings.")
        print("See analyze_config.json for reference structure.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file: {e}")
        sys.exit(1)


def get_file_extension(file_path):
    """
    Extract the file extension, handling compound extensions like .nii.gz or .tar.gz.
    
    Args:
        file_path: Path object representing the file
    
    Returns:
        str: The file extension (e.g., '.nrrd', '.nii.gz', or '(no extension)')
    """
    file_path = Path(file_path)
    suffixes = file_path.suffixes
    
    if not suffixes:
        return '(no extension)'
    
    # For compound extensions like .nii.gz, .tar.gz, capture both parts
    if len(suffixes) >= 2 and suffixes[-2].lower() in ['.nii', '.tar']:
        return ''.join(suffixes[-2:]).lower()
    
    # Otherwise just return the last suffix
    return suffixes[-1].lower()


def should_exclude_file(file_path):
    """
    Check if a file should be excluded based on its extension.    
    Args:
        file_path: Path object or string representing the file
    
    Returns:
        tuple: (bool, str) - (True if file should be excluded, reason for exclusion)
    """
    file_path = Path(file_path)
    file_name_lower = file_path.name.lower()
    
    # Check if filename ends with any excluded extension (handles both .ext and .compound.ext)
    for ext in EXCLUDED_EXTENSIONS:
        if file_name_lower.endswith(ext.lower()):
            return (True, ext.lower())
    
    return (False, None)


def should_exclude_series(series_description, num_slices, image_type=None):
    """
    Determine if a series should be excluded based on description, image type, and slice count.
    
    Args:
        series_description: Series description string
        num_slices: Number of slices in the series
        image_type: ImageType DICOM attribute (list or None)
    
    Returns:
        bool: True if series should be excluded
    """
    if not series_description:
        # If no description and very few slices, exclude
        return num_slices < MIN_SLICES_THRESHOLD
    
    series_desc_lower = series_description.lower().strip()
    
    # Check against excluded descriptions
    for excluded in EXCLUDED_SERIES_DESCRIPTIONS:
        if excluded.lower() in series_desc_lower:
            return True
    
    # Check against excluded image types
    if image_type and EXCLUDED_IMAGE_TYPES:
        # Convert ImageType to list if it's not already
        if isinstance(image_type, str):
            image_type_list = [image_type]
        else:
            image_type_list = list(image_type) if hasattr(image_type, '__iter__') else [str(image_type)]
        
        # Support both flat list and list of lists for excluded_image_types
        # Normalize to list of lists: ["A", "B"] -> [["A", "B"]]
        patterns_to_check = []
        for item in EXCLUDED_IMAGE_TYPES:
            if isinstance(item, list):
                patterns_to_check.append(item)
            else:
                # If it's a flat list, treat entire list as one AND pattern
                patterns_to_check = [EXCLUDED_IMAGE_TYPES]
                break
        
        # Check if ANY pattern matches (OR logic between patterns)
        # Within each pattern, ALL items must be found (AND logic)
        for pattern in patterns_to_check:
            all_patterns_found = True
            for excluded_type in pattern:
                excluded_lower = excluded_type.lower().strip()
                pattern_found = False
                for img_type_value in image_type_list:
                    if excluded_lower in img_type_value.lower():
                        pattern_found = True
                        break
                if not pattern_found:
                    all_patterns_found = False
                    break
            
            if all_patterns_found:
                return True
    
    # Exclude series with too few slices
    if num_slices < MIN_SLICES_THRESHOLD:
        return True
    
    return False


def sequence_to_dict(seq, fields_filter=None):
    """
    Convert a DICOM sequence to a JSON-serializable dictionary.
    
    Args:
        seq: pydicom Sequence
        fields_filter: Optional list of field names (keywords) to include. If None, includes all fields.
    
    Returns:
        list: List of dictionaries representing sequence items
    """
    result = []
    try:
        for item in seq:
            item_dict = {}
            for elem in item:
                try:
                    # Get tag name if available
                    tag_name = elem.keyword if hasattr(elem, 'keyword') and elem.keyword else str(elem.tag)
                    
                    # Skip if fields_filter is provided and this field is not in it
                    if fields_filter is not None and tag_name not in fields_filter:
                        continue
                    
                    # Get value, handling different types
                    if elem.VR == 'SQ':  # Nested sequence
                        # Don't filter nested sequences - include all their fields
                        item_dict[tag_name] = sequence_to_dict(elem.value, fields_filter=None)
                    else:
                        item_dict[tag_name] = str(elem.value)
                except:
                    pass
            if item_dict:
                result.append(item_dict)
    except:
        pass
    return result


def check_tags_in_dicom(dcm):
    """
    Check all tags of interest in a DICOM file with a single iteration through the dataset.
    Uses efficient arithmetic checks for Curve and Overlay repeating groups.
    For sequence tags, also captures the sequence content.
    
    This function is completely config-driven - it checks for all tags defined in TAGS_TO_CHECK.
    Config format:
    - Simple tag: "TagName": ["0x0070", "0x0001"]
    - Sequence with field filter: "TagName": ["0x0054", "0x0016", ["Field1", "Field2"]]
    - Nested value extraction: "TagName": ["0x0054", "0x0016", "SequenceName.FieldName"]
    
    Args:
        dcm: pydicom Dataset
    
    Returns:
        dict: Dictionary mapping tag names to tuples of (is_present, sequence_content or None)
              For nested value extraction, returns list of extracted values
    """
    # Initialize results dict from config
    results = {tag_name: (False, None) for tag_name in TAGS_TO_CHECK.keys()}
    
    try:
        # Single iteration through all DICOM tags
        for elem in dcm:
            group = elem.tag.group
            element = elem.tag.element
            
            # Check each configured tag
            for tag_name, tag_config in TAGS_TO_CHECK.items():
                if results[tag_name][0]:  # Already found
                    continue
                
                # Parse tag config: can be [group, element] or [group, element, [fields]] or [group, element, "Path.To.Field"]
                tag_group = tag_config[0]
                tag_element = tag_config[1]
                fields_filter = tag_config[2] if len(tag_config) > 2 else None
                
                # Special handling for repeating groups (Curve and Overlay)
                # Curve Data: (50xx,xxxx) - any element in even groups 0x5000-0x50FF
                if tag_name == 'CurveData' and 0x5000 <= group <= 0x50FF and group % 2 == 0:
                    results[tag_name] = (True, None)
                    continue
                
                # Overlay Data: (60xx,3000) - element 0x3000 in even groups 0x6000-0x60FF
                if tag_name == 'OverlayData' and 0x6000 <= group <= 0x60FF and group % 2 == 0 and element == 0x3000:
                    results[tag_name] = (True, None)
                    continue
                
                # Overlay Comments: (60xx,4000) - element 0x4000 in even groups 0x6000-0x60FF
                if tag_name == 'OverlayComments' and 0x6000 <= group <= 0x60FF and group % 2 == 0 and element == 0x4000:
                    results[tag_name] = (True, None)
                    continue
                
                # Standard tag match
                if elem.tag == (tag_group, tag_element):
                    value = elem.value
                    if hasattr(value, '__len__') and not isinstance(value, str) and len(value) > 0:
                        # Check if it's a sequence (VR == 'SQ')
                        if elem.VR == 'SQ':
                            # Check if fields_filter is a nested path string (e.g., "SequenceName.FieldName")
                            if isinstance(fields_filter, str) and '.' in fields_filter:
                                # Extract nested values
                                extracted_values = extract_nested_values(value, fields_filter)
                                if extracted_values:
                                    results[tag_name] = (True, extracted_values)
                            else:
                                # Normal sequence handling with field filter
                                results[tag_name] = (True, sequence_to_dict(value, fields_filter=fields_filter))
                        else:
                            results[tag_name] = (True, None)
        
        return results
    
    except Exception as e:
        #print(f"Error checking tags: {e}")
        return results


def extract_nested_values(sequence, path):
    """
    Extract values from a nested sequence path.
    
    Args:
        sequence: pydicom Sequence
        path: Dot-separated path (e.g., "RadiopharmaceuticalCodeSequence.CodeValue")
    
    Returns:
        list: List of extracted values
    """
    parts = path.split('.')
    if len(parts) != 2:
        return []
    
    sequence_name, field_name = parts
    extracted_values = []
    
    try:
        for item in sequence:
            # Check if the nested sequence exists
            if hasattr(item, sequence_name):
                nested_seq = getattr(item, sequence_name)
                # If it's a sequence, iterate through it
                if hasattr(nested_seq, '__iter__') and not isinstance(nested_seq, str):
                    for nested_item in nested_seq:
                        if hasattr(nested_item, field_name):
                            value = getattr(nested_item, field_name)
                            if value and value not in extracted_values:
                                extracted_values.append(value)
    except:
        pass
    
    return extracted_values


def analyze_dicom_directory(root_dir, excluded_csv_path=None):
    """
    Analyze DICOM directory structure for graphics and structured content.
    
    Recursively scans all subdirectories for DICOM files, checks for specific
    DICOM tags, and generates comprehensive statistics about series and patients.
    
    Args:
        root_dir: Root directory to start scanning (contains patient folders)
        excluded_csv_path: Path to CSV file for streaming excluded files (optional)
    
    Returns:
        dict: Analysis results containing:
            - global_summary: Global statistics and tag occurrences
            - patient_data: Per-patient analysis data
            - excluded_summary: Statistics about excluded series
            - excluded_patient_data: Per-patient excluded series data
            - excluded_files_count: Count of excluded files written to CSV
            - non_dicom_files_by_patient: Non-DICOM files grouped by patient
    """
    root_path = Path(root_dir)
    
    # Global statistics
    global_stats = {
        'total_patients': 0,
        'total_studies': 0,
        'total_series_checked': 0,
        'total_instances': 0,
        'sop_class_uids': set(),
        'sop_class_uids_with_occurrences': set(),
        'sop_class_uids_without_any_occurrences': set(),
        'sop_class_uids_per_tag': {},
        'unique_sequence_contents': {},  # Track unique sequence contents
        'series_uids_per_tag': {},  # Track series UIDs for each tag
        'series_count_per_sop_class': {},  # Track total series count per SOP Class UID
        'series_with_occurrences_per_sop_class': {},  # Track series WITH any tag occurrence per SOP Class UID
        'series_without_occurrences_per_sop_class': {},  # Track series WITHOUT any tag occurrence per SOP Class UID
        'series_count_per_tag_per_sop_class': {},  # Track series count per tag per SOP Class UID
        'study_instance_uids': set(),  # Track all unique study UIDs encountered
        'kept_study_instance_uids': set(),  # Track study UIDs with at least one kept series
    }
    
    # Initialize tag occurrence counters, per-tag SOP Class UID tracking, sequence content tracking, and series UID tracking
    for tag_name, tag_config in TAGS_TO_CHECK.items():
        global_stats[f'{tag_name}_occurrences'] = 0
        global_stats['sop_class_uids_per_tag'][tag_name] = set()
        global_stats['series_uids_per_tag'][tag_name] = set()  # Track series UIDs with this tag
        global_stats['series_count_per_tag_per_sop_class'][tag_name] = {}  # Track series count for THIS tag per SOP Class
        
        # Check if this is a nested value extraction (string path with dot) or regular sequence
        if len(tag_config) > 2 and isinstance(tag_config[2], str) and '.' in tag_config[2]:
            # For nested value extraction, use a set to collect unique values
            global_stats['unique_sequence_contents'][tag_name] = set()
        else:
            # For regular sequences, use a list
            global_stats['unique_sequence_contents'][tag_name] = []
    
    # Per-patient data
    patient_data = {}
    
    # Per-patient modality tracking
    patient_modalities = {}  # PatientID -> set of modalities
    
    # Track errors and warnings per series to avoid duplicates
    series_messages = {}  # (PatientID, SeriesInstanceUID) -> set of (level, message) tuples
    
    # Helper function to log deduplicated messages per series
    def log_series_message(patient_id, series_uid, level, message):
        """Log a message only once per series."""
        key = (patient_id, series_uid)
        if key not in series_messages:
            series_messages[key] = set()
        
        message_tuple = (level, message)
        if message_tuple not in series_messages[key]:
            series_messages[key].add(message_tuple)
            if level == 'ERROR':
                logging.error(f"Patient {patient_id}, Series {series_uid}: {message}")
            elif level == 'WARNING':
                logging.warning(f"Patient {patient_id}, Series {series_uid}: {message}")
            elif level == 'INFO':
                logging.info(f"Patient {patient_id}, Series {series_uid}: {message}")
    
    # Excluded series tracking
    excluded_stats = {
        'total_series_examined': 0,
        'total_series_excluded': 0,
        'total_non_dicom_files': 0,
        'excluded_series_instance_uids': set(),
        'excluded_sop_class_uids': set(),
        'kept_sop_class_uids': set(),
        'excluded_instances_count': 0,  # Count of excluded DICOM instances (from series + reference images)
        'additional_extensions_found': set(),  # Track file extensions found in non-DICOM files
        'unique_series_descriptions': set(),  # Track unique series descriptions found
        'unique_image_types': set(),  # Track unique image type values found
    }
    
    # Per-patient excluded series data
    excluded_patient_data = {}
    
    # Track non-DICOM files by patient
    non_dicom_files_by_patient = {}  # PatientID -> list of file paths
    
    # Open CSV file for streaming excluded files (write immediately, don't accumulate in memory)
    csv_file = None
    csv_writer = None
    excluded_files_count = 0
    
    if excluded_csv_path:
        import csv
        csv_file = open(excluded_csv_path, 'w', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file)
        # Write header
        csv_writer.writerow(['File Path', 'PatientID', 'RationaleClass', 'RationaleDetails', 'SeriesInstanceUID', 'SeriesNumber', 'StudyDate'])
    
    # Recursively find all files
    print(f"Scanning directory: {root_dir}")
    all_files = []
    files_excluded_by_extension = 0
    total_files_found = 0
    for file_path in root_path.rglob('*'):
        if file_path.is_file():
            total_files_found += 1
            should_exclude, exclusion_reason = should_exclude_file(file_path)
            if should_exclude:
                if csv_writer:
                    csv_writer.writerow([str(file_path.relative_to(root_path)), "", "Extension", exclusion_reason, "", "", ""])
                    excluded_files_count += 1
                files_excluded_by_extension += 1
            else:
                all_files.append(file_path)
    
    print(f"Total files found in directory scan: {total_files_found}")
    logging.info(f"Total files found in directory scan: {total_files_found}")
    print(f"Files excluded by extension: {files_excluded_by_extension}")
    logging.info(f"Files excluded by extension: {files_excluded_by_extension}")
    print(f"Found {len(all_files)} potential files to examine")
    logging.info(f"Found {len(all_files)} potential files to examine")
    
    # Group files by SeriesInstanceUID and PatientID
    # Track ImageOrientationPatient for reference image detection
    series_by_patient = {}  # PatientID -> SeriesInstanceUID -> list of file paths
    series_orientation_map = {}  # PatientID -> SeriesInstanceUID -> list of ImageOrientationPatient values
    series_sop_class_map = {}  # PatientID -> SeriesInstanceUID -> set of SOP Class UIDs
    series_metadata_map = {}  # PatientID -> SeriesInstanceUID -> {'series_number': str, 'study_date': str}
    
    files_examined = 0
    files_skipped = 0
    
    for file_path in all_files:
        patient_id_for_non_dicom = None
        try:
            # Try to read as DICOM - this validates it's actually a DICOM file
            dcm = pydicom.dcmread(str(file_path), stop_before_pixels=True, force=True)
            
            # Verify it's a valid DICOM by checking for required tags
            if not hasattr(dcm, 'SOPClassUID'):
                # Not a valid DICOM file
                patient_id_for_non_dicom = getattr(dcm, 'PatientID', None)
                files_skipped += 1
                excluded_stats['total_non_dicom_files'] += 1
                if csv_writer:
                    csv_writer.writerow([str(file_path.relative_to(root_path)), patient_id_for_non_dicom or "", "Not DICOM", "Missing SOPClassUID", "", "", ""])
                    excluded_files_count += 1
                # Track file extension
                file_ext = get_file_extension(file_path)
                excluded_stats['additional_extensions_found'].add(file_ext)
                # Track non-DICOM file
                if patient_id_for_non_dicom:
                    if patient_id_for_non_dicom not in non_dicom_files_by_patient:
                        non_dicom_files_by_patient[patient_id_for_non_dicom] = []
                    non_dicom_files_by_patient[patient_id_for_non_dicom].append(str(file_path.relative_to(root_path)))
                continue
            
            patient_id = getattr(dcm, 'PatientID', 'Unknown')
            series_uid = getattr(dcm, 'SeriesInstanceUID', None)
            study_uid = getattr(dcm, 'StudyInstanceUID', None)
            
            # Track study UID for counting total studies
            if study_uid:
                global_stats['study_instance_uids'].add(study_uid)
            
            if not series_uid:
                files_skipped += 1
                excluded_stats['total_non_dicom_files'] += 1
                if csv_writer:
                    csv_writer.writerow([str(file_path.relative_to(root_path)), patient_id, "Not DICOM", "Missing SeriesInstanceUID", "", "", ""])
                    excluded_files_count += 1
                # Track file extension
                file_ext = get_file_extension(file_path)
                excluded_stats['additional_extensions_found'].add(file_ext)
                # Track non-DICOM file
                if patient_id not in non_dicom_files_by_patient:
                    non_dicom_files_by_patient[patient_id] = []
                non_dicom_files_by_patient[patient_id].append(str(file_path.relative_to(root_path)))
                continue
            
            if patient_id not in series_by_patient:
                series_by_patient[patient_id] = {}
                series_orientation_map[patient_id] = {}
                series_sop_class_map[patient_id] = {}
                series_metadata_map[patient_id] = {}
            
            # Get series metadata once (for efficiency) - will be stored and reused
            if series_uid not in series_by_patient[patient_id]:
                series_by_patient[patient_id][series_uid] = []
                series_orientation_map[patient_id][series_uid] = []
                series_sop_class_map[patient_id][series_uid] = set()
                # Store SeriesNumber and StudyDate for this series
                series_number_value = getattr(dcm, 'SeriesNumber', '')
                study_date_value = getattr(dcm, 'StudyDate', '')
                series_metadata_map[patient_id][series_uid] = {
                    'series_number': str(series_number_value) if series_number_value else '',
                    'study_date': str(study_date_value) if study_date_value else ''
                }
            
            # Get metadata for this series (already stored, so just retrieve once)
            series_meta = series_metadata_map[patient_id][series_uid]
            series_number = series_meta['series_number']
            study_date = series_meta['study_date']
            
            # Check for Enhanced CT Image Storage (not supported) - silently skip without any accounting
            ENHANCED_CT_SOP_CLASS_UID = "1.2.840.10008.5.1.4.1.1.2.1"
            sop_class = getattr(dcm, 'SOPClassUID', None)
            if sop_class == ENHANCED_CT_SOP_CLASS_UID:
                logging.error(
                    f"Enhanced CT Image Storage (SOP Class UID {ENHANCED_CT_SOP_CLASS_UID}) detected. "
                    f"Only standard CT Image Storage is supported for analysis. Skipping file without accounting. "
                    f"Patient ID: {patient_id}, Series Instance UID: {series_uid}, File: {file_path.name}"
                )
                continue
            
            # At this point, we have a valid DICOM file (has SOPClassUID and SeriesInstanceUID)
            # Increment files_examined - this file was successfully read and examined
            files_examined += 1
            
            # Check if ImageOrientationPatient is present and valid, exclude file if not
            orientation_value = getattr(dcm, 'ImageOrientationPatient', None)
            if orientation_value is None:
                # This is a valid DICOM file, but we're excluding it due to missing required tag
                excluded_stats['excluded_instances_count'] += 1
                rel_path = str(file_path.relative_to(root_path))
                if csv_writer:
                    csv_writer.writerow([rel_path, patient_id, "None Value ImageOrientationPatient", "ImageOrientationPatient tag not found or is None", series_uid, series_number, study_date])
                    excluded_files_count += 1
                continue
            
            # Track SOP Class UID (collect during initial read to avoid re-reading files)
            sop_class = getattr(dcm, 'SOPClassUID', None)
            if sop_class:
                series_sop_class_map[patient_id][series_uid].add(sop_class)
            
            # Track ImageOrientationPatient for image detection
            try:
                orientation = tuple(orientation_value)
                series_orientation_map[patient_id][series_uid].append((file_path, orientation))
            except (TypeError, ValueError) as e:
                # If orientation can't be converted to tuple, it's a valid DICOM but excluded
                excluded_stats['excluded_instances_count'] += 1
                rel_path = str(file_path.relative_to(root_path))
                if csv_writer:
                    csv_writer.writerow([rel_path, patient_id, "Invalid ImageOrientationPatient Value", f"Cannot convert to tuple: {e}", series_uid, series_number, study_date])
                    excluded_files_count += 1
                continue
            
            # File passed all checks, add it to the series
            series_by_patient[patient_id][series_uid].append(file_path)
            
        except Exception as e:
            # Not a valid DICOM file, skip - log error once per series
            patient_id_err = getattr(dcm, 'PatientID', 'Unknown') if 'dcm' in locals() else 'Unknown'
            series_uid_err = getattr(dcm, 'SeriesInstanceUID', 'Unknown') if 'dcm' in locals() else 'Unknown'
            error_msg = f"Not DICOM: Read error - {type(e).__name__}: {str(e)}"
            
            log_series_message(patient_id_err, series_uid_err, 'ERROR', error_msg)
            
            files_skipped += 1
            excluded_stats['total_non_dicom_files'] += 1
            error_details = f"Read error - {type(e).__name__}: {str(e)}"
            if csv_writer:
                csv_writer.writerow([str(file_path.relative_to(root_path)), patient_id_err, "Not DICOM", error_details, "", "", ""])
                excluded_files_count += 1
            # Track file extension
            file_ext = get_file_extension(file_path)
            excluded_stats['additional_extensions_found'].add(file_ext)
            # For files that can't be read at all, store under 'Unknown' patient
            if 'Unknown' not in non_dicom_files_by_patient:
                non_dicom_files_by_patient['Unknown'] = []
            non_dicom_files_by_patient['Unknown'].append(str(file_path.relative_to(root_path)))
            continue
    
    print(f"\nFile Accounting Breakdown (initial):")
    logging.info("")
    logging.info("File Accounting Breakdown (initial):")
    print(f"  Total files found in directory scan: {total_files_found}")
    logging.info(f"  Total files found in directory scan: {total_files_found}")
    print(f"  Files excluded by extension: {files_excluded_by_extension}")
    logging.info(f"  Files excluded by extension: {files_excluded_by_extension}")
    print(f"  Potential files to examine: {len(all_files)}")
    logging.info(f"  Potential files to examine: {len(all_files)}")
    print(f"  Successfully read as DICOM: {files_examined}")
    logging.info(f"  Successfully read as DICOM: {files_examined}")
    print(f"  Skipped (non-DICOM or read errors): {files_skipped}")
    logging.info(f"  Skipped (non-DICOM or read errors): {files_skipped}")
    print(f"  Excluded during initial file scan: {excluded_stats['excluded_instances_count']}")
    logging.info(f"  Excluded during initial file scan: {excluded_stats['excluded_instances_count']}")
    
    print(f"\nFound {len(series_by_patient)} patients")
    logging.info(f"")
    logging.info(f"Found {len(series_by_patient)} patients")
    
    # Process each patient and their series
    for patient_id in sorted(series_by_patient.keys()):
        print(f"\nProcessing patient: {patient_id}")
        logging.info(f"Processing patient: {patient_id}")
        
        patient_info = {
            'patient_id': patient_id,
            'total_series_checked': 0,
            'series_descriptions': [],
            'tag_occurrences': {},
            'series_information': {},
            'sop_class_uids_with_occurrences': set(),
            'sop_class_uids_without_any_occurrences': set(),
            'sop_class_uids_per_tag': {},
        }
        
        # Initialize tag counters, series information lists, and per-tag SOP Class UID tracking for this patient
        for tag_name in TAGS_TO_CHECK.keys():
            patient_info['tag_occurrences'][tag_name] = 0
            patient_info['series_information'][tag_name] = []
            patient_info['sop_class_uids_per_tag'][tag_name] = set()
        
        # Initialize excluded series tracking for this patient
        if patient_id not in excluded_patient_data:
            excluded_patient_data[patient_id] = {
                'patient_id': patient_id,
                'excluded_series': [],
                'non_dicom_files': {
                    'count': 0,
                    'reason': 'not in excluded extension list, but not dicom either',
                    'file_paths': []
                }
            }
        
        # Add non-DICOM files for this patient if any exist
        if patient_id in non_dicom_files_by_patient:
            excluded_patient_data[patient_id]['non_dicom_files']['count'] = len(non_dicom_files_by_patient[patient_id])
            excluded_patient_data[patient_id]['non_dicom_files']['file_paths'] = non_dicom_files_by_patient[patient_id]
        
        series_count = len(series_by_patient[patient_id])
        print(f"  Found {series_count} series for patient {patient_id}")
        logging.info(f"  Found {series_count} series for patient {patient_id}")
        
        # Process each series for this patient
        for series_uid, dicom_files in sorted(series_by_patient[patient_id].items()):
            num_slices = len(dicom_files)
            
            # Read first DICOM file to get series info
            try:
                dcm = pydicom.dcmread(str(dicom_files[0]), stop_before_pixels=True)
            except (InvalidDicomError, Exception) as e:
                continue
            
            # Get series description from first file
            series_description = getattr(dcm, 'SeriesDescription', '')
            
            # Get image type from first file
            image_type = getattr(dcm, 'ImageType', None)
            
            # Get SOP Class UIDs from the pre-collected map (efficient - no re-reading files)
            sop_class_uids_in_series = series_sop_class_map.get(patient_id, {}).get(series_uid, set())
            
            # For backward compatibility, use first file's SOP Class UID as primary
            sop_class_uid = getattr(dcm, 'SOPClassUID', None)
            
            # Track that we examined this series
            excluded_stats['total_series_examined'] += 1
            
            # Track unique series descriptions and image types (only for kept series, will be tracked below)
            # We'll add them to the tracking sets only if series is NOT excluded
            
            # Get relative file path
            relative_file_path = str(dicom_files[0].relative_to(root_path))
            
            # Get series metadata (SeriesNumber and StudyDate) from the metadata map
            series_meta = series_metadata_map.get(patient_id, {}).get(series_uid, {})
            series_number = series_meta.get('series_number', '')
            study_date = series_meta.get('study_date', '')
            
            # Check if series should be excluded by SOP Class UID
            series_excluded_by_sop_class = False
            if sop_class_uids_in_series & EXCLUDED_SOP_CLASS_UIDS:  # Intersection check
                series_excluded_by_sop_class = True
                excluded_stats['total_series_excluded'] += 1
                excluded_stats['excluded_series_instance_uids'].add(series_uid)
                
                for uid in sop_class_uids_in_series:
                    excluded_stats['excluded_sop_class_uids'].add(uid)
                
                # Determine which SOP Class UIDs matched
                matched_sop_classes = sop_class_uids_in_series & EXCLUDED_SOP_CLASS_UIDS
                exclusion_details = f"Series contains excluded SOP Class UID(s): {', '.join(sorted(matched_sop_classes))}"
                
                # Add all files from this series to excluded files log
                for file_path in dicom_files:
                    rel_path = str(file_path.relative_to(root_path))
                    if csv_writer:
                        csv_writer.writerow([rel_path, patient_id, "Excluded SOP Class", exclusion_details, series_uid, series_number, study_date])
                        excluded_files_count += 1
                    excluded_stats['excluded_instances_count'] += 1
                
                # Store excluded series information
                excluded_patient_data[patient_id]['excluded_series'].append({
                    'series_instance_uid': series_uid,
                    'series_description': series_description or '(no description)',
                    'sop_class_uid': sop_class_uid or 'Unknown',
                    'file_path': relative_file_path,
                    'num_slices': num_slices,
                    'exclusion_reason': exclusion_details
                })
                
                continue
            
            # Check if series should be excluded by description, image type, or slice count
            if should_exclude_series(series_description, num_slices, image_type):
                # Track excluded series in global stats
                excluded_stats['total_series_excluded'] += 1
                excluded_stats['excluded_series_instance_uids'].add(series_uid)
                
                if sop_class_uid:
                    excluded_stats['excluded_sop_class_uids'].add(sop_class_uid)
                
                # Determine exclusion reason
                rationale_class = None
                rationale_details = None
                if num_slices < MIN_SLICES_THRESHOLD:
                    rationale_class = "Min Slices"
                    rationale_details = f"Series has {num_slices} slices (below minimum threshold of {MIN_SLICES_THRESHOLD})"
                else:
                    # Check which excluded description pattern matched
                    series_desc_lower = series_description.lower().strip() if series_description else ""
                    for excluded in EXCLUDED_SERIES_DESCRIPTIONS:
                        if excluded.lower() in series_desc_lower:
                            rationale_class = "Excluded SeriesDescription"
                            rationale_details = f"Series description matches excluded pattern: '{excluded}'"
                            break
                    
                    # Check if excluded by image type
                    if not rationale_class and image_type and EXCLUDED_IMAGE_TYPES:
                        image_type_list = [image_type] if isinstance(image_type, str) else list(image_type) if hasattr(image_type, '__iter__') else [str(image_type)]
                        
                        # Support both flat list and list of lists for excluded_image_types
                        patterns_to_check = []
                        for item in EXCLUDED_IMAGE_TYPES:
                            if isinstance(item, list):
                                patterns_to_check.append(item)
                            else:
                                # If it's a flat list, treat entire list as one AND pattern
                                patterns_to_check = [EXCLUDED_IMAGE_TYPES]
                                break
                        
                        # Check if ANY pattern matches (OR logic between patterns)
                        for pattern in patterns_to_check:
                            all_patterns_found = True
                            matched_patterns = []
                            for excluded_type in pattern:
                                excluded_lower = excluded_type.lower().strip()
                                pattern_found = False
                                for img_type_value in image_type_list:
                                    if excluded_lower in img_type_value.lower():
                                        pattern_found = True
                                        matched_patterns.append(excluded_type)
                                        break
                                if not pattern_found:
                                    all_patterns_found = False
                                    break
                            
                            if all_patterns_found:
                                rationale_class = "Excluded ImageType"
                                rationale_details = f"Image type matches all excluded patterns: {matched_patterns}"
                                break
                    
                    if not rationale_class:
                        rationale_class = "Other"
                        rationale_details = "Series excluded by filter criteria"
                
                # Add all files from this series to excluded files log
                for file_path in dicom_files:
                    rel_path = str(file_path.relative_to(root_path))
                    if csv_writer:
                        csv_writer.writerow([rel_path, patient_id, rationale_class, rationale_details, series_uid, series_number, study_date])
                        excluded_files_count += 1
                    excluded_stats['excluded_instances_count'] += 1
                
                # Store excluded series information
                excluded_patient_data[patient_id]['excluded_series'].append({
                    'series_instance_uid': series_uid,
                    'series_description': series_description or '(no description)',
                    'sop_class_uid': sop_class_uid or 'Unknown',
                    'file_path': relative_file_path,
                    'num_slices': num_slices,
                    'exclusion_reason': exclusion_reason
                })
                
                continue
            
            # Series is included - check for reference images before processing
            # Check for reference images based on ImageOrientationPatient with tolerance
            # Only exclude if there is exactly 1 file total with different orientation
            # If 2+ files have different orientations (even if all different from each other), keep all
            if patient_id in series_orientation_map and series_uid in series_orientation_map[patient_id]:
                orientation_data = series_orientation_map[patient_id][series_uid]
                if orientation_data:
                    # Group orientations with tolerance-based comparison
                    orientation_groups = {}  # orientation_key -> list of (file_path, orientation)
                    
                    for file_path, orientation in orientation_data:
                        # Find matching group with tolerance
                        matched_key = None
                        for existing_key in orientation_groups.keys():
                            if orientations_equal(orientation, existing_key):
                                matched_key = existing_key
                                break
                        
                        if matched_key is None:
                            # New orientation group
                            orientation_groups[orientation] = [(file_path, orientation)]
                        else:
                            # Add to existing group
                            orientation_groups[matched_key].append((file_path, orientation))
                    
                    # If multiple orientation groups exist
                    if len(orientation_groups) > 1:
                        # Find the most common orientation group (main series)
                        most_common_key = max(orientation_groups.keys(), key=lambda k: len(orientation_groups[k]))
                        most_common_count = len(orientation_groups[most_common_key])
                        
                        # Count total files with different orientations
                        total_different_orientation_files = len(orientation_data) - most_common_count
                        
                        # Only exclude if exactly 1 file has a different orientation (true reference image)
                        if total_different_orientation_files == 1:
                            for orient_key, files_list in orientation_groups.items():
                                if not orientations_equal(orient_key, most_common_key):
                                    for file_path, orientation in files_list:
                                        rel_path = str(file_path.relative_to(root_path))
                                        if csv_writer:
                                            csv_writer.writerow([rel_path, patient_id, "Reference image: Different ImageOrientationPatient", "only 1 file with different orientation", series_uid, series_number, study_date])
                                            excluded_files_count += 1
                                        excluded_stats['excluded_instances_count'] += 1
            
            # Process included series
            patient_info['total_series_checked'] += 1
            global_stats['total_series_checked'] += 1
            patient_info['series_descriptions'].append(series_description or '(no description)')
            
            # Track study UID for kept series
            study_uid = getattr(dcm, 'StudyInstanceUID', None)
            if study_uid:
                global_stats['kept_study_instance_uids'].add(study_uid)
            
            # Track unique series descriptions (only for kept series)
            if series_description:
                excluded_stats['unique_series_descriptions'].add(series_description)
            
            # Track unique image types (only for kept series) - store as tuple for hashability
            if image_type:
                # Convert to tuple (hashable for set) - preserves multi-valued nature
                if isinstance(image_type, str):
                    excluded_stats['unique_image_types'].add((image_type,))
                elif hasattr(image_type, '__iter__'):
                    # Convert multi-valued ImageType to tuple
                    image_type_tuple = tuple(str(val) for val in image_type)
                    excluded_stats['unique_image_types'].add(image_type_tuple)
            
            # Track modality for this patient (only for included series)
            series_modality = getattr(dcm, 'Modality', None)
            if series_modality:
                if patient_id not in patient_modalities:
                    patient_modalities[patient_id] = set()
                patient_modalities[patient_id].add(series_modality)
            
            # Track ALL SOP Class UIDs found in this series
            for uid in sop_class_uids_in_series:
                global_stats['sop_class_uids'].add(uid)
                excluded_stats['kept_sop_class_uids'].add(uid)
                # Count series per SOP Class UID
                if uid not in global_stats['series_count_per_sop_class']:
                    global_stats['series_count_per_sop_class'][uid] = 0
                global_stats['series_count_per_sop_class'][uid] += 1
            
            # Track if this series has any tag occurrences
            series_has_occurrence = False
            
            # Check all tags in a single pass through the DICOM dataset
            tag_results = check_tags_in_dicom(dcm)
            
            # Process results
            for tag_name, (is_present, sequence_content) in tag_results.items():
                if is_present:
                    patient_info['tag_occurrences'][tag_name] += 1
                    global_stats[f'{tag_name}_occurrences'] += 1
                    series_has_occurrence = True
                    
                    # Track SOP Class UID and Series UID for this specific tag
                    if sop_class_uid:
                        patient_info['sop_class_uids_per_tag'][tag_name].add(sop_class_uid)
                        global_stats['sop_class_uids_per_tag'][tag_name].add(sop_class_uid)
                    
                    # Also track all SOP Class UIDs from the series (if multiple)
                    for uid in sop_class_uids_in_series:
                        patient_info['sop_class_uids_per_tag'][tag_name].add(uid)
                        global_stats['sop_class_uids_per_tag'][tag_name].add(uid)
                        # Count series with THIS specific tag per SOP Class UID
                        if uid not in global_stats['series_count_per_tag_per_sop_class'][tag_name]:
                            global_stats['series_count_per_tag_per_sop_class'][tag_name][uid] = 0
                        global_stats['series_count_per_tag_per_sop_class'][tag_name][uid] += 1
                    
                    # Track Series UID for this specific tag
                    global_stats['series_uids_per_tag'][tag_name].add(series_uid)
                    
                    # Prepare series information entry
                    series_info_entry = {
                        'series_instance_uid': series_uid,
                        'keep_series': True,
                        'series_description': series_description or '(no description)',
                        'sop_class_uid': sop_class_uid or 'Unknown'
                    }
                    
                    # Add all SOP Class UIDs if there are multiple
                    if len(sop_class_uids_in_series) > 1:
                        series_info_entry['all_sop_class_uids'] = list(sop_class_uids_in_series)
                    
                    # Add sequence content if this is a sequence tag
                    if sequence_content is not None:
                        # Check if this is extracted nested values (list of strings) or sequence dict
                        if isinstance(sequence_content, list) and all(isinstance(v, str) for v in sequence_content):
                            # This is extracted nested values - add to unique sequence contents as a set
                            series_info_entry['extracted_values'] = sequence_content
                            
                            # Add to global unique values collection
                            if tag_name not in global_stats['unique_sequence_contents']:
                                global_stats['unique_sequence_contents'][tag_name] = set()
                            # If it's already a set, add the values
                            if isinstance(global_stats['unique_sequence_contents'][tag_name], set):
                                global_stats['unique_sequence_contents'][tag_name].update(sequence_content)
                            else:
                                # Convert to set and add values
                                global_stats['unique_sequence_contents'][tag_name] = set(sequence_content)
                        else:
                            # Normal sequence handling
                            series_info_entry['sequence_content'] = sequence_content
                            
                            # Add to global unique sequence contents if not already there
                            if tag_name in global_stats['unique_sequence_contents']:
                                # Convert to string for comparison (JSON serialization)
                                content_str = json.dumps(sequence_content, sort_keys=True)
                                existing_contents = [json.dumps(c, sort_keys=True) for c in global_stats['unique_sequence_contents'][tag_name]]
                                if content_str not in existing_contents:
                                    global_stats['unique_sequence_contents'][tag_name].append(sequence_content)
                    
                    # Store the series information
                    patient_info['series_information'][tag_name].append(series_info_entry)
            
            # Track SOP Class UIDs with/without occurrences and count series separately
            if sop_class_uids_in_series:
                if series_has_occurrence:
                    for uid in sop_class_uids_in_series:
                        patient_info['sop_class_uids_with_occurrences'].add(uid)
                        global_stats['sop_class_uids_with_occurrences'].add(uid)
                        # Count series WITH occurrences per SOP Class UID
                        if uid not in global_stats['series_with_occurrences_per_sop_class']:
                            global_stats['series_with_occurrences_per_sop_class'][uid] = 0
                        global_stats['series_with_occurrences_per_sop_class'][uid] += 1
                else:
                    for uid in sop_class_uids_in_series:
                        patient_info['sop_class_uids_without_any_occurrences'].add(uid)
                        global_stats['sop_class_uids_without_any_occurrences'].add(uid)
                        # Count series WITHOUT occurrences per SOP Class UID
                        if uid not in global_stats['series_without_occurrences_per_sop_class']:
                            global_stats['series_without_occurrences_per_sop_class'][uid] = 0
                        global_stats['series_without_occurrences_per_sop_class'][uid] += 1
        
        # Add patient data if any series were checked
        if patient_info['total_series_checked'] > 0:
            global_stats['total_patients'] += 1
            patient_data[patient_id] = patient_info
        
        # Log modality and SOP Class UID summary for this patient
        if patient_id in patient_modalities and patient_modalities[patient_id]:
            modalities_str = ', '.join(sorted(patient_modalities[patient_id]))
            
            # Collect all SOP Class UIDs for this patient from included series
            patient_sop_class_uids = set()
            if patient_info['total_series_checked'] > 0:
                # Get SOP Class UIDs from both with and without occurrences
                patient_sop_class_uids.update(patient_info['sop_class_uids_with_occurrences'])
                patient_sop_class_uids.update(patient_info['sop_class_uids_without_any_occurrences'])
            
            if patient_sop_class_uids:
                sop_uids_str = ', '.join(sorted(patient_sop_class_uids))
                logging.info(f"Patient {patient_id}: Modalities = {modalities_str} | SOP Class UIDs = {sop_uids_str}")
            else:
                logging.info(f"Patient {patient_id}: Modalities = {modalities_str}")
        
        # Free memory for this patient's file lists (no longer needed after processing)
        # This reduces peak memory by ~95% for large datasets (from 500MB to 50MB for 1M files)
        if patient_id in series_by_patient:
            del series_by_patient[patient_id]
        if patient_id in series_orientation_map:
            del series_orientation_map[patient_id]
        if patient_id in series_sop_class_map:
            del series_sop_class_map[patient_id]
        # Keep series_metadata_map - it's tiny (~100 bytes per series) and already minimal
    
    # Set total studies count
    global_stats['total_studies'] = len(global_stats['study_instance_uids'])
    
    # Set final kept patient studies count
    global_stats['final_kept_patient_studies'] = len(global_stats['kept_study_instance_uids'])
    
    # Calculate kept instances AFTER all exclusions (series-level and reference image exclusions)
    global_stats['total_instances'] = files_examined - excluded_stats['excluded_instances_count']
    
    # Print final accounting breakdown
    print(f"\nFinal File Accounting Breakdown:")
    logging.info("")
    logging.info("Final File Accounting Breakdown:")
    print(f"  Total files found in directory scan: {total_files_found}")
    logging.info(f"  Total files found in directory scan: {total_files_found}")
    print(f"  Files excluded by extension: {files_excluded_by_extension}")
    logging.info(f"  Files excluded by extension: {files_excluded_by_extension}")
    print(f"  Potential files to examine: {len(all_files)}")
    logging.info(f"  Potential files to examine: {len(all_files)}")
    print(f"  Successfully read as DICOM: {files_examined}")
    logging.info(f"  Successfully read as DICOM: {files_examined}")
    print(f"  Skipped (non-DICOM or read errors): {files_skipped}")
    logging.info(f"  Skipped (non-DICOM or read errors): {files_skipped}")
    print(f"  Excluded after examination (all types): {excluded_stats['excluded_instances_count']}")
    logging.info(f"  Excluded after examination (all types): {excluded_stats['excluded_instances_count']}")
    print(f"  Final kept instances (total_instances): {global_stats['total_instances']}")
    logging.info(f"  Final kept instances (total_instances): {global_stats['total_instances']}")
    print(f"\nVerification: {files_examined} - {excluded_stats['excluded_instances_count']} = {global_stats['total_instances']}")
    logging.info(f"")
    logging.info(f"Verification: {files_examined} - {excluded_stats['excluded_instances_count']} = {global_stats['total_instances']}")
    
    # Verify CSV count matches expected total
    expected_csv_count = files_excluded_by_extension + files_skipped + excluded_stats['excluded_instances_count']
    print(f"CSV rows written: {excluded_files_count}")
    logging.info(f"CSV rows written: {excluded_files_count}")
    print(f"Expected: {files_excluded_by_extension} (extension) + {files_skipped} (non-DICOM) + {excluded_stats['excluded_instances_count']} (excluded after exam) = {expected_csv_count}")
    logging.info(f"Expected: {files_excluded_by_extension} (extension) + {files_skipped} (non-DICOM) + {excluded_stats['excluded_instances_count']} (excluded after exam) = {expected_csv_count}")
    
    if excluded_files_count != expected_csv_count:
        print(f"⚠️  WARNING: CSV count mismatch! Written: {excluded_files_count}, Expected: {expected_csv_count}")
        logging.warning(f"CSV count mismatch! Written: {excluded_files_count}, Expected: {expected_csv_count}")
    
    # Check for accounting discrepancies (all files should be accounted for)
    accounted_for = files_excluded_by_extension + files_examined + files_skipped
    if accounted_for != total_files_found:
        missing = total_files_found - accounted_for
        print(f"\n⚠️  WARNING: Accounting discrepancy detected!")
        logging.warning(f"Accounting discrepancy detected!")
        print(f"  Total files found: {total_files_found}")
        logging.warning(f"  Total files found: {total_files_found}")
        print(f"  Accounted for (extension + examined + skipped): {accounted_for}")
        logging.warning(f"  Accounted for (extension + examined + skipped): {accounted_for}")
        print(f"  Missing from accounting: {missing} files")
        logging.warning(f"  Missing from accounting: {missing} files")
        print(f"  These files may have been silently dropped without proper logging.")
        logging.warning(f"  These files may have been silently dropped without proper logging.")
    
    # Add patients that only have non-DICOM files (weren't in series_by_patient)
    for patient_id in non_dicom_files_by_patient.keys():
        if patient_id not in excluded_patient_data:
            excluded_patient_data[patient_id] = {
                'patient_id': patient_id,
                'excluded_series': [],
                'non_dicom_files': {
                    'count': len(non_dicom_files_by_patient[patient_id]),
                    'reason': 'not in excluded extension list, but not dicom either',
                    'file_paths': non_dicom_files_by_patient[patient_id]
                }
            }
    
    # Close CSV file
    if csv_file:
        csv_file.close()
        print(f"Excluded files CSV saved to: {excluded_csv_path}")
        print(f"Total excluded files logged: {excluded_files_count}")
    
    return {
        'global_summary': global_stats,
        'patient_data': patient_data,
        'excluded_summary': excluded_stats,
        'excluded_patient_data': excluded_patient_data,
        'excluded_files_count': excluded_files_count,
        'non_dicom_files_by_patient': non_dicom_files_by_patient
    }


def format_results_as_json(results):
    """
    Format results as JSON with proper structure.
    
    Args:
        results: Analysis results dictionary
    
    Returns:
        dict: Formatted JSON-ready dictionary
    """
    global_summary = results['global_summary']
    
    # Format global summary
    formatted_global = {
        'total_patients': global_summary['total_patients'],
        'total_studies': global_summary['total_studies'],
        'final_kept_patient_studies': global_summary['final_kept_patient_studies'],
        'total_series_checked': global_summary['total_series_checked'],
        'total_instances': global_summary['total_instances'],
    }
    
    # Add tag occurrence statistics
    for tag_name in TAGS_TO_CHECK.keys():
        formatted_global[f'{tag_name}_occurrences'] = global_summary[f'{tag_name}_occurrences']
    
    # Format SOP Class UIDs with occurrences per tag (with series counts for THAT SPECIFIC TAG)
    formatted_sop_class_uids_with_occurrences = {}
    for tag_name in TAGS_TO_CHECK.keys():
        sop_uids = global_summary['sop_class_uids_per_tag'][tag_name]
        if sop_uids:
            # Create dict with SOP Class UID as key and series count WITH THIS TAG as value
            formatted_sop_class_uids_with_occurrences[tag_name] = {
                sop_uid: global_summary['series_count_per_tag_per_sop_class'][tag_name].get(sop_uid, 0)
                for sop_uid in sorted(sop_uids)
            }
    
    # Format Series UIDs with occurrences per tag
    formatted_series_uids_with_occurrences = {}
    for tag_name in TAGS_TO_CHECK.keys():
        series_uids = global_summary['series_uids_per_tag'][tag_name]
        if series_uids:
            formatted_series_uids_with_occurrences[tag_name] = sorted(list(series_uids))
    
    # Add unified SOP Class UID lists
    formatted_global['sop_class_uids_with_occurrences'] = formatted_sop_class_uids_with_occurrences
    # Format sop_class_uids_without_any_occurrences with correct series counts (only series WITHOUT occurrences)
    formatted_global['sop_class_uids_without_any_occurrences'] = {
        sop_uid: global_summary['series_without_occurrences_per_sop_class'].get(sop_uid, 0)
        for sop_uid in sorted(global_summary['sop_class_uids_without_any_occurrences'])
    }
    
    # Convert SOP Class UIDs set to sorted list with series counts
    formatted_global['sop_class_uids'] = {
        sop_uid: global_summary['series_count_per_sop_class'].get(sop_uid, 0)
        for sop_uid in sorted(global_summary['sop_class_uids'])
    }
    
    # Add additional extensions found in non-DICOM files
    formatted_global['additional_extensions_found'] = sorted(list(results['excluded_summary']['additional_extensions_found']))
    
    # Add unique series descriptions found (only kept series)
    formatted_global['unique_series_descriptions'] = sorted(list(results['excluded_summary']['unique_series_descriptions']))
    
    # Add unique image types found (only kept series) - convert tuples to lists
    formatted_global['unique_image_types'] = sorted([list(img_type_tuple) for img_type_tuple in results['excluded_summary']['unique_image_types']])
    
    # Add unique sequence contents (convert sets to sorted lists for JSON serialization)
    formatted_unique_contents = {}
    for tag_name, content in global_summary['unique_sequence_contents'].items():
        if isinstance(content, set):
            # Convert set to sorted list
            formatted_unique_contents[tag_name] = sorted(list(content))
        else:
            # Keep list as is
            formatted_unique_contents[tag_name] = content
    formatted_global['unique_sequence_contents'] = formatted_unique_contents
    
    # Add Series UID lists per tag
    formatted_global['series_uids_with_occurrences'] = formatted_series_uids_with_occurrences
    
    # Format per-patient data
    formatted_patients = []
    for patient_id, patient_info in sorted(results['patient_data'].items()):
        formatted_patient = {
            'patient_id': patient_id,
            'total_series_checked': patient_info['total_series_checked'],
            'tag_occurrences': {}
        }
        
        # Format tag occurrences as "found/total"
        for tag_name in TAGS_TO_CHECK.keys():
            occurrences = patient_info['tag_occurrences'][tag_name]
            total = patient_info['total_series_checked']
            formatted_patient['tag_occurrences'][tag_name] = {
                'found': occurrences,
                'total': total,
                'percentage': round(100 * occurrences / total, 2) if total > 0 else 0
            }
            # Add series information if any were found
            if patient_info['series_information'][tag_name]:
                formatted_patient['tag_occurrences'][tag_name]['series_information'] = patient_info['series_information'][tag_name]
        
        # Format SOP Class UIDs with occurrences per tag for this patient
        formatted_patient_sop_class_uids_with_occurrences = {}
        for tag_name in TAGS_TO_CHECK.keys():
            sop_uids = patient_info['sop_class_uids_per_tag'][tag_name]
            if sop_uids:
                formatted_patient_sop_class_uids_with_occurrences[tag_name] = sorted(list(sop_uids))
        
        # Add unified SOP Class UID lists for this patient
        if formatted_patient_sop_class_uids_with_occurrences:
            formatted_patient['sop_class_uids_with_occurrences'] = formatted_patient_sop_class_uids_with_occurrences
        if patient_info['sop_class_uids_without_any_occurrences']:
            formatted_patient['sop_class_uids_without_any_occurrences'] = sorted(list(patient_info['sop_class_uids_without_any_occurrences']))
        
        formatted_patient['series_descriptions'] = patient_info['series_descriptions']
        formatted_patients.append(formatted_patient)
    
    return {
        'global_summary': formatted_global,
        'patients': formatted_patients
    }


def save_results(results, output_file):
    """
    Save results to JSON file.
    
    Args:
        results: Formatted results dictionary
        output_file: Output file path
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to: {output_file}")


def format_excluded_results_as_json(results):
    """
    Format excluded series results as JSON.
    
    Args:
        results: Analysis results dictionary
    
    Returns:
        dict: Formatted JSON-ready dictionary for excluded series
    """
    excluded_summary = results['excluded_summary']
    
    formatted_excluded = {
        'global_summary': {
            'total_series_examined': excluded_summary['total_series_examined'],
            'total_series_excluded': excluded_summary['total_series_excluded'],
            'total_non_dicom_files': excluded_summary['total_non_dicom_files'],
            'excluded_series_instance_uids': sorted(list(excluded_summary['excluded_series_instance_uids'])),
            'excluded_sop_class_uids': sorted(list(excluded_summary['excluded_sop_class_uids'])),
            'kept_sop_class_uids': sorted(list(excluded_summary['kept_sop_class_uids'])),
        },
        'non_dicom_files_without_patient_id': {
            'count': 0,
            'reason': 'not in excluded extension list, but not dicom either',
            'file_paths': []
        },
        'patients': []
    }
    
    # Format per-patient excluded series data
    for patient_id, patient_info in sorted(results['excluded_patient_data'].items()):
        # Handle files without patient ID separately
        if patient_id == 'Unknown':
            if patient_info.get('non_dicom_files') and patient_info['non_dicom_files']['count'] > 0:
                formatted_excluded['non_dicom_files_without_patient_id'] = patient_info['non_dicom_files']
            continue
        
        formatted_patient = {
            'patient_id': patient_id,
            'excluded_series': patient_info['excluded_series']
        }
        
        # Add non-DICOM files info if present
        if patient_info.get('non_dicom_files') and patient_info['non_dicom_files']['count'] > 0:
            formatted_patient['non_dicom_files'] = patient_info['non_dicom_files']
        
        # Include patient if they have excluded series or non-DICOM files
        if patient_info['excluded_series'] or (patient_info.get('non_dicom_files') and patient_info['non_dicom_files']['count'] > 0):
            formatted_excluded['patients'].append(formatted_patient)
    
    return formatted_excluded


def print_summary(results):
    """
    Print a summary of the analysis to console.
    
    Args:
        results: Formatted results dictionary
    """
    summary = results['global_summary']
    
    print("\n" + "="*80)
    print("GLOBAL SUMMARY")
    print("="*80)
    logging.info("="*80)
    logging.info("GLOBAL SUMMARY")
    logging.info("="*80)
    print(f"Total Patients: {summary['total_patients']}")
    logging.info(f"Total Patients: {summary['total_patients']}")
    print(f"Total Studies: {summary['total_studies']}")
    logging.info(f"Total Studies: {summary['total_studies']}")
    print(f"Final Kept Patient Studies: {summary['final_kept_patient_studies']}")
    logging.info(f"Final Kept Patient Studies: {summary['final_kept_patient_studies']}")
    print(f"Total Series Checked: {summary['total_series_checked']}")
    logging.info(f"Total Series Checked: {summary['total_series_checked']}")
    print(f"Total Instances: {summary['total_instances']}")
    logging.info(f"Total Instances: {summary['total_instances']}")
    print("\nTag Occurrences:")
    logging.info("")
    logging.info("Tag Occurrences:")
    print("-" * 80)
    logging.info("-" * 80)
    
    for tag_name in TAGS_TO_CHECK.keys():
        occurrences = summary[f'{tag_name}_occurrences']
        total = summary['total_series_checked']
        percentage = 100 * occurrences / total if total > 0 else 0
        print(f"  {tag_name}: {occurrences} / {total} ({percentage:.2f}%)")
        logging.info(f"  {tag_name}: {occurrences} / {total} ({percentage:.2f}%)")
    
    print(f"\nUnique SOP Class UIDs found: {len(summary['sop_class_uids'])}")
    logging.info(f"")
    logging.info(f"Unique SOP Class UIDs found: {len(summary['sop_class_uids'])}")
    for uid in summary['sop_class_uids']:
        print(f"  {uid}")
        logging.info(f"  {uid}")
    
    print("\n" + "="*80)
    logging.info("")
    logging.info("="*80)


def print_excluded_summary(excluded_results):
    """
    Print a summary of excluded series to console.
    
    Args:
        excluded_results: Formatted excluded results dictionary
    """
    summary = excluded_results['global_summary']
    
    print("\n" + "="*80)
    print("EXCLUDED SERIES SUMMARY")
    print("="*80)
    logging.info("="*80)
    logging.info("EXCLUDED SERIES SUMMARY")
    logging.info("="*80)
    print(f"Total Series Examined: {summary['total_series_examined']}")
    logging.info(f"Total Series Examined: {summary['total_series_examined']}")
    print(f"Total Series Excluded: {summary['total_series_excluded']}")
    logging.info(f"Total Series Excluded: {summary['total_series_excluded']}")
    print(f"Total Series Kept: {summary['total_series_examined'] - summary['total_series_excluded']}")
    logging.info(f"Total Series Kept: {summary['total_series_examined'] - summary['total_series_excluded']}")
    print(f"\nUnique Excluded Series: {len(summary['excluded_series_instance_uids'])}")
    logging.info(f"")
    logging.info(f"Unique Excluded Series: {len(summary['excluded_series_instance_uids'])}")
    print(f"Unique Excluded SOP Class UIDs: {len(summary['excluded_sop_class_uids'])}")
    logging.info(f"Unique Excluded SOP Class UIDs: {len(summary['excluded_sop_class_uids'])}")
    print(f"Unique Kept SOP Class UIDs: {len(summary['kept_sop_class_uids'])}")
    logging.info(f"Unique Kept SOP Class UIDs: {len(summary['kept_sop_class_uids'])}")
    
    print("\n" + "="*80)
    logging.info("")
    logging.info("="*80)


def save_excluded_files_log(excluded_files_log, output_file):
    """
    Save excluded files log to a CSV file with structured exclusion reasons.
    
    Args:
        excluded_files_log: List of (file_path, patient_id, rationale_class, rationale_details, series_uid, series_number, study_date) tuples
        output_file: Path to output CSV file
    """
    import csv
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow(['File Path', 'PatientID', 'RationaleClass', 'RationaleDetails', 'SeriesInstanceUID', 'SeriesNumber', 'StudyDate'])
        
        # Write each excluded file
        for file_path, patient_id, rationale_class, rationale_details, series_uid, series_number, study_date in sorted(excluded_files_log):
            writer.writerow([file_path, patient_id, rationale_class, rationale_details, series_uid, series_number, study_date])
    
    print(f"Excluded files log saved to: {output_file}")
    print(f"Total excluded files logged: {len(excluded_files_log)}")


def main(config_path='analyze_config.json'):
    """
    Main execution function for DICOM analysis.
    
    Loads configuration, sets up logging, analyzes DICOM directory for graphics
    and structured content, and saves results to JSON files.
    
    Args:
        config_path: Path to configuration JSON file (default: 'analyze_config.json')
    
    Returns:
        None: Saves analysis results to output files and prints summary
    """
    # Read original config to get relative paths as written
    config_path_obj = Path(config_path).resolve()
    with open(config_path_obj, 'r') as f:
        original_config = json.load(f)
    
    # Load configuration (with resolved absolute paths)
    config = load_config(config_path)
    
    # Get configuration values (absolute paths for operations)
    data_directory = config.get('input_folder')
    output_folder = config.get('output_folder', '.')
    
    # Get original relative paths for logging
    data_directory_display = original_config.get('input_folder', data_directory)
    output_folder_display = original_config.get('output_folder', output_folder)
    
    # Create output folder if it doesn't exist
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True, parents=True)
    
    # Build output file paths relative to output folder
    output_file = output_path / config.get('output_file')
    excluded_output_file = output_path / config.get('excluded_output_file')
    excluded_files_list = output_path / config.get('excluded_files_list')
    log_file = output_path / config.get('log_file')
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            # Don't add StreamHandler to avoid duplicate console output
        ]
    )
    
    logging.info("=" * 80)
    logging.info("Starting DICOM analysis")
    logging.info("=" * 80)
    logging.info(f"Data directory: {data_directory_display}")
    logging.info(f"Output directory: {output_folder_display}")
    logging.info("")
    logging.info("Configuration options:")
    logging.info(f"  min_slices_threshold: {MIN_SLICES_THRESHOLD}")
    logging.info(f"  excluded_series_descriptions: {EXCLUDED_SERIES_DESCRIPTIONS}")
    logging.info(f"  excluded_image_types: {EXCLUDED_IMAGE_TYPES}")
    logging.info(f"  excluded_extensions: {sorted(list(EXCLUDED_EXTENSIONS))}")
    logging.info(f"  excluded_sop_class_uids: {sorted(list(EXCLUDED_SOP_CLASS_UIDS))}")
    logging.info(f"  tags_to_check: {list(TAGS_TO_CHECK.keys())}")
    logging.info("=" * 80)
    
    print(f"Starting DICOM analysis...")
    print(f"Data directory: {data_directory_display}")
    print(f"Minimum slices threshold: {MIN_SLICES_THRESHOLD}")
    print(f"Excluded patterns: {', '.join(EXCLUDED_SERIES_DESCRIPTIONS)}")
    print("\n")
    
    # Analyze data (CSV is written during analysis for memory efficiency)
    results = analyze_dicom_directory(data_directory, excluded_csv_path=excluded_files_list)
    
    # Format results for included series
    formatted_results = format_results_as_json(results)
    
    # Format results for excluded series
    formatted_excluded_results = format_excluded_results_as_json(results)
    
    # Save to files
    save_results(formatted_results, output_file)
    save_results(formatted_excluded_results, excluded_output_file)
    # Note: excluded_files_list CSV already written during analyze_dicom_directory()
    
    # Print summaries
    print_summary(formatted_results)
    print_excluded_summary(formatted_excluded_results)
    
    print("\nAnalysis complete!")
    print(f"Log file saved to: {log_file}")
    
    logging.info("=" * 80)
    logging.info("Analysis completed successfully")
    logging.info("=" * 80)

if __name__ == '__main__':
    # Allow config path to be passed as command line argument
    config_path = sys.argv[1] if len(sys.argv) > 1 else 'analyze_config.json'
    main(config_path)