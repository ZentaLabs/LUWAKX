import pydicom
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict
import cv2
import logging
import json
import sys

# Handle malformed private DICOM tags
pydicom.config.convert_wrong_length_to_UN = True

# Global configuration variables (loaded from config file)
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
    global TAGS_TO_CHECK

    try:
        config_path = Path(config_path).resolve()
        config_dir = config_path.parent

        with open(config_path, 'r') as f:
            config = json.load(f)

        # Convert tags_to_check from list format to tuple format
        # Handle both hex strings (e.g., "0x0070") and integers
        tags_dict = config.get('tags_to_check', {})
        TAGS_TO_CHECK = {}
        for name, tag in tags_dict.items():
            if isinstance(tag[0], str):
                # Convert hex strings to integers
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
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file: {e}")
        sys.exit(1)

class SeriesData:
    """Container for a DICOM series."""
    def __init__(self, patient_id, study_uid, series_uid, series_description, dicom_files, sop_class_uid=None, image_type=None, modality=None, photometric_interpretation=None, study_date=None, series_number=None):
        self.patient_id = patient_id
        self.study_uid = study_uid
        self.series_uid = series_uid
        self.series_description = series_description or "No Description"
        self.dicom_files = sorted(dicom_files)
        self.sop_class_uid = sop_class_uid or "Unknown"
        self.image_type = image_type or []
        self.modality = modality or "Unknown"
        self.photometric_interpretation = photometric_interpretation or "Unknown"
        self.study_date = study_date or "Unknown"
        self.series_number = series_number or "Unknown"
        self.projections = None
        self.has_overlay = False
        self.has_tags = {}  # Dictionary mapping tag names to boolean
        self.overlay_projections = {}  # Dictionary mapping overlay groups to their projections
        self.original_series_uid = None  # Track original UID if series was split
        self.split_reason = None  # Track reason for split: 'orientation' or 'dimension'
        self.split_details = None  # Store split details (orientations or dimensions)

    def __repr__(self):
        return f"Series({self.patient_id}/{self.series_description}/{len(self.dicom_files)} files)"

def scan_all_dicom_files(base_folder):
    """
    Scan base folder recursively for all DICOM files and organize by patient/study/series.
    No predefined folder structure required - just finds all DICOM files.

    Args:
        base_folder: Root directory to scan

    Returns:
        dict: Dictionary mapping patient_id -> list of SeriesData objects
    """
    base_path = Path(base_folder)

    # Find all potential files
    print(f"Scanning directory recursively: {base_folder}")
    all_files = [f for f in base_path.rglob('*') if f.is_file()]
    print(f"Found {len(all_files)} files, checking for DICOM...")

    # Organize by patient/study/series
    series_map = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    dicom_count = 0
    skipped_count = 0

    for file_path in all_files:
        try:
            # Try to read as DICOM
            ds = pydicom.dcmread(file_path, stop_before_pixels=True, force=True)

            # Verify it's valid DICOM
            if not hasattr(ds, 'SOPClassUID') or not hasattr(ds, 'SeriesInstanceUID'):
                skipped_count += 1
                continue

            patient_id = getattr(ds, 'PatientID', 'Unknown')
            study_uid = getattr(ds, 'StudyInstanceUID', 'Unknown')
            series_uid = getattr(ds, 'SeriesInstanceUID', 'Unknown')
            series_desc = getattr(ds, 'SeriesDescription', '')
            modality = getattr(ds, 'Modality', 'Unknown')
            sop_class_uid = getattr(ds, 'SOPClassUID', 'Unknown')
            image_type = getattr(ds, 'ImageType', [])
            photometric = getattr(ds, 'PhotometricInterpretation', 'Unknown')
            study_date = getattr(ds, 'StudyDate', 'Unknown')
            series_number = getattr(ds, 'SeriesNumber', 'Unknown')

            # Initialize series entry if not exists
            if series_uid not in series_map[patient_id][study_uid]:
                series_map[patient_id][study_uid][series_uid] = {
                    'files': [],
                    'description': series_desc,
                    'modality': modality,
                    'sop_class_uid': sop_class_uid,
                    'photometric': photometric,
                    'image_type': list(image_type) if hasattr(image_type, '__iter__') and not isinstance(image_type, str) else [str(image_type)],
                    'study_date': study_date,
                    'series_number': str(series_number) if series_number != 'Unknown' else 'Unknown',
                    'has_tags': {tag_name: False for tag_name in TAGS_TO_CHECK.keys()}
                }

            # Check for presence of tags (check once per series, not per file)
            for tag_name, (group_base, element) in TAGS_TO_CHECK.items():
                if tag_name in ['CurveData', 'OverlayData', 'OverlayComments']:
                    # Check range of groups (50xx, 60xx)
                    if tag_name == 'CurveData':
                        group_range = range(0x5000, 0x5100, 2)
                    else:
                        group_range = range(0x6000, 0x6100, 2)

                    for group in group_range:
                        if (group, element) in ds:
                            series_map[patient_id][study_uid][series_uid]['has_tags'][tag_name] = True
                            break
                else:
                    # Single tag check - check if tag exists in dataset
                    if (group_base, element) in ds:
                        series_map[patient_id][study_uid][series_uid]['has_tags'][tag_name] = True

            # Add file to series
            series_map[patient_id][study_uid][series_uid]['files'].append(file_path)
            dicom_count += 1

        except Exception as e:
            # Not a valid DICOM file, skip
            skipped_count += 1
            continue

    print(f"Found {dicom_count} DICOM files, skipped {skipped_count} non-DICOM files")

    # Convert to per-patient SeriesData objects
    patient_series_map = {}  # patient_id -> list of SeriesData

    for patient_id, studies in series_map.items():
        series_list = []
        for study_uid, series_dict in studies.items():
            for series_uid, series_info in series_dict.items():
                modality = series_info['modality']
                num_files = len(series_info['files'])

                # No modality or slice count filtering - let analyze script handle it

                series_data = SeriesData(
                    patient_id=patient_id,
                    study_uid=study_uid,
                    series_uid=series_uid,
                    series_description=series_info['description'],
                    dicom_files=series_info['files'],
                    sop_class_uid=series_info['sop_class_uid'],
                    image_type=series_info['image_type'],
                    modality=modality,
                    photometric_interpretation=series_info['photometric'],
                    study_date=series_info['study_date'],
                    series_number=series_info['series_number']
                )
                series_data.has_tags = series_info['has_tags']
                series_list.append(series_data)

        if series_list:
            patient_series_map[patient_id] = series_list

    return patient_series_map

def split_series_by_orientation_and_dimension(series_data):
    """
    Split series by ImageOrientationPatient and/or image dimensions in a single pass.
    This efficiently detects mixed orientations and dimensions.

    Args:
        series_data: SeriesData object to potentially split

    Returns:
        list: List of SeriesData objects (original if uniform, split if different)
    """
    # Group files by (orientation, dimension) tuple in a single pass
    composite_map = {}  # (orientation_key, dimension) -> list of file paths

    for dcm_file in series_data.dicom_files:
        try:
            ds_temp = pydicom.dcmread(dcm_file, stop_before_pixels=True)

            # Get orientation
            orientation = None
            if hasattr(ds_temp, 'ImageOrientationPatient'):
                orientation = tuple(ds_temp.ImageOrientationPatient)
                # Find matching orientation with tolerance
                matched_key = None
                for existing_key in [k[0] for k in composite_map.keys() if k[0] is not None]:
                    if orientations_equal(orientation, existing_key):
                        matched_key = existing_key
                        break
                if matched_key is None:
                    matched_key = orientation
                orientation = matched_key

            # Get dimension
            rows = getattr(ds_temp, 'Rows', None)
            cols = getattr(ds_temp, 'Columns', None)
            dimension = (rows, cols) if (rows and cols) else None

            # Composite key
            composite_key = (orientation, dimension)
            if composite_key not in composite_map:
                composite_map[composite_key] = []
            composite_map[composite_key].append(dcm_file)
        except:
            continue

    # If only one group, no split needed
    if len(composite_map) <= 1:
        return [series_data]

    # Determine what caused the split: orientation, dimension, or both
    unique_orientations = set(k[0] for k in composite_map.keys())
    unique_dimensions = set(k[1] for k in composite_map.keys())

    has_multiple_orientations = len(unique_orientations) > 1
    has_multiple_dimensions = len(unique_dimensions) > 1

    # Build split details for logging
    split_details_parts = []
    if has_multiple_orientations:
        orientation_details = []
        for orient in sorted(unique_orientations, key=lambda x: str(x)):
            if orient is not None:
                orient_str = f"[{', '.join([f'{v:.6f}' for v in orient])}]"
                count = sum(len(files) for (o, d), files in composite_map.items() if o == orient)
                orientation_details.append(f"{orient_str} ({count} files)")
        if orientation_details:
            split_details_parts.append("Orientations: " + ", ".join(orientation_details))

    if has_multiple_dimensions:
        dimension_details = []
        for dim in sorted(unique_dimensions, key=lambda x: str(x)):
            if dim is not None:
                count = sum(len(files) for (o, d), files in composite_map.items() if d == dim)
                dimension_details.append(f"{dim[0]}x{dim[1]} ({count} files)")
        if dimension_details:
            split_details_parts.append("Dimensions: " + ", ".join(dimension_details))

    split_details_str = "; ".join(split_details_parts)

    # Determine split reason
    if has_multiple_orientations and has_multiple_dimensions:
        split_reason = 'orientation+dimension'
    elif has_multiple_orientations:
        split_reason = 'orientation'
    else:
        split_reason = 'dimension'

    # Create subseries for each unique (orientation, dimension) combination
    split_series = []
    for idx, ((orientation, dimension), files) in enumerate(sorted(composite_map.items(), key=lambda x: (str(x[0][0]), str(x[0][1])))):
        # Build descriptive suffix
        suffix_parts = []
        if has_multiple_orientations and orientation is not None:
            # Find the index of this orientation among unique orientations
            orient_idx = sorted(unique_orientations, key=lambda x: str(x)).index(orientation)
            suffix_parts.append(f"orient{orient_idx}")
        if has_multiple_dimensions and dimension is not None:
            suffix_parts.append(f"dim{dimension[0]}x{dimension[1]}")

        suffix = "_" + "_".join(suffix_parts) if suffix_parts else f"_sub{idx}"
        new_desc = f"{series_data.series_description}{suffix}"

        new_series = SeriesData(
            patient_id=series_data.patient_id,
            study_uid=series_data.study_uid,
            series_uid=f"{series_data.series_uid}{suffix}",
            series_description=new_desc,
            dicom_files=files,
            sop_class_uid=series_data.sop_class_uid,
            image_type=series_data.image_type,
            modality=series_data.modality,
            photometric_interpretation=series_data.photometric_interpretation,
            study_date=series_data.study_date,
            series_number=series_data.series_number
        )
        new_series.has_tags = series_data.has_tags.copy()
        new_series.original_series_uid = series_data.series_uid
        new_series.split_reason = split_reason
        new_series.split_details = split_details_str
        split_series.append(new_series)

    return split_series

def load_series_volume(series_data, extract_overlays=False, log_callback=None):
    """
    Load DICOM series into a 3D numpy array, handling RGB and grayscale images.

    No filtering applied - assumes analyze script has already excluded non-clinical series.

    Args:
        series_data: SeriesData object
        extract_overlays: If True, extract overlay data as separate arrays
        log_callback: Optional callback function(patient_id, series_uid, series_desc, level, message)

    Returns:
        tuple: (volume, has_overlay, overlay_volume) where:
            - volume: 3D numpy array of pixel data
            - has_overlay: boolean indicating if any overlay was found
            - overlay_volume: dict mapping overlay groups to 3D overlay arrays (if extract_overlays=True)
    """
    try:
        has_overlay = False

        # Use all files without filtering
        files_to_process = series_data.dicom_files

        # Read first file to get dimensions
        ds = pydicom.dcmread(files_to_process[0])

        # Check for overlay in first file using proper tag arithmetic
        for elem in ds:
            group = elem.tag.group
            element = elem.tag.element
            # Check Overlay Data (60xx,3000) - element 0x3000 in even groups 0x6000-0x60FF
            if 0x6000 <= group <= 0x60FF and group % 2 == 0 and element == 0x3000:
                has_overlay = True
                break

        first_array = ds.pixel_array

        # Extract overlays if requested
        overlay_data = {} if extract_overlays else None
        if extract_overlays and has_overlay:
            overlays = extract_overlay_arrays(ds, series_data, log_callback)
            for group, overlay_array in overlays.items():
                if group not in overlay_data:
                    overlay_data[group] = []
                overlay_data[group].append(overlay_array)

        # Check if RGB (3 channels) and convert to grayscale if needed
        if len(first_array.shape) == 3:
            # RGB image - convert to grayscale
            first_array = cv2.cvtColor(first_array, cv2.COLOR_RGB2GRAY)

        expected_shape = first_array.shape

        # Initialize volume with correct dimensions
        num_slices = len(files_to_process)
        volume = np.zeros((num_slices, expected_shape[0], expected_shape[1]))
        volume[0] = first_array

        # Load all remaining slices and check for consistent dimensions
        for i, dcm_file in enumerate(files_to_process[1:], start=1):
            ds = pydicom.dcmread(dcm_file)

            # Check for overlay using proper tag arithmetic
            if not has_overlay:
                for elem in ds:
                    group = elem.tag.group
                    element = elem.tag.element
                    # Check Overlay Data (60xx,3000) - element 0x3000 in even groups 0x6000-0x60FF
                    if 0x6000 <= group <= 0x60FF and group % 2 == 0 and element == 0x3000:
                        has_overlay = True
                        break

            pixel_array = ds.pixel_array

            # Extract overlays if requested
            if extract_overlays and has_overlay:
                overlays = extract_overlay_arrays(ds, series_data, log_callback)
                for group, overlay_array in overlays.items():
                    if group not in overlay_data:
                        overlay_data[group] = []
                    overlay_data[group].append(overlay_array)

            # Convert RGB to grayscale if needed
            if len(pixel_array.shape) == 3:
                pixel_array = cv2.cvtColor(pixel_array, cv2.COLOR_RGB2GRAY)

            # Check if dimensions match
            if pixel_array.shape != expected_shape:
                raise ValueError(
                    f"Inconsistent slice dimensions: expected {expected_shape}, "
                    f"got {pixel_array.shape} at slice {i} of {num_slices} total slices. "
                    f"SeriesInstanceUID: {series_data.series_uid}. "
                    f"This is likely not a volume."
                )

            volume[i] = pixel_array

        # Convert overlay_data lists to 3D arrays if overlays were extracted
        if extract_overlays and overlay_data:
            for group in overlay_data:
                try:
                    overlay_data[group] = np.array(overlay_data[group])
                except:
                    pass

        return volume, has_overlay, (overlay_data if extract_overlays else None)

    except Exception as e:
        msg = f"Error loading volume: {e}"
        if log_callback:
            log_callback(series_data.patient_id, series_data.series_uid,
                       series_data.series_description, 'ERROR', msg)
        else:
            logging.error(f"Patient {series_data.patient_id}, Series '{series_data.series_description}': {msg}")
        print(f"Error loading volume for {series_data}: {e}")
        return None, False, None


def extract_overlay_arrays(ds, series_data=None, log_callback=None):
    """
    Extract all overlay arrays from a DICOM dataset.

    Args:
        ds: pydicom Dataset
        series_data: Optional SeriesData for logging context
        log_callback: Optional callback function for logging

    Returns:
        dict: Dictionary mapping overlay group numbers to overlay arrays
    """
    overlays = {}

    try:
        # Find all overlay groups (60xx,3000)
        for elem in ds:
            group = elem.tag.group
            element = elem.tag.element
            # Check Overlay Data (60xx,3000) - element 0x3000 in even groups 0x6000-0x60FF
            if 0x6000 <= group <= 0x60FF and group % 2 == 0 and element == 0x3000:
                try:
                    # Extract overlay using pydicom's built-in method
                    overlay_array = ds.overlay_array(group)
                    overlays[group] = overlay_array
                except Exception as e:
                    # If extraction fails, log warning
                    warning_msg = f"Overlay group {hex(group)} found but extraction failed: {e}"
                    if log_callback and series_data:
                        log_callback(series_data.patient_id, series_data.series_uid,
                                   series_data.series_description, 'WARNING', warning_msg)
                    else:
                        logging.warning(f"  {warning_msg}")
                        print(f"  Warning: {warning_msg}")

        return overlays

    except Exception as e:
        msg = f"Error extracting overlays: {e}"
        if log_callback and series_data:
            log_callback(series_data.patient_id, series_data.series_uid,
                       series_data.series_description, 'ERROR', msg)
        else:
            logging.error(msg)
        print(f"  Error extracting overlays: {e}")
        return {}

def compute_projections(volume, modality='CT'):
    """Compute projections along axial axis.

    For CT/PET/MR: MIP, MinIP, Mean
    For XA (Angiography): MIP, AIP (Average Intensity Projection), Middle Slice
    For SC (Secondary Capture): Mean, First Slice, Last Slice (often single images/screenshots)
    """
    if volume is None:
        return None

    if modality == 'XA':
        # For angiography: MIP is essential, AIP is useful, middle slice for reference
        projections = {
            'mip': np.max(volume, axis=0),
            'aip': np.mean(volume, axis=0),  # Average Intensity Projection
            'mean': np.mean(volume, axis=0),  # Explicit mean
            'middle_slice': volume[volume.shape[0] // 2, :, :] if volume.shape[0] > 0 else volume[0, :, :],
        }
    elif modality in ['SC', 'OT']:
        # For Secondary Capture: often single images or screenshots
        # Show first and mean (in case there are multiple)
        projections = {
            'first': volume[0, :, :],
            'mean': np.mean(volume, axis=0),
            'last': volume[-1, :, :] if volume.shape[0] > 1 else volume[0, :, :],
        }
    else:
        # For CT/PET/MR: standard projections
        projections = {
            'mip': np.max(volume, axis=0),
            'minip': np.min(volume, axis=0),
            'mean': np.mean(volume, axis=0)
        }

    return projections

def normalize_image(img):
    """Normalize image to 0-255 range."""
    if img is None or img.size == 0:
        return np.zeros((100, 100), dtype=np.uint8)

    img_norm = cv2.normalize(img, None, 0, 255, cv2.NORM_MINMAX)
    return img_norm.astype(np.uint8)

def sanitize_folder_name(name):
    """Convert a string to a valid folder name."""
    # Replace invalid characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '_')
    # Limit length
    return name[:200] if name else "Unknown"

def get_next_plot_index(folder_path, plot_prefix):
    """Get the next available index for plots in a folder."""
    existing_files = list(folder_path.glob(f'{plot_prefix}_*.jpg'))
    if not existing_files:
        return 0

    # Extract indices from existing filenames
    indices = []
    for file in existing_files:
        try:
            # Extract the 4-digit index from filename like 'projection_0000.jpg'
            index_str = file.stem.split('_')[1]
            indices.append(int(index_str))
        except (IndexError, ValueError):
            continue

    return max(indices) + 1 if indices else 0

def save_series_metadata(folder_path, series_list, base_folder, plot_prefix='projection', start_index=0):
    """
    Save metadata JSON for a group of series, merging with existing data if present.

    New structure: Folder-level flag followed by individual plot entries.
    {
        "keep_folder_series": true,  // Folder-level flag to keep/delete entire folder
        "plot_name": {
            "plot_filename": "projection_0001.png",
            "patient_id": "...",
            "series_uid": "1.2.3.4.5...",
            "series_description": "T1 MPRAGE",
            "modality": "MR",
            "image_type": ["ORIGINAL", "PRIMARY"],
            "photometric_interpretation": "MONOCHROME2",
            "sop_class_uid": "1.2.840...",
            "overlay_groups": ["0x6000"],  # Only for overlay plots
            "keep_series": true,
            "file_paths": ["relative/path/to/file1.dcm", ...]
        }
    }
    """
    json_path = folder_path / 'metadata.json'

    # Load existing metadata if it exists
    if json_path.exists():
        with open(json_path, 'r') as f:
            metadata = json.load(f)
    else:
        metadata = {}

    # Ensure folder-level flag exists (preserve if already set, default to True)
    if 'keep_folder_series' not in metadata:
        metadata['keep_folder_series'] = True

    # Add new series with plot names as keys
    base_path = Path(base_folder)
    for i, series in enumerate(series_list):
        plot_name = f"{plot_prefix}_{start_index + i:04d}"
        plot_filename = f"{plot_name}.jpg"

        # Skip if this plot already exists (avoid overwriting)
        if plot_filename in metadata:
            continue

        # Collect file paths for this series
        file_paths = []
        for file_path in series.dicom_files:
            try:
                rel_path = str(Path(file_path).relative_to(base_path))
            except ValueError:
                rel_path = str(file_path)
            file_paths.append(rel_path)

        # Create entry for this series/plot with complete metadata
        entry = {
            'plot_filename': plot_filename,
            'keep_series': True,
            'patient_id': series.patient_id,
            'study_date': series.study_date,
            'series_uid': series.series_uid,
            'series_number': series.series_number,
            'series_description': series.series_description or 'No Description',
            'modality': series.modality,
            'image_type': series.image_type,
            'photometric_interpretation': series.photometric_interpretation,
            'sop_class_uid': series.sop_class_uid,
            'file_paths': file_paths
        }

        # Add overlay groups if this is an overlay plot
        if plot_prefix == 'overlay' and series.overlay_projections:
            overlay_groups = [hex(g) for g in series.overlay_projections.keys()]
            entry['overlay_groups'] = overlay_groups

        # Store original series UID if series was split
        if series.original_series_uid:
            entry['original_series_uid'] = series.original_series_uid

        metadata[plot_filename] = entry

    # Write updated metadata with custom formatting
    json_str = json.dumps(metadata, indent=2)
    # Replace multi-line image_type arrays with single-line format
    import re
    json_str = re.sub(
        r'"image_type":\s*\[\s*([^\]]+?)\s*\]',
        lambda m: '"image_type": [' + ', '.join(m.group(1).replace('\n', '').split(',')) + ']',
        json_str,
        flags=re.DOTALL
    )
    with open(json_path, 'w') as f:
        f.write(json_str)

    return json_path

def log_split_series(series_list, output_folder):
    """
    Log warnings for series that were split, with plot locations.
    Logs once per original series, not per subvolume.

    Args:
        series_list: List of SeriesData objects with plot information
        output_folder: Base output folder for plot locations
    """
    # Group by original series UID
    split_groups = {}  # original_series_uid -> list of subseries

    for series in series_list:
        if series.split_reason and series.original_series_uid:
            if series.original_series_uid not in split_groups:
                split_groups[series.original_series_uid] = []
            split_groups[series.original_series_uid].append(series)

    # Log once per original series
    for series_instance_uid, subseries_list in split_groups.items():
        if not subseries_list:
            continue

        # Get info from first subseries (all share same original metadata)
        first = subseries_list[0]
        split_reason = first.split_reason
        split_details = first.split_details

        # Build plot names list
        plot_names = []
        for series in subseries_list:
            if hasattr(series, 'plot_name'):
                plot_names.append(series.plot_name)

        # Determine split type message
        if split_reason == 'orientation':
            reason_msg = f"different ImageOrientationPatient values: {split_details}"
        elif split_reason == 'dimension':
            reason_msg = f"different image dimensions: {split_details}"
        elif split_reason == 'orientation+dimension':
            reason_msg = f"different orientations and dimensions: {split_details}"
        else:
            reason_msg = f"unknown reason"

        # Build log message
        log_msg = (
            f"SERIES SPLIT DETECTED - Manual review recommended\n"
            f"  Patient: {first.patient_id}\n"
            f"  Series: {first.series_description}\n"
            f"  SeriesNumber: {first.series_number}\n"
            f"  StudyDate: {first.study_date}\n"
            f"  SeriesInstanceUID: {series_instance_uid}\n"
            f"  Split Reason: {reason_msg}\n"
            f"  Number of subvolumes: {len(subseries_list)}\n"
        )

        if plot_names:
            log_msg += f"  Plot files: {', '.join(plot_names)}\n"
            # Get plot folder from first series
            if hasattr(first, 'plot_folder'):
                log_msg += f"  Plot location: {first.plot_folder}\n"

        log_msg += f"  ACTION: Please manually inspect these plots to decide if subvolumes should be kept or removed."

        logging.warning(log_msg)

def create_structured_plots(series_list, output_folder, base_folder):
    """
    Create plots organized by: SOP Class UID / Photometric Interpretation / Tag Type.

    For each series, creates plots in the appropriate folder based on which tags it has:
    - OverlayData/ (if has overlay)
    - AcquisitionContextSequence/ (if has this tag)
    - ContentSequence/ (if has this tag)
    - etc.
    - RegularData/ (if has no special tags)

    Args:
        series_list: List of SeriesData objects with computed projections
        output_folder: Base output folder
        base_folder: Base data folder for relative path calculation
    """
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True, parents=True)

    # Group series by: SOP Class UID -> Photometric Interpretation -> Tag presence
    hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for series in series_list:
        if series.projections is None:
            continue

        sop_uid = series.sop_class_uid
        photometric = series.photometric_interpretation

        # Determine which tag folders this series belongs to
        # Only create separate folders for tags that are in TAGS_TO_CHECK config
        tag_folders = []

        # Check for overlay (only if OverlayData is in config)
        if 'OverlayData' in TAGS_TO_CHECK and series.has_overlay and series.overlay_projections:
            tag_folders.append('OverlayData')

        # Check for CurveData (only if in config)
        if 'CurveData' in TAGS_TO_CHECK and series.has_tags.get('CurveData', False):
            tag_folders.append('CurveData')

        # For other tags or if no special tags, goes to RegularData
        if not tag_folders:
            tag_folders = ['RegularData']

        # Add series to all applicable tag folders
        for tag_folder in tag_folders:
            hierarchy[sop_uid][photometric][tag_folder].append(series)

    # Create plots for each combination
    for sop_uid, photometric_dict in hierarchy.items():
        sop_folder_name = sanitize_folder_name(sop_uid.replace('.', '_'))

        for photometric, tag_dict in photometric_dict.items():
            photometric_folder_name = sanitize_folder_name(photometric)

            for tag_name, series_group in tag_dict.items():
                # Create folder path
                folder_path = output_path / sop_folder_name / photometric_folder_name / tag_name
                folder_path.mkdir(exist_ok=True, parents=True)

                # Get next available index based on existing plots
                if tag_name == 'OverlayData':
                    start_idx = get_next_plot_index(folder_path, 'overlay')
                    plot_prefix = 'overlay'
                else:
                    start_idx = get_next_plot_index(folder_path, 'projection')
                    plot_prefix = 'projection'

                # Save metadata (merges with existing) - pass plot info
                metadata_path = save_series_metadata(folder_path, series_group, base_folder, plot_prefix, start_idx)

                # Create plots (one plot per series)
                for i, series in enumerate(series_group):
                        idx = start_idx + i
                        # Store plot info for later logging
                        if tag_name == 'OverlayData':
                            plot_name = f"overlay_{idx:04d}.jpg"
                            series.plot_name = plot_name
                            series.plot_folder = str(folder_path.relative_to(output_path))
                            # Create overlay plot
                            plot_path = create_single_overlay_plot(series, folder_path, idx)
                        else:
                            plot_name = f"projection_{idx:04d}.jpg"
                            series.plot_name = plot_name
                            series.plot_folder = str(folder_path.relative_to(output_path))
                            # Create regular projection plot
                            plot_path = create_single_projection_plot(series, folder_path, idx)

                        if plot_path:
                            print(f"  Saved plot: {plot_path.relative_to(output_path)}")

    # Log warnings for split series after all plots are created
    log_split_series(series_list, output_folder)

    print(f"\n(SUCCCESS) All plots saved to {output_folder}")

def create_single_projection_plot(series, folder_path, index):
    """Create a single projection plot for a series."""
    proj = series.projections
    num_frames = len(series.dicom_files)

    # For SC/OT modalities, use 1x2 layout with square aspect
    if series.modality in ['SC', 'OT']:
        fig, axes = plt.subplots(1, 2, figsize=(10, 10))

        img_first = normalize_image(proj.get('first', proj.get('mean', list(proj.values())[0])))
        axes[0].imshow(img_first, cmap='gray')
        axes[0].set_title('First/Mean Image', fontsize=10)
        axes[0].axis('off')

        img_second = normalize_image(proj['mean'])
        axes[1].imshow(img_second, cmap='gray')
        axes[1].set_title(f'Mean ({num_frames} frames)', fontsize=10)
        axes[1].axis('off')
    else:
        # For CT/PET/MR/XA: use 2x2 layout (with one empty subplot) for square aspect
        fig, axes = plt.subplots(2, 2, figsize=(12, 12))

        # Top-left: MIP
        img_mip = normalize_image(proj['mip'])
        axes[0, 0].imshow(img_mip, cmap='gray')
        axes[0, 0].set_title(f'MIP ({num_frames} frames)', fontsize=10)
        axes[0, 0].axis('off')

        # Top-right: MinIP or AIP
        if 'minip' in proj:
            img_minip = normalize_image(proj['minip'])
            axes[0, 1].imshow(img_minip, cmap='gray')
            axes[0, 1].set_title(f'MinIP ({num_frames} frames)', fontsize=10)
        else:
            # Fallback for XA which doesn't have minip
            img_minip = normalize_image(proj.get('aip', proj['mean']))
            axes[0, 1].imshow(img_minip, cmap='gray')
            axes[0, 1].set_title(f'AIP ({num_frames} frames)', fontsize=10)
        axes[0, 1].axis('off')

        # Bottom-left: Mean
        img_mean = normalize_image(proj['mean'])
        axes[1, 0].imshow(img_mean, cmap='gray')
        axes[1, 0].set_title(f'Mean ({num_frames} frames)', fontsize=10)
        axes[1, 0].axis('off')

        # Bottom-right: Empty (hide)
        axes[1, 1].axis('off')

    plt.tight_layout()

    output_file = folder_path / f'projection_{index:04d}.jpg'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()

    return output_file

def create_single_overlay_plot(series, folder_path, index):
    """Create a single overlay plot for a series with 2x3 layout showing all projections."""
    proj = series.projections
    overlay_groups = list(series.overlay_projections.keys())
    if not overlay_groups:
        return None

    overlay_proj = series.overlay_projections[overlay_groups[0]]
    num_frames = len(series.dicom_files)

    # Prepare images and titles based on modality
    if series.modality in ['SC', 'OT']:
        # For SC/OT: use 2x2 layout (First and Mean only)
        fig, axes = plt.subplots(2, 2, figsize=(10, 10))

        img_first = normalize_image(proj.get('first', proj.get('mean', list(proj.values())[0])))
        img_second = normalize_image(proj.get('mean', proj.get('first', list(proj.values())[0])))
        overlay_first = normalize_image(overlay_proj.get('first', overlay_proj.get('mean', list(overlay_proj.values())[0])))
        overlay_second = normalize_image(overlay_proj.get('mean', overlay_proj.get('first', list(overlay_proj.values())[0])))

        # Top-left: Overlay First
        axes[0, 0].imshow(overlay_first, cmap='gray')
        axes[0, 0].set_title('Overlay First', fontsize=10)
        axes[0, 0].axis('off')

        # Top-right: Overlay Mean
        axes[0, 1].imshow(overlay_second, cmap='gray')
        axes[0, 1].set_title(f'Overlay Mean ({num_frames} frames)', fontsize=10)
        axes[0, 1].axis('off')

        # Bottom-left: Combined First
        if overlay_first.shape != img_first.shape:
            overlay_first = cv2.resize(overlay_first, (img_first.shape[1], img_first.shape[0]), interpolation=cv2.INTER_NEAREST)
        combined_first = np.maximum(img_first, overlay_first)
        axes[1, 0].imshow(combined_first, cmap='gray')
        axes[1, 0].set_title('Combined First', fontsize=10)
        axes[1, 0].axis('off')

        # Bottom-right: Combined Mean
        if overlay_second.shape != img_second.shape:
            overlay_second = cv2.resize(overlay_second, (img_second.shape[1], img_second.shape[0]), interpolation=cv2.INTER_NEAREST)
        combined_second = np.maximum(img_second, overlay_second)
        axes[1, 1].imshow(combined_second, cmap='gray')
        axes[1, 1].set_title('Combined Mean', fontsize=10)
        axes[1, 1].axis('off')
    else:
        # For CT/PET/MR/XA: use 2x3 layout (MIP, MinIP/AIP, Mean) - square aspect
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))

        # Extract all three projections
        img_mip = normalize_image(proj['mip'])
        img_second = normalize_image(proj.get('minip', proj.get('aip', proj['mip'])))
        img_mean = normalize_image(proj['mean'])

        overlay_mip = normalize_image(overlay_proj.get('mip', list(overlay_proj.values())[0]))
        overlay_second = normalize_image(overlay_proj.get('minip', overlay_proj.get('aip', overlay_mip)))
        overlay_mean = normalize_image(overlay_proj.get('mean', overlay_mip))

        # Determine titles
        second_title = 'MinIP' if 'minip' in proj else 'AIP'

        # Top row: Overlay only
        axes[0, 0].imshow(overlay_mip, cmap='gray')
        axes[0, 0].set_title(f'Overlay MIP ({num_frames} frames)', fontsize=10)
        axes[0, 0].axis('off')

        axes[0, 1].imshow(overlay_second, cmap='gray')
        axes[0, 1].set_title(f'Overlay {second_title} ({num_frames} frames)', fontsize=10)
        axes[0, 1].axis('off')

        axes[0, 2].imshow(overlay_mean, cmap='gray')
        axes[0, 2].set_title(f'Overlay Mean ({num_frames} frames)', fontsize=10)
        axes[0, 2].axis('off')

        # Bottom row: Combined
        # Resize overlays if needed
        if overlay_mip.shape != img_mip.shape:
            overlay_mip = cv2.resize(overlay_mip, (img_mip.shape[1], img_mip.shape[0]), interpolation=cv2.INTER_NEAREST)
        if overlay_second.shape != img_second.shape:
            overlay_second = cv2.resize(overlay_second, (img_second.shape[1], img_second.shape[0]), interpolation=cv2.INTER_NEAREST)
        if overlay_mean.shape != img_mean.shape:
            overlay_mean = cv2.resize(overlay_mean, (img_mean.shape[1], img_mean.shape[0]), interpolation=cv2.INTER_NEAREST)

        combined_mip = np.maximum(img_mip, overlay_mip)
        axes[1, 0].imshow(combined_mip, cmap='gray')
        axes[1, 0].set_title(f'Combined MIP ({num_frames} frames)', fontsize=10)
        axes[1, 0].axis('off')

        combined_second = np.maximum(img_second, overlay_second)
        axes[1, 1].imshow(combined_second, cmap='gray')
        axes[1, 1].set_title(f'Combined {second_title} ({num_frames} frames)', fontsize=10)
        axes[1, 1].axis('off')

        combined_mean = np.maximum(img_mean, overlay_mean)
        axes[1, 2].imshow(combined_mean, cmap='gray')
        axes[1, 2].set_title(f'Combined Mean ({num_frames} frames)', fontsize=10)
        axes[1, 2].axis('off')

    plt.tight_layout()

    output_file = folder_path / f'overlay_{index:04d}.jpg'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()

    return output_file

def main(config_path='analyze_config.json'):
    """
    Main pipeline to process all DICOM files.

    Loads configuration, scans input folder recursively for DICOM files,
    processes patients individually, and saves plots immediately to minimize memory usage.

    Args:
        config_path: Path to configuration JSON file
    """
    # Read original config to get paths as written
    config_path_obj = Path(config_path).resolve()
    with open(config_path_obj, 'r') as f:
        original_config = json.load(f)

    # Load configuration (with resolved absolute paths)
    config = load_config(config_path)

    # Get configuration values (absolute paths for operations)
    base_folder = config.get('input_folder')
    output_folder = config.get('output_folder')
    log_file = Path(config.get('output_folder')) / config.get('plot_pixel_data_file', 'plot-pixel-data.log')

    # Get original relative paths for logging
    base_folder_display = original_config.get('input_folder', base_folder)
    output_folder_display = original_config.get('output_folder', output_folder)

    # Set up logging
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True, parents=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            # Don't add StreamHandler here to avoid duplicate console output
        ]
    )

    # Suppress pydicom's repetitive warnings about malformed DICOM tags
    logging.getLogger('pydicom').setLevel(logging.ERROR)

    logging.info("=" * 80)
    logging.info("Starting plot_pixel_data processing")
    logging.info(f"Input folder: {base_folder_display}")
    logging.info(f"Output folder: {output_folder_display}")
    logging.info("=" * 80)

    # Track errors and warnings per series to avoid duplicates
    series_messages = {}  # (PatientID, SeriesInstanceUID) -> set of (level, message) tuples

    # Helper function to log deduplicated messages per series
    def log_series_message(patient_id, series_uid, series_desc, level, message):
        """Log a message only once per series."""
        key = (patient_id, series_uid)
        if key not in series_messages:
            series_messages[key] = set()

        message_tuple = (level, message)
        if message_tuple not in series_messages[key]:
            series_messages[key].add(message_tuple)
            full_msg = f"Patient {patient_id}, Series '{series_desc}' ({series_uid}): {message}"
            if level == 'ERROR':
                logging.error(full_msg)
            elif level == 'WARNING':
                logging.warning(full_msg)
            elif level == 'INFO':
                logging.info(full_msg)

    print("=" * 80)
    print("STEP 1: Scanning for DICOM files...")
    print("=" * 80)

    # Scan all DICOM files and organize by patient
    patient_series_map = scan_all_dicom_files(base_folder)

    if not patient_series_map:
        print("No valid series found!")
        return

    print(f"\nFound {len(patient_series_map)} patient(s) with valid series\n")

    for patient_idx, (patient_id, series_list) in enumerate(sorted(patient_series_map.items()), 1):
        print("=" * 80)
        print(f"PROCESSING PATIENT {patient_idx}/{len(patient_series_map)}: {patient_id}")
        print("=" * 80)

        # Split by orientation and/or dimension in a single pass
        final_series_list = []
        for series in series_list:
            split = split_series_by_orientation_and_dimension(series)
            if len(split) > 1:
                # Determine what caused the split for better user feedback
                first = split[0]
                if first.split_reason == 'orientation':
                    print(f"  (INFO) Split series '{series.series_description}' into {len(split)} groups by orientation")
                elif first.split_reason == 'dimension':
                    print(f"  (INFO) Split series '{series.series_description}' into {len(split)} groups by dimension")
                else:
                    print(f"  (INFO) Split series '{series.series_description}' into {len(split)} groups by orientation and dimension")
            final_series_list.extend(split)

        series_list = final_series_list

        # Log series info
        print(f"Found {len(series_list)} volume(s) for {patient_id}")
        for series in series_list:
            print(f"  (SUCCESS) {series.modality} volume: {series.series_description} ({len(series.dicom_files)} files)")

        # Compute projections for each series
        print(f"\nComputing projections...")
        for idx, series in enumerate(series_list, 1):
            print(f"  [{idx}/{len(series_list)}] {series.series_description}... ", end='', flush=True)

            # Load volume (without extracting overlays first)
            volume, has_overlay, _ = load_series_volume(series, extract_overlays=False, log_callback=log_series_message)

            if volume is None:
                print("FAILED")
                continue

            # Compute projections (pass modality for appropriate visualization)
            projections = compute_projections(volume, modality=series.modality)
            series.projections = projections
            series.has_overlay = has_overlay

            print(f"OK (shape: {volume.shape}, overlay: {has_overlay})")

            # If overlay detected, extract and compute overlay projections
            if has_overlay:
                print(f"    Extracting overlay data... ", end='', flush=True)
                _, _, overlay_volumes = load_series_volume(series, extract_overlays=True, log_callback=log_series_message)
                if overlay_volumes:
                    # Compute projections for each overlay group
                    for group, overlay_vol in overlay_volumes.items():
                        overlay_proj = compute_projections(overlay_vol, modality=series.modality)
                        series.overlay_projections[group] = overlay_proj
                    print(f"OK (found {len(overlay_volumes)} overlay group(s))")
                    del overlay_volumes
                else:
                    print("FAILED")

            # Free memory immediately
            del volume

        # Create plots for this patient's series immediately (saves memory)
        print(f"\nCreating plots for {patient_id}...")
        create_structured_plots(series_list, output_folder, base_folder)

        # Clear series data to free memory before next patient
        for series in series_list:
            series.projections = None
            series.overlay_projections = {}

        print(f"(SUCCESS) Completed and saved plots for {patient_id}\n")

    print("\n" + "=" * 80)
    print("DONE!")
    print("=" * 80)
    print(f"Output saved to: {output_folder}")
    print(f"Overlay images saved to: {output_folder}/overlay_images")
    print(f"Log file saved to: {log_file}")

    logging.info("=" * 80)
    logging.info("Processing completed successfully")
    logging.info("=" * 80)

# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Allow config path to be passed as command line argument
    config_path = sys.argv[1] if len(sys.argv) > 1 else 'analyze_config.json'
    main(config_path)