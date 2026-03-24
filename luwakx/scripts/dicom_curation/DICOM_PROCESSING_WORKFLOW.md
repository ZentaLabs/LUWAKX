# DICOM Data Processing and Review Workflow

This document describes the complete workflow for processing, analyzing, and cleaning DICOM data using the validation scripts.

---

## Overview

The workflow consists of four main phases:

1. **Analysis Phase**: Analyze DICOM files for graphics/structured content tags
2. **Cleanup Phase**: Remove excluded files based on analysis results
3. **Inspection Phase**: Generate visual plots to detect burned-in annotations
4. **Review Phase**: Manual review and selective deletion of problematic series

All scripts use the same configuration file (`analyze_config.json`) for consistency.

---

## Prerequisites

### Required Files
- `analyze_config.json` - Main configuration file
- `analyze_graphics_structured_content.py` - Content analysis script
- `remove_excluded_files.py` - File removal script
- `plot_pixel_data.py` - Plot generation script
- `delete_rejected_series.py` - Series deletion script (optional)

### Configuration File
Ensure your `analyze_config.json` is configured with all the options you want to fill in, for example:

```json
{
  "input_folder": "/path/to/your/dicom/data",
  "output_folder": "/path/to/output/plots",
  "output_file": "included-DICOM-files-analysis.json",
  "excluded_output_file": "dicom-excluded-series-analysis.json",
  "excluded_files_list": "excluded-files-log.csv",
  "log_file": "analyze-graphics-structured-content.log",
  "plot_pixel_data_file": "plot-pixel-data.log",
  "excluded_series_descriptions": [
    {"value": "LOCALIZER", "reason": "Positioning image"},
    {"value": "Scout", "reason": "Positioning image"},
    {"value": "Survey", "reason": "Positioning image"}
  ],
  "excluded_image_types": [
    {"value": ["DERIVED", "MIP"], "reason": "MIP reconstruction"},
    {"value": ["DERIVED", "SECONDARY"], "reason": "Secondary reconstruction"}
  ],
  "excluded_extensions": [
    ".nii.gz",
    ".nii",
    ".json"
  ],
  "excluded_sop_class_uids": [
    {"value": "1.2.840.10008.5.1.4.1.1.7", "reason": "Secondary Capture"}
  ],
  "excluded_series_instance_uids": [
    {"value": "1.2.######", "reason": "Known problematic series"}
  ],
  "min_slices_threshold": 3,
  "tags_to_check": {
    "GraphicAnnotationSequence": ["0x0070", "0x0001"],
    "OverlayData": ["0x6000", "0x3000"],
    "BurnedInAnnotation": ["0x0028", "0x0301"],
    "ContentSequence": ["0x0040", "0xA730"],
    "AcquisitionContextSequence": ["0x0040", "0x0555"],
    "PresentationStateRelationshipSequence": ["0x0070", "0x031A"],
    "IconImageSequence": ["0x0088", "0x0200"],
    "ReferencedImageSequence": ["0x0008", "0x1140"],
    "RadiopharmaceuticalCodeValue": ["0x0054", "0x0016", "RadiopharmaceuticalCodeSequence.CodeValue"]
  }
}
```

**Configuration Parameters Explained:**

- **`input_folder`**: Path to the folder containing your DICOM files. Scripts will scan this folder recursively for all DICOM files. Can be absolute (e.g., `/data/dicoms`) or relative to the config file location (e.g., `../data` or `dicom_files`).

- **`output_folder`**: Path where all output files will be saved. Used by `analyze_graphics_structured_content.py` to save JSON analysis files and logs, and by `plot_pixel_data.py` to organize plot images and metadata by SOP Class / Photometric Interpretation / Tag Type. Can be absolute (e.g., `/results/output`) or relative to the config file location (e.g., `./output` or `analysis_results`).

- **`output_file`**: Filename for the analysis results JSON (included series). Will be saved in `output_folder`. Default: `included-DICOM-files-analysis.json`

- **`excluded_output_file`**: Filename for the excluded series JSON. Will be saved in `output_folder`. Default: `dicom-excluded-series-analysis.json`

- **`excluded_files_list`**: Filename for the CSV file listing all excluded file paths with structured exclusion reasons. Will be saved in `output_folder`. The CSV contains seven columns: `File Path`, `PatientID`, `RationaleClass` (e.g., Reference image, Min Slices, Not DICOM, etc.), `RationaleDetails` (detailed explanation), `SeriesInstanceUID` (when available), `SeriesNumber` (DICOM SeriesNumber tag value), and `StudyDate` (DICOM StudyDate tag value). The CSV is written using a streaming approach (rows are written immediately as files are processed) to minimize memory usage when processing large datasets. Default: `excluded-files-log.csv`

- **`log_file`**: Filename for the detailed processing log from content analysis script. Default: `analyze-graphics-structured-content.log`

- **`pixel_plot_log_file`**: Filename for the log from burned-in pixel detection script. Default: `plot-pixel-data.log`


- **`excluded_series_descriptions`**: List of dictionaries to match against the DICOM **SeriesDescription** tag using **case-insensitive exact matching**. The series description must exactly match (after lowercasing and stripping whitespace) one of the excluded values.
  
  **Configuration Format:**
  ```json
  "excluded_series_descriptions": [
    {"value": "LOCALIZER", "reason": "Positioning image"},
    {"value": "3-Plane Localizer", "reason": "Multi-plane positioning image"},
    {"value": "Scout View Axial", "reason": "Scout image"}
  ]
  ```
  
  **Fields:**
  - `value` (required): String to exactly match against SeriesDescription
  - `reason` (optional): Human-readable explanation for why this series is excluded
  
  **How matching works:**
  - **Exact match only** - the description must match the configured value exactly (after normalization)
  - **Case-insensitive** - "localizer", "LOCALIZER", "Localizer" all match
  - **Whitespace normalized** - leading/trailing whitespace is stripped before comparison
  
  **Examples:**
  ```
  Config: [{"value": "LOCALIZER", "reason": "..."}, {"value": "Scout", "reason": "..."}]
  
   EXCLUDED (exact matches):
    - "LOCALIZER"                    -> matches "LOCALIZER" exactly
    - "localizer"                    -> matches "LOCALIZER" (case-insensitive)
    - "  LOCALIZER  "                -> matches "LOCALIZER" (whitespace stripped)
    - "Scout"                        -> matches "Scout" exactly
  
  ✗ NOT EXCLUDED (not exact matches):
    - "3-Plane Localizer"            -> no exact match
    - "Scout View Axial"             -> no exact match
    - "CT Dose Report Series"        -> no exact match
    - "LOCALIZER_THORAX"             -> no exact match
    - "pre_localizer_scan"           -> no exact match
    - "LOCA"                         -> no exact match
  ```

- **`excluded_image_types`**: List of dictionaries to match against the DICOM **ImageType** tag using **case-insensitive substring matching with AND logic within patterns and OR logic between patterns**.
  
  **Configuration Format:**
  ```json
  "excluded_image_types": [
    {"value": ["DERIVED", "MIP"], "reason": "MIP reconstruction"},
    {"value": ["DERIVED", "SECONDARY"], "reason": "Secondary reconstruction"},
  ]
  ```
  
  **Fields:**
  - `value` (required): String or list of strings. For lists, ALL items must match (AND logic)
  - `reason` (optional): Human-readable explanation for why this pattern is excluded
  
  **How matching works:**
  - **Within each pattern**: ALL items must be found (AND logic)
  - **Between patterns**: ANY pattern can match (OR logic)
  - **Case-insensitive**: "derived", "DERIVED", "Derived" all match
  - **Substring match**: pattern can appear anywhere in ImageType element
  - **Order doesn't matter**: patterns can appear in any position
  
  **Examples with `[["DERIVED", "MIP"]]`:**
  ```
  ImageType in DICOM file:
  
   EXCLUDED (contains both DERIVED and MIP):
    - ["DERIVED", "SECONDARY", "MIP", "AVERAGE"]    -> both found
    - ["ORIGINAL", "DERIVED", "MIP"]                -> both found
    - ["MIP", "DERIVED"]                            -> both found (order doesn't matter)
    - ["derived", "mip"]                            -> both found (case-insensitive)
  
  ✗ NOT EXCLUDED (missing at least one):
    - ["DERIVED", "PRIMARY", "AXIAL"]               -> missing "MIP"
    - ["ORIGINAL", "PRIMARY", "MIP"]                -> missing "DERIVED"
    - ["ORIGINAL", "PRIMARY", "AXIAL"]              -> missing both
  ```
  
  Empty array `[]` means no exclusion by image type.

- **`excluded_extensions`**: List of file extensions to skip during processing. Files with these extensions are not DICOM medical images and will be excluded. Examples: `.nii.gz` (NIfTI), `.nii` (NIfTI), `.json` (metadata), `.xml`, `.txt`, etc.

- **`excluded_sop_class_uids`**: List of dictionaries containing SOP Class UIDs to exclude. When any file in a series has a SOP Class UID matching this list, **all files in that entire series** will be excluded. This allows excluding entire series based on their imaging type (e.g., Secondary Capture, Enhanced CT Image Storage, etc.).
  
  **Configuration Format:**
  ```json
  "excluded_sop_class_uids": [
    {"value": "1.2.840.10008.5.1.4.1.1.7", "reason": "Secondary Capture - not original imaging data"},
    {"value": "1.2.840.10008.5.1.4.1.1.88.11", "reason": "Basic Text SR - structured report"}
  ]
  ```
  
  **Fields:**
  - `value` (required): SOP Class UID string
  - `reason` (optional): Human-readable explanation for why this SOP Class is excluded
  
  Empty array `[]` means no exclusion by SOP Class UID.

- **`excluded_series_instance_uids`**: List of dictionaries containing specific Series Instance UIDs to exclude. When a series' SeriesInstanceUID matches any UID in this list, **all files in that entire series** will be excluded. This allows targeted exclusion of specific problematic series identified during manual review.
  
  **Configuration Format:**
  ```json
  "excluded_series_instance_uids": [
    {"value": "1.2.######", "reason": "Corrupted series identified during QA"},
    {"value": "1.2.#####3", "reason": "Series with annotation artifacts"}
  ]
  ```
  
  **Fields:**
  - `value` (required): Series Instance UID string
  - `reason` (optional): Human-readable explanation for why this specific series is excluded
  
  Empty array `[]` means no exclusion by Series Instance UID.

- **`min_slices_threshold`**: Minimum number of files required in a series for it to be included. The script groups files by **Series Instance UID** and counts how many files share that Series Instance UID. If the count is below this threshold, the entire series is excluded.
  
  **How it works:**
  - During initial scan, the script reads each DICOM file and groups file paths by their Series Instance UID
  - For each series, it counts the number of file paths in that group: `num_slices = len(dicom_files)`
  - If the count is below the threshold, the series is excluded
  
  **What this means:**
  - The threshold is based on **file count**, not actual anatomical slice count
  
  **Why this is useful:**
  - Localizers and scout images typically have 1-3 files per series
  - Clinical series (e.g., full CT scans) typically have dozens to hundreds of files
  - Helps automatically filter out positioning/planning images
  
  **Default:** `3` (excludes series with 1 or 2 files)
  
  **Important:** Enhanced CT Image Storage (SOP Class UID `1.2.840.10008.5.1.4.1.1.2.1`) is not supported and will be skipped with an error log, as it stores multiple slices (frames) in a single file, making file-based counting inappropriate for those series.

- **`tags_to_check`**: Dictionary of DICOM tags to check for graphics/structured content. Each entry can have different formats:
  
  **Standard Tag Format:**
  - **Key**: Descriptive name for the tag (e.g., "OverlayData")
  - **Value**: Array with two elements `[group, element]` in hexadecimal format
  - Example: `"OverlayData": ["0x6000", "0x3000"]`
  - These tags indicate potential burned-in annotations, overlays, or structured content that may need review
  - Scripts check if these tags exist and have non-empty values in DICOM files
  
  **Sequence with Field Filter Format:**
  - **Value**: Array with three elements `[group, element, [field_list]]`
  - Example: `"RadiopharmaceuticalInformationSequence": ["0x0054", "0x0016", ["Radiopharmaceutical", "RadiopharmaceuticalCodeSequence"]]`
  - The third element is a list of field names to extract from the sequence
  - Only the specified fields will be included in the output JSON
  - Nested sequences within filtered fields will show all their contents
  
  **Nested Value Extraction Format:**
  - **Value**: Array with three elements `[group, element, "Path.To.Field"]`
  - Example: `"RadiopharmaceuticalCodeValue": ["0x0054", "0x0016", "RadiopharmaceuticalCodeSequence.CodeValue"]`
  - The third element is a dot-separated path string that navigates through nested sequences
  - Path format: `"NestedSequenceName.FieldName"` (two levels only)
  - Extracts only specific values from deeply nested DICOM sequences
  - Results appear in `unique_sequence_contents` as a sorted list of unique values (e.g., `["C-B1031"]`)
  - Per-series results include `extracted_values` instead of full `sequence_content`
  - Useful for collecting unique coded values like radiopharmaceutical codes, procedure codes, etc.

---

### A Priori Exclusions and Output Files

**Important:** Before/while applying the configuration-based exclusion criteria described above, the script automatically performs several validation checks that result in **a priori exclusions**:

1. **File Extension Filtering**: Files with extensions in `excluded_extensions` are immediately excluded during the initial scan without attempting to read them
2. **Invalid DICOM Files**: Files that cannot be read by `pydicom.dcmread()` are automatically excluded (read errors, corrupted files)
3. **Missing Required Tags**: Files missing **SOPClassUID** or **SeriesInstanceUID** are excluded even if pydicom can partially read them
4. **Missing ImageOrientationPatient Tag**: Files that do not have the **ImageOrientationPatient** tag are automatically excluded and logged with reason: `"Missing ImageOrientationPatient tag"`
5. **Enhanced CT Image Storage**: Series with SOP Class UID `1.2.840.10008.5.1.4.1.1.2.1` are automatically skipped and logged as errors (not supported for analysis)
6. **Reference image Detection within Series**: For series that pass other exclusion criteria, the script detects and excludes reference images that have different orientations from the main acquisition:
   - **ImageOrientationPatient-based detection**: If a series contains files with different ImageOrientationPatient values (difference larger than 1e-5), the script identifies the most common orientation (considered the main series). Files with different orientations are excluded as Reference images **only if there is exactly one file with a different orientation**. If multiple files have different orientations, they are kept (considered part of a valid multi-orientation acquisition). Excluded files are logged with RationaleClass: `"Refernce image: Different ImageOrientationPatient"` and RationaleDetails: `"only 1 file with different orientation"`
   - **Why this matters**: Reference images are typically single positioning images taken in different planes (sagittal, coronal, axial) before the main acquisition. They are often mixed into the same series but have different spatial orientations. This automatic detection removes them without requiring manual configuration.
   - **Important Note**: This detection method is a practical heuristic but not perfect. In DICOM, localizer sequences can have parallel slices (all with the same ImageOrientationPatient), and valid volumetric acquisitions may contain non-parallel slices (e.g., curved reformats or multi-angle acquisitions). However, checking for different ImageOrientationPatient values within a series, and excluding only when there's a single outlier file, remains a simple and effective way to identify most localizer images that are intermixed with the main acquisition.

All excluded files are logged in the `excluded_files_list` output file with their specific exclusion reasons.

---

### Output Files Structure

The `analyze_graphics_structured_content.py` script generates several output files, each serving a specific purpose:

#### **1. `output_file` (Default: `included-DICOM-files-analysis.json`)**

**Purpose**: Contains comprehensive analysis of **kept series** (series that passed all exclusion criteria).

**Structure**:
```json
{
  "global_summary": {
    "total_patients": <int>,
    "total_studies": <int>,  // Total number of unique Study Instance UIDs encountered during processing (all studies)
    "final_kept_patient_studies": <int>,  // Number of studies that contain at least one kept series after all exclusions
    "total_series_checked": <int>,  // Total number of series that passed all exclusion criteria
    "total_instances": <int>,  // Total number of DICOM files in kept series (files_examined - excluded_instances_count)
    "<TagName>_occurrences": <int>,  // Number of series (not files) where this specific tag was found
    "sop_class_uids": {<string>: <int>, ...},  // All unique SOP Class UIDs found in kept series (from reading one file per series during initial scan), with count of series for each SOP Class UID
    "unique_series_descriptions": [<string>, ...],  // Sorted list of all unique SeriesDescription values found in kept series
    "unique_image_types": [<string>, ...],  // Sorted list of all unique ImageType arrays (as tuple strings) found in kept series
    "unique_sequence_contents": {  // For sequence tags
      "<TagName>": [<object>, ...]  // Unique sequence contents found across all kept series (for extracted nested values: sorted list of unique values)
    },
    "sop_class_uids_with_occurrences": {  // For each tag: SOP Class UIDs where that tag was found, with series count
      "<TagName>": {<string>: <int>, ...}  // SOP Class UID -> number of series with THAT TAG and that SOP Class UID
    },
    "sop_class_uids_without_any_occurrences": {<string>: <int>, ...},  // SOP Class UIDs found in series WITHOUT any tag occurrences, with count of series for each
    "series_uids_with_occurrences": {  // Series Instance UIDs containing each tag
      "<TagName>": [<SeriesInstanceUID>, ...]
    },
    "additional_extensions_found": [<string>, ...]  // File extensions found during pydicom.dcmread() that failed DICOM validation (missing SOPClassUID/SeriesInstanceUID) and were NOT in excluded_extensions list
  },
  "patients": [
    {
      "patient_id": <string>,
      "total_series_checked": <int>,
      "tag_occurrences": {
        "<TagName>": {
          "found": <int>,  // Number of series for THIS PATIENT that contain this tag
          "total": <int>,  // Total number of series checked for this patient
          "percentage": <float>,  // Percentage of patient's series with this tag (found/total * 100)
          "series_information": [  // List of series for this patient that contain this tag
            {
              "series_instance_uid": <string>,
              "keep_series": true,  // Always true in kept series output (flag used for manual review)
              "series_description": <string>,
              "sop_class_uid": <string>,  // Primary SOP Class UID from first file in series
              "all_sop_class_uids": [<string>, ...],  // Optional: present only if series contains multiple different SOP Class UIDs
              "file_path": <string>,  // Relative path to first file in series
              "sequence_content": <object> or "extracted_values": [<value>, ...]  // For sequence tags: full structure or extracted nested values
            }
          ]
        }
      },
      "sop_class_uids_with_occurrences": {  // For this patient: SOP Class UIDs per tag
        "<TagName>": [<string>, ...]  // SOP Class UIDs found in this patient's series that contain this tag
      },
      "sop_class_uids_without_any_occurrences": [<string>, ...],  // SOP Class UIDs found in this patient's series WITHOUT any tag occurrences
      "series_descriptions": [<string>, ...]  // List of all series descriptions for this patient's kept series
    }
  ]
}
```

**Key Information**:
- Statistics for series that **passed** all exclusion checks
- Tag occurrence counts are **series counts** (not file/instance counts) - how many series contain each tag
- Series-level details for series containing checked tags, including `keep_series` flag (always true in this file)
- Complete mapping of which series contain which tags
- SOP Class UIDs and sequence contents are collected by reading one file per series during the initial scan
- **Tag detection**: Tags from `tags_to_check` config are searched at the **top level only** of the DICOM dataset (single iteration through dataset elements). Tags are NOT found if they are nested inside other sequences. However, once a sequence tag is found at the top level, its content extraction **does include nested sequences** (all fields within the found sequence are captured, including sequences nested within it)

#### **2. `excluded_output_file` (Default: `dicom-excluded-series-analysis.json`)**

**Purpose**: Contains information about **excluded series** and the reasons for their exclusion.

**Structure**:
```json
{
  "global_summary": {
    "total_series_examined": <int>,  // Total series processed
    "total_series_excluded": <int>,  // Series excluded by any criterion
    "total_non_dicom_files": <int>,  // Files that failed DICOM validation: (1) files with excluded extensions (not read), (2) files that cannot be read by pydicom.dcmread() (read errors/corrupted), (3) files missing required tags (SOPClassUID or SeriesInstanceUID) even if partially readable
    "excluded_series_instance_uids": [<string>, ...],  // All excluded Series Instance UIDs
    "excluded_sop_class_uids": [<string>, ...],  // SOP Class UIDs found in excluded series
    "kept_sop_class_uids": [<string>, ...]  // SOP Class UIDs found in kept series
  },
  "non_dicom_files_without_patient_id": {
    "count": <int>,
    "reason": "not in excluded extension list, but not dicom either",
    "file_paths": [<string>, ...]
  },
  "patients": [
    {
      "patient_id": <string>,
      "excluded_series": [
        {
          "series_instance_uid": <string>,
          "series_description": <string>,
          "sop_class_uid": <string>,  // Primary SOP Class UID from first file in series
          "file_path": <string>,  // Relative path to first file in series
          "num_slices": <int>,  // Number of files in series (len(dicom_files) for this SeriesInstanceUID)
          "exclusion_reason": <string>  // Specific reason for exclusion (e.g., "Series has 2 slices (below minimum threshold of 3)")
        }
      ],
      "non_dicom_files": {  // Optional, only if patient has non-DICOM files
        "count": <int>,
        "reason": "not in excluded extension list, but not dicom either",
        "file_paths": [<string>, ...]
      }
    }
  ]
}
```

**Key Information**:
- **Exclusion reasons** for each series (e.g., "Series has 2 slices (below minimum threshold of 3)", "Series description matches excluded pattern: 'LOCALIZER'", "Series contains excluded SOP Class UID")
- Patient-by-patient breakdown of excluded series
- Non-DICOM files grouped by patient
- Complete audit trail of what was excluded and why

#### **3. `excluded_files_list` (Default: `excluded-files-log.csv`)**

**Purpose**: CSV file listing every excluded file with structured exclusion reasons (used by `remove_excluded_files.py`).

**Format**: CSV with seven columns:
- `File Path`: Relative path to the excluded file
- `PatientID`: Patient ID from DICOM header (empty if not available)
- `RationaleClass`: High-level category of exclusion (e.g., Reference image, Min Slices, Not DICOM, Missing Tag, Excluded Description, Excluded Image Type, Excluded SOP Class, Extension, Other)
- `RationaleDetails`: Detailed explanation of why the file was excluded
- `SeriesInstanceUID`: Series Instance UID (when available, empty otherwise)
- `SeriesNumber`: DICOM SeriesNumber tag value (when available, empty otherwise)
- `StudyDate`: DICOM StudyDate tag value (when available, empty otherwise)

**Important:** File paths in this log are **relative to the `input_folder`** specified in your config file. For example, if `input_folder` is `/data/dicoms` and a file at `/data/dicoms/patient123/image001.dcm` is excluded, the log will contain `patient123/image001.dcm`.

**Example CSV Content**:
```csv
File Path,PatientID,RationaleClass,RationaleDetails,SeriesInstanceUID,SeriesNumber,StudyDate
patient123/image001.dcm,,Extension,.nii.gz,,,
patient456/image002.dcm,PAT456,Not DICOM,Missing SOPClassUID,,,
patient789/image003.dcm,PAT789,Min Slices,Series has 2 slices (below minimum threshold of 3),1.2.840.113619.2.55.3.123456,3,20240115
patient101/image004.dcm,PAT101,Excluded Description,Series description matches excluded pattern: 'LOCALIZER',1.2.840.113619.2.55.3.789012,1,20240115
patient202/image005.dcm,PAT202,Reference image: Different ImageOrientationPatient,only 1 file with different orientation,1.2.840.113619.2.55.3.345678,2,20240115
patient303/image006.dcm,PAT303,Missing Tag,ImageOrientationPatient tag not found,,5,20240116
```

**Key Information**:
- Complete list of file paths to be deleted
- Patient ID for tracking which files belong to which patient
- Structured categorization of exclusion reasons for easy filtering and analysis
- Series Instance UID allows tracking which files belong to the same series
- All files in an excluded series are listed

#### **4. `log_file` (Default: `analyze-graphics-structured-content.log`)**

**Purpose**: Detailed execution log with per-series messages, warnings, and errors.

**Contains**:
- Timestamp for each operation
- Configuration parameters used
- Patient/Series level processing messages
- Error messages (e.g., Enhanced CT Image Storage detected)
- Final summary statistics

**Format**: Standard log format with timestamps, log levels (INFO/WARNING/ERROR), and messages organized by Patient ID and Series Instance UID.

#### **5. `pixel_plot_log_file` (Default: `plot-pixel-data.log`)**

**Purpose**: Log file generated by `plot_pixel_data.py` script during visual inspection plot generation.

**Contains**:
- Execution details for plot generation
- Overlay detection results
- File processing progress
- Any errors encountered during plotting

---

## Phase 1: Initial Data Preparation

### Step 1.1: Create Working Copy

**Always work on a copy of your data** to preserve the original:

**What this does:**
- Protects original data from modifications
- Allows you to restart if something goes wrong

### Step 1.2: Update Configuration

Edit `analyze_config.json` to point to your working copy:

---

## Phase 2: Content Analysis

### Step 2.1: Run Content Analysis

```bash
python analyze_graphics_structured_content.py analyze_config.json
```

**Command parameters:**
- First argument: Path to the configuration file (can be relative or absolute)
- Example: `python analyze_graphics_structured_content.py /full/path/to/analyze_config.json`

**What this script does:**
- Recursively scans all DICOM files in `input_folder`
- Groups files by Patient -> Study -> Series
- Detects reference images (with different orientations, excluding only single-file outliers)
- Checks for DICOM tags specified in `tags_to_check` config (e.g., OverlayData, BurnedInAnnotation, ContentSequence, etc.)
- Excludes series based on:
  - Series description patterns from `excluded_descriptions` config
  - Minimum slice count from `min_slices_threshold` config (default: < 3 slices)
  - Non-DICOM files with extensions in `excluded_extensions` config

**Memory Optimization:**
- **Streaming CSV writes**: Excluded files are written to CSV immediately as they are encountered (not accumulated in memory), drastically reducing memory usage for large-scale processing
- **Per-patient cleanup**: After processing each patient, internal data structures (series_by_patient, series_orientation_map, series_sop_class_map) are explicitly deleted to free memory before moving to the next patient
- **Expected memory usage**: With these optimizations, the script can process millions of files with hundreds of series using moderate memory (typically several GB depending on dataset structure)

**Outputs:**
Output filenames are configurable in `analyze_config.json`:
- Analysis results JSON (default: `included-DICOM-files-analysis.json`) - Statistics for included series
- Excluded series JSON (default: `dicom-excluded-series-analysis.json`) - List of excluded series with reasons
- Exclusion log CSV (default: `excluded-files-log.csv`) - Complete list of excluded file paths with structured reasons
- Log file (default: `analyze-graphics-structured-content.log`) - Detailed processing log

**Expected runtime:** Several minutes to hours depending on dataset size (millions of files)
---

## Phase 3: File Cleanup

### Step 3.1: Review Exclusion Log

Before removing files, review what will be deleted

### Step 3.2: Remove Excluded Files

```bash
python remove_excluded_files.py -d <base_directory> -f <excluded-files-log.csv> [--dry-run]
```

**Command parameters:**
- `-d`, `--directory`: **Required**. Base directory containing your DICOM files (the directory from which relative paths in the log file are resolved)
- `-f`, `--log-file`: **Required**. Path to the exclusion log CSV file (e.g., `excluded-files-log.csv`)
- `--dry-run`: **Optional**. Preview what would be deleted without actually deleting files

**Important:** File paths in the exclusion log are **relative paths** from the base directory. For example:
- If log contains: `patient123/study456/image001.dcm`
- And you specify: `-d /data/dicom_folder`
- Script will delete: `/data/dicom_folder/patient123/study456/image001.dcm`

**Examples:**
```bash
# Preview deletions (dry run - recommended first step)
python remove_excluded_files.py \
  -d input_folder \
  -f excluded-files-log.csv \
  --dry-run

# Actually delete files (requires typing 'DELETE' to confirm)
python remove_excluded_files.py \
  -d -d input_folder \
  -f excluded-files-log.csv
```

**What this script does:**
- Reads file paths from the exclusion log file (tab-separated format)
- Resolves each relative path against the base directory
- Deletes all files listed in the log
- Removes empty directories after file deletion (recursively up to base directory)
- Requires confirmation ('DELETE') before actual deletion (unless dry-run)

**Outputs:**
- Console output showing each file removed/not found
- Summary statistics (files removed, not found, errors, directories cleaned)

**⚠️ Warning:** This permanently deletes files. Ensure you're working on a copy!

### Step 3.3: Run Analysis Again (Recommended)

**After removing the first batch of excluded files, it's highly recommended to run the analysis script a second time.** This iterative approach helps refine the dataset and catch issues that may only become apparent after initial cleanup.

#### Why Run Analysis Again?

1. **Updated Statistics**: Get accurate counts of remaining series and files after cleanup
2. **Identify New Patterns**: With excluded series removed, new patterns may emerge in the remaining data
3. **Refine Exclusion Criteria**: Add newly discovered SOP Class UIDs or description patterns to the config
4. **Verify Cleanup**: Ensure the first cleanup worked as expected

#### Common SOP Class UIDs to Consider Excluding

Based on your review of the first pass results, you might want to exclude:

- **`1.2.840.10008.5.1.4.1.1.7`** - Secondary Capture (often screenshots with burned-in annotations)
- **`1.2.840.10008.5.1.4.1.1.88.67`** - X-Ray Radiation Dose SR (structured reports, not images)

#### Benefits of Multiple Passes

- **Better exclusion accuracy**: Each pass refines the exclusion criteria
- **Cleaner dataset**: Progressive removal of non-clinical and problematic data
- **Reduced manual review time**: Fewer series to manually inspect in Phase 5
- **Documentation**: Each pass creates a log showing what was removed and why

**Recommendation:** Run at least 2 analysis passes and repeat step 3.2 to clean up the data directory as much as possible, before proceeding to the visual inspection phase.

---

## Phase 4: Burned-In Content Detection

### Step 4.1: Generate Inspection Plots

```bash
python plot_pixel_data.py analyze_config.json
```

**What this script does:**
- Scans remaining DICOM files in `input_folder`
- Groups files by Patient -> Study -> Series
- **Dimension-based splitting**: If a series contains files with different image dimensions (Rows × Columns), it is split into separate sub-series, each with consistent dimensions. Each dimension group gets its own projection plots. This handles cases where a series incorrectly contains images of different sizes.
- For each series (or dimension group within a series), computes projection images:
  - **MIP** (Maximum Intensity Projection) - shows brightest pixels
  - **MinIP** (Minimum Intensity Projection) - shows darkest pixels
  - **AIP** (Average Intensity Projection) - for XA modality
  - **First/Mean** - for SC/OT modalities
- Detects overlay data and checks for tags specified in `tags_to_check` config
- **Conditional folder creation**: Creates separate folders for CurveData/OverlayData only if those specific tags are present in `tags_to_check` config. All other tags (like ContentSequence, etc.) are grouped together in a RegularData folder.
- Organizes plots by: SOP Class UID -> Photometric Interpretation -> Tag Type (OverlayData, CurveData, or RegularData)
- Creates `metadata.json` in each folder with series information (including series_number) and file paths

**Outputs:**
```
plot_output/
├── 1_2_840_10008_5_1_4_1_1_2/          # SOP Class UID
│   ├── MONOCHROME2/                     # Photometric Interpretation
│   │   ├── OverlayData/                 # Has overlay tags (only if OverlayData in tags_to_check)
│   │   │   ├── overlay_0000.png
│   │   │   ├── overlay_0001.png
│   │   │   └── metadata.json
│   │   ├── CurveData/                   # Has CurveData tag (only if in tags_to_check)
│   │   │   ├── projection_0000.png
│   │   │   ├── projection_0001.png
│   │   │   └── metadata.json
│   │   └── RegularData/                 # All other series (including those with ContentSequence, etc.)
│   │       ├── projection_0000.png
│   │       └── metadata.json
```

**Note:** Folders for specific tags are only created as follows:
1. **OverlayData folder**: Created only if OverlayData tag is in `tags_to_check` config AND series with overlay data are found
2. **CurveData folder**: Created only if CurveData tag is in `tags_to_check` config AND series with curve data are found
3. **RegularData folder**: Contains all other series, including those with other tags from config (BurnedInAnnotation, ContentSequence, etc.)

**Expected runtime:** Minutes to hours depending on remaining dataset size

**What each plot shows:**
**Projection plots:**
  - For all modalities, the mean projection is always computed and plotted.
  - **SC/OT modalities** (2 panels in 1x2 layout - square aspect):
    - Left: First image
    - Right: Mean (computed for all cases)
  - **CT/PET/MR/XA modalities** (3 panels in 2x2 layout - square aspect, one empty):
    - Top-left: MIP (Maximum Intensity Projection)
    - Top-right: MinIP (Minimum Intensity) or AIP (Average Intensity for XA)
    - Bottom-left: Mean (computed for all cases)
    - Bottom-right: Empty (hidden)
**Overlay plots:**
  - For all modalities, the mean overlay projection is always computed and plotted.
  - **SC/OT modalities** (4 panels in 2x2 layout - square aspect):
    - Top row: Overlay data alone (First, Mean)
    - Bottom row: Combined images (First, Mean)
  - **CT/PET/MR/XA modalities** (2x3 layout):
    - Overlay MIP, Overlay MinIP/AIP, Overlay Mean (top row)
    - Combined MIP, Combined MinIP/AIP, Combined Mean (bottom row)
  - **CT/PET/MR/XA modalities** (6 panels in 2x3 layout - balanced 18x12 figsize): 
    - Top row: Overlay data alone (MIP, MinIP/AIP, Mean)
    - Bottom row: Combined images (MIP, MinIP/AIP, Mean)
  - Combined images use maximum intensity blend (grayscale)
- **Metadata storage**: Plot images do NOT contain titles. All metadata (Patient ID, Series Description, Series UID, Modality, Image Type, SOP Class UID, Overlay Groups, file paths) is stored in `metadata.json` files within each folder. Each entry uses the plot filename as key (e.g., `projection_0000.jpg` or `overlay_0001.jpg`).

**What each metadata.json includes:**

Each `metadata.json` file contains a dictionary with two types of content:

1. **Folder-level flag** (at root level):
   - **`keep_folder_series`**: Boolean flag at the root of the JSON (not inside individual plot entries). Default: `true`. 
     - When set to `false`: **All series in the folder are marked for deletion**, regardless of individual `keep_series` flags (folder-level flag overrides individual flags)
     - When set to `true`: Individual `keep_series` flags are checked for each series (normal behavior)

2. **Plot entries** where **keys are plot filenames with extension** (e.g., `"projection_0000.jpg"`, `"overlay_0001.jpg"`) and **values are metadata objects** for the corresponding DICOM series.

**Per-series metadata fields:**

- **`plot_filename`**: Name of the associated plot file (e.g., `"projection_0000.jpg"` or `"overlay_0001.jpg"`)
- **`keep_series`**: Boolean flag indicating whether to keep (`true`) or delete (`false`) this series. **Default: `true`**. Change to `false` during manual review to mark series for deletion.
- **`patient_id`**: DICOM Patient ID from the series
- **`series_uid`**: DICOM Series Instance UID - unique identifier for the series
- **`series_number`**: DICOM SeriesNumber tag value (integer, when available)
- **`series_description`**: Human-readable series description (e.g., `"T1 MPRAGE"`, `"CT Abdomen Contrast"`)
- **`modality`**: DICOM modality code (e.g., `"CT"`, `"MR"`, `"PT"`, `"XA"`)
- **`image_type`**: DICOM Image Type array as a list (e.g., `["ORIGINAL", "PRIMARY", "AXIAL"]`)
- **`photometric_interpretation`**: Pixel data interpretation (e.g., `"MONOCHROME2"`, `"RGB"`, `"YBR_FULL"`)
- **`sop_class_uid`**: Primary SOP Class UID for the series (e.g., `"1.2.840.10008.5.1.4.1.1.2"`)
- **`overlay_groups`**: *(Only for overlay plots)* List of overlay group numbers as hex strings (e.g., `["0x6000", "0x6002"]`)
- **`file_paths`**: List of **relative paths** (relative to `input_folder`) to all DICOM files in this series

**Example structure:**
```json
{
  "keep_folder_series": true,
  "projection_0000.jpg": {
    "plot_filename": "projection_0000.jpg",
    "keep_series": true,
    "patient_id": "ANON12345",
    "series_uid": "1.2.840.113619.2.55.3.123456789.123",
    "series_number": 3,
    "series_description": "T1 MPRAGE Axial",
    "modality": "MR",
    "image_type": ["ORIGINAL", "PRIMARY", "M", "ND", "NORM"],
    "photometric_interpretation": "MONOCHROME2",
    "sop_class_uid": "1.2.840.10008.5.1.4.1.1.4",
    "file_paths": [
      "patient_ANON12345/study_001/series_003/image_001.dcm",
      "patient_ANON12345/study_001/series_003/image_002.dcm",
      "patient_ANON12345/study_001/series_003/image_003.dcm"
    ]
  },
  "overlay_0001.jpg": {
    "plot_filename": "overlay_0001.jpg",
    "keep_series": true,
    "patient_id": "ANON12345",
    "series_uid": "1.2.840.113619.2.55.3.123456789.456",
    "series_number": 1,
    "series_description": "XA Run 1",
    "modality": "XA",
    "image_type": ["ORIGINAL", "PRIMARY"],
    "photometric_interpretation": "MONOCHROME2",
    "sop_class_uid": "1.2.840.10008.5.1.4.1.1.12.1",
    "overlay_groups": ["0x6000"],
    "file_paths": [
      "patient_ANON12345/study_002/series_001/image_001.dcm",
      "patient_ANON12345/study_002/series_001/image_002.dcm"
    ]
  }
}
```

**Key points:**
- Each metadata.json file corresponds to one folder in the plot output structure
- The `keep_folder_series` field is a folder-level flag (at root level of JSON)
- The `keep_series` field is a **per-series flag** you modify during manual review to mark individual series for deletion
- File paths are relative to the `input_folder` specified in your configuration
- The `overlay_groups` field only appears for plots generated from series with detected overlay data (values are hex strings like `"0x6000"`)

### Step 4.2: Quick Visual Check

Browse through the plot folders to get a sense of the data:

---

## Phase 5: Manual Review and Flagging

### Step 5.1: Review Workflow

For each plot folder, follow this process:

1. **Open the folder** (e.g., `plot_output/.../OverlayData/`)
2. **Look at plot images** one by one (e.g., `overlay_0000.png`)
3. **Check for problems:**
   - Burned-in annotations (text, measurements, labels)
   - Patient identifying information (PHI/PII)
   - Quality issues
   - Unwanted series
4. **Mark for deletion** by editing `metadata.json`


### Step 5.2: Marking Series for Deletion

**Method 1: Mark Individual Series**

Open `metadata.json` in a text editor and change the `keep_series` flag for specific series:

```json
"overlay_0003.jpg": {
  "keep_series": false,    ← Changed from true to false
  "series_uid": "1.2.840...",
  "series_description": "Scout with annotations",
  "file_paths": [...]
}
```

**Method 2: Mark Entire Folder**

To mark **all series in a folder** for deletion at once, change the folder-level flag:

```json
{
  "keep_folder_series": false,    ← Changed from true to false
  "projection_0000.jpg": {
    "keep_series": true,    ← This will be IGNORED when keep_folder_series=false
    ...
  },
  "projection_0001.jpg": {
    "keep_series": true,    ← This will also be IGNORED
    ...
  }
}
```

**⚠️ Important:** When `keep_folder_series` is set to `false`, **ALL series in that folder will be deleted**, regardless of individual `keep_series` flag values. Use this when an entire category (e.g., all OverlayData in a specific SOP Class) should be removed.

### Step 5.3: Review Tips

1. **Work systematically**: Review one folder at a time
2. **Document your decisions**: Keep a separate `review_notes.txt` file in each folder to note why series were rejected (JSON doesn't support comments)
3. **Check overlays carefully**: Overlay data often contains PHI
4. **Save frequently**: Save `metadata.json` after editing each folder

---

## Phase 6: Automated Series Deletion

### Step 6.1: Preview Deletions (Dry Run)

**Always preview before deleting:**

```bash
python delete_rejected_series.py ./plot_output ./clean_dicom_data --dry-run
```

**Command parameters:**
- First argument (`./plot_output`): Folder containing plot outputs with `metadata.json` files - the script will recursively scan this folder for all metadata.json files
- Second argument (`./clean_dicom_data`): Base directory where DICOM files are stored - file paths from metadata.json are resolved relative to this directory
- `--dry-run`: **Optional**. Preview what would be deleted without actually deleting files (highly recommended first step)

**What this does:**
- Scans all `metadata.json` files in the output folder
- Finds all entries with `"keep_series": false` OR folders with `"keep_folder_series": false`
- Shows what would be deleted without actually deleting
- Displays summary statistics

### Step 6.2: Perform Actual Deletion

Once satisfied with the dry run:

```bash
python delete_rejected_series.py ./plot_output ./clean_dicom_data
```

**What this does:**
- Deletes all DICOM files for series marked with `"keep_series": false`
- Creates a detailed deletion log
- Reports summary statistics

**Outputs:**
- `plot_output/deleted_files_YYYYMMDD_HHMMSS.txt` - Complete deletion log

---

## Summary of Script Functions

| Script | Purpose | Input | Output |
|--------|---------|-------|--------|
| `analyze_graphics_structured_content.py` | Analyze DICOM tags and exclude unwanted series | DICOM files + config | JSON analysis + exclusion list |
| `remove_excluded_files.py` | Delete files listed in exclusion log | Exclusion TXT file | Deletion log |
| `plot_pixel_data.py` | Generate visual inspection plots | DICOM files + config | PNG plots + metadata.json |
| `delete_rejected_series.py` | Delete series marked for removal | metadata.json files | Deletion log |
| `review_series_example.py` | Helper for reviewing/marking series | metadata.json | Updated metadata.json |

---

## Questions or Issues?

- Check that all file paths in config are absolute or relative to config location
- Verify Python environment has required packages (pydicom, numpy, matplotlib, opencv-python)
- Review log files for error messages
- Ensure sufficient disk space for plots and working copies
