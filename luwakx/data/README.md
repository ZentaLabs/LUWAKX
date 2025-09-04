# Tag Template Files Provenance

This directory contains several CSV files that define DICOM tag templates for anonymization workflows, based on the 2025b DICOM standards.

## private_tags_template.csv

This CSV file contains a comprehensive template for private DICOM tag anonymization, combining data from:
- TCIA (The Cancer Imaging Archive) Private Tag Knowledge Base (https://wiki.cancerimagingarchive.net/download/attachments/3539047/TCIAPrivateTagKB-02-01-2024-formatted.csv?version=2&modificationDate=1707174689263&api=v2)
- DICOM Standard Safe Private Tags Table E.3.10-1 (https://dicom.nema.org/medical/dicom/current/output/chtml/part15/sect_E.3.10.html)

### How this file was generated

The file was generated using the retrieve_tags.py script:

```
python retrieve_tags.py --create_private_tag_template --merged_private_tags ../data/TagsArchive/private_tags_template.csv
```

This command:
1. Downloads the TCIA Private Tag Knowledge Base CSV
2. Fetches the DICOM Safe Private Tags table
3. Merges and cross-references the data
4. Generates a unified template with anonymization recommendations

## standard_tags_template.csv

This CSV file contains a comprehensive template for standard DICOM tag anonymization, combining data from:
- TCIA Submission and De-identification Overview (Table 1) (https://wiki.cancerimagingarchive.net/display/Public/Submission+and+De-identification+Overview)
- DICOM Standard Anonymization Profile Table E.1-1 (https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E)

### How this file was generated

The file was generated using the retrieve_tags.py script:

```
python retrieve_tags.py --create_standard_tag_template --merged_standard_tags ../data/TagsArchive/standard_tags_template.csv
```

This command:
1. Scrapes the TCIA standard tags table from their wiki
2. Fetches the DICOM standard anonymization table
3. Merges and processes the data with VR (Value Representation) information
4. Applies anonymization profile rules (basic profile, retain options, etc.)
5. Generates a unified template with specific anonymization actions

## deid.dicom.burnedin-pixel-recipe

This file is part of the basic deid recipe collection and contains comprehensive rules for detecting and handling burned-in pixel annotations in DICOM images. Burned-in annotations are text or graphics that have been permanently embedded into the pixel data of medical images and may contain patient identifying information that cannot be removed through standard DICOM header anonymization.

### Purpose

The burned-in pixel recipe defines:
- **Whitelist filters**: Image types and modalities that are known to be clean of burned-in annotations
- **Graylist filters**: Specific regions and coordinates where burned-in text commonly appears in different imaging equipment
- **Equipment-specific rules**: Tailored detection patterns for various manufacturer models and imaging protocols

### Content Structure

The recipe includes detection rules for:
- **CT scanners**: Siemens Sensation 64, GE LightSpeed VCT, Somatom Definition AS+
- **Common burned-in locations**: Dose reports, localizer images, enhancement curves, reconstruction metadata
- **Coordinate-based detection**: Specific pixel regions where annotations typically appear (e.g., `coordinates 0,0,512,121`)

### Integration

This recipe is automatically integrated into the Luwak anonymization pipeline and works alongside the standard DICOM header anonymization to provide comprehensive de-identification by:
1. Identifying images with potential burned-in annotations
2. Flagging problematic regions for manual review or automated masking
3. Ensuring compliance with privacy regulations that require pixel-level anonymization

## File Usage

These files are used by the Luwak anonymization pipeline to:
- Define which private tags should be kept, removed, or anonymized
- Specify anonymization actions for standard DICOM tags
- Detect and handle burned-in pixel annotations in medical images
- Provide metadata for recipe generation and validation

All files ensure reproducible, standards-compliant anonymization workflows based on official DICOM specifications and community best practices, including comprehensive pixel-level de-identification.

# luwak-config.json Description and JSON Schema

## Overview

The `luwak-config.json` file is the main configuration file for the Luwak DICOM anonymization and metadata extraction pipeline. It defines all options for input/output paths, anonymization recipes, metadata export, and workflow parameters.

## Example Structure

```json
{
  "input_folder": "/path/to/dicom/files",
  "output_folder": "/path/to/outputs",
  "deid_recipe": "deid.dicom.basic-profile",
  "private_tags_csv": "data/TagsArchive/DICOM_SAFE_PRIVATE_TAGS.csv",
  "metadata_parquet": "outputs/privateMapping/metadata.parquet",
  "fixed_datetime": "2020-01-01T00:00:00",
  "log_level": "INFO"
}
```

## JSON Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Luwak Config",
  "type": "object",
  "properties": {
    "input_folder": {
      "type": "string",
      "description": "Path to the folder containing DICOM files to process."
    },
    "output_folder": {
      "type": "string",
      "description": "Path to the folder where outputs will be saved."
    },
    "deid_recipe": {
      "type": "string",
      "description": "Filename of the de-identification recipe to use."
    },
    "private_tags_csv": {
      "type": "string",
      "description": "Path to the CSV file containing safe private DICOM tags."
    },
    "metadata_parquet": {
      "type": "string",
      "description": "Path to the Parquet file for exported metadata."
    },
    "log_level": {
      "type": "string",
      "enum": ["DEBUG", "INFO", "WARNING", "ERROR"],
      "description": "Logging level for the pipeline."
    }
  },
  "required": ["input_folder", "output_folder", "deid_recipe", "private_tags_csv", "metadata_parquet"],
  "additionalProperties": false
}
```

## Usage

- Place your configuration in `luwak-config.json` in the project directory.
- Adjust paths and options as needed for your workflow.
- See the example above for typical usage.

For more details, refer to the main project README or script documentation.
