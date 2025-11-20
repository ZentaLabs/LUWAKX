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
  "log_level": "INFO",
  "projectHashRoot": "your_secure_project_key",
  "maxDateShiftDays": 1095,
  "cleanDescriptorsLlmBaseUrl": "https://openrouter.ai/api/v1",
  "cleanDescriptorsLlmModel": "openai/gpt-4o-mini",
  "cleanDescriptorsLlmApiKeyEnvVar": "OPENROUTER_API_KEY"
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
      "enum": ["PRIVATE", "DEBUG", "INFO", "WARNING", "ERROR"],
      "description": "Logging level for the pipeline. PRIVATE includes sensitive data for debugging/audit."
    },
    "cleanDescriptorsLlmBaseUrl": {
      "type": "string",
      "description": "Base URL for the LLM API used for cleaning descriptors (e.g., 'http://localhost:1234/v1' or OpenRouter URL).",
      "default": "http://localhost:1234/v1"
    },
    "cleanDescriptorsLlmModel": {
      "type": "string", 
      "description": "Model name for the LLM used for cleaning descriptors (e.g., 'openai/gpt-oss-20b' for OpenRouter).",
      "default": "openai/gpt-oss-20b"
    },
    "cleanDescriptorsLlmApiKeyEnvVar": {
      "type": "string",
      "description": "Environment variable name containing the API key for the LLM service.",
      "default": "OPENAI_API_KEY"
    },
    "projectHashRoot": {
      "type": "string",
      "description": "Root hash used for deterministic anonymization across the project."
    },
    "maxDateShiftDays": {
      "type": "integer",
      "description": "Maximum number of days for date shifting anonymization.",
      "default": 1095,
      "minimum": 0
    },
    "customTags": {
      "type": "object",
      "properties": {
        "standard": {
          "type": "string",
          "description": "Path to the custom CSV file for standard DICOM tags."
        },
        "private": {
          "type": "string",
          "description": "Path to the custom CSV file for private DICOM tags."
        }
      },
      "additionalProperties": false
    },
    "analysisCacheFolder": {
      "type": "string",
      "description": "Path to folder for analysis cache databases (patient_uid.db and llm_cache.db).
                      If specified and folder exists with databases, they will be loaded and updated; if not, new databases will be created.
                      Databases persist across anonymization runs to ensure consistent mappings.
                      If not specified, temporary databases are created in the private mapping folder and deleted after processing.",
      "examples": ["./analysis_cache", "/var/data/luwak/cache"]
    },
    "testOptions": {
      "type": "object",
      "properties": {
        "useExistingMaskDefacer": {
          "type": "array",
          "items": {
            "type": "string",
            "description": "File path for mask defacer files used in testing."
          }
        }
      },
      "additionalProperties": false
    }
  },
  "required": ["inputFolder", "outputDeidentifiedFolder", "outputPrivateMappingFolder", "recipesFolder", "recipes"],
  "additionalProperties": false
}
```

## Usage

- Place your configuration in `luwak-config.json` in the project directory.
- Adjust paths and options as needed for your workflow.
- See the example above for typical usage.

For more details, refer to the main project README or script documentation.

## Additional Configuration Options

### Manually Revised Tag Files

You can override the default tag templates by specifying custom CSV files for standard and private tags:

```json
"customTags": {
  "standard": "./data/custom_standard_tags.csv",
  "private": "./data/custom_private_tags.csv"
}
```
- If only one is provided, only that template is overridden.
- If the file does not exist, the default template is used.

### Analysis Cache Options

Control persistent storage for patient UID database and LLM cache:

```json
"analysisCacheFolder": "./analysis_cache"
```
- `analysisCacheFolder`: Path to folder for analysis databases (`patient_uid.db` and `llm_cache.db`)
  - If specified: Databases persist in this folder across runs
  - If not specified: Temporary databases are created and deleted after processing
  - Enables consistent patient ID and UID mappings across multiple anonymization runs
  - Enables LLM cache reuse for cost savings and performance

### Test Options

Options for testing and development, including mask defacer support:

```json
"testOptions": {
  "useExistingMaskDefacer": ["/path/to/mask1.nii.gz", "/path/to/mask2.nii.gz"]
}
```
- `useExistingMaskDefacer`: List of file paths for mask defacer files used in testing.

