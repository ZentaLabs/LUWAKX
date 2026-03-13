# LuwakX - Config-Driven DICOM Anonymization

LuwakX is a powerful, config-driven tool for anonymizing DICOM files using the `deid` library. It supports flexible configuration through JSON files and provides both command-line and programmatic interfaces.
It is based on the 2025b DICOM standards.

## Prerequisites

1. **Python**: Ensure you have Python >=3.12 installed on your system.
2. **Dependencies**: Install the required dependencies by running:
   ```bash
   pip install -r requirements.txt
   ```

## Architecture

LuwakX uses a config-driven architecture with two main components:

- **`anonymize.py`**: Contains the `LuwakAnonymizer` class for programmatic use
- **`luwakx.py`**: Command-line wrapper script that uses JSON configuration files

## Configuration File Format

Create a JSON configuration file with the following structure:

```json
{
  "inputFolder": "/path/to/input/dicom/files",
  "patientIdPrefix": "Patient",
  "outputDeidentifiedFolder": "/path/to/output/deidentified",
  "outputPrivateMappingFolder": "/path/to/output/privateMapping",
  "recipesFolder": "/path/to/output/recipes",
  "recipes": ["basic_profile"],
  "projectHashRoot": "your_encryption_key",
  "maxDateShiftDays": 1095,
  "excludedTagsFromParquet": ["(7FE0,0010)"],
  "logLevel": "INFO",
  "cleanDescriptorsLlmBaseUrl": "https://api.openai.com/v1",
  "cleanDescriptorsLlmModel": "gpt-4o-mini",
  "cleanDescriptorsLlmApiKeyEnvVar": "ZENTA_OPENAI_API_KEY"
}
```

### Configuration Parameters

#### Required Parameters

- **`inputFolder`**: Path to input DICOM file or directory
- **`outputDeidentifiedFolder`**: Output directory for anonymized files
- **`outputPrivateMappingFolder`**: Directory for private tag mappings and metadata exports
- **`recipesFolder`**: Directory containing deid recipe files
- **`recipes`**: List of recipe names to apply (e.g., `["basic_profile", "retain_safe_private_tags"]`)


#### Optional Parameters

- **`projectHashRoot`**: Salt for deterministic UID generation and date shifting (default: "myproject2025")
- **`maxDateShiftDays`**: Maximum number of days for date shifting (default: 1095)
- **`excludedTagsFromParquet`**: List of DICOM tags to exclude from Parquet export (default: ["(7FE0,0010)"])
- **`logLevel`**: Logging level - PRIVATE, DEBUG, INFO, WARNING, ERROR (default: "INFO")
- **`physicalFacePixelationSizeMm`**: Physical block size (in mm) for face pixelation during defacing (default: 8.5)
- **`keepTempFiles`**: If `true`, temporary directories created during processing (`temp_organized_input`, `temp_defaced_organized`) are retained after the workflow completes. Useful for step-by-step validation of the deidentification pipeline. (default: false)
- **`selectedModalities`**: List of DICOM modalities to include in processing. If empty or not set, all modalities are included. Example: `["MR", "CT"]` (default: empty)


#### LLM Integration Parameters

- **`cleanDescriptorsLlmBaseUrl`**: Base URL for LLM API used in descriptor cleaning (optional)
- **`cleanDescriptorsLlmModel`**: LLM model name for descriptor cleaning (default: "openai/gpt-4o-mini")
- **`cleanDescriptorsLlmApiKeyEnvVar`**: Environment variable name containing the LLM API key (optional)
- **`bypassCleanDescriptorsLlm`**: If `true`, skips the LLM call entirely in `clean_descriptors_with_llm`. The result is always treated as 0 (no PHI detected) and the tag is kept with its original value. Useful for testing or when LLM access is unavailable. (default: false)

#### Analysis Cache Parameters

- **`analysisCacheFolder`**: Folder path for persistent analysis databases (`patient_uid.db` and `llm_cache.db`)
  - If specified: Databases are created/loaded from this folder and persist after processing
  - If not specified: Temporary databases are created in the private mapping folder and deleted after processing
  - Enables consistent mappings across multiple anonymization runs

**Analysis Cache Benefits:**
- **Consistency**: Ensures same patient mappings and UID translations across runs
- **Cost Savings**: LLM cache avoids redundant API calls for previously analyzed content
- **Performance**: Faster processing for repeated content
- **Parallel Safe**: Thread-safe SQLite implementation for concurrent processing

**Example configuration with persistent cache:**
```json
{
  "cleanDescriptorsLlmBaseUrl": "https://api.openai.com/v1",
  "cleanDescriptorsLlmModel": "gpt-4o-mini", 
  "cleanDescriptorsLlmApiKeyEnvVar": "OPENAI_API_KEY",
  "analysisCacheFolder": "./analysis_cache"
}
```

### Built-in Recipes

LuwakX supports multiple anonymization profiles that can be used individually or combined:

#### Basic Anonymization Profiles

- **`basic_profile`**: DICOM basic anonymization profile (Part 15, Table E.1-1)
  - Removes or replaces patient identifiers
  - Applies date/time anonymization
  - Generates new UIDs
  - Handles private tags according to basic profile rules

- **`retain_safe_private_tags`**: Keeps only DICOM-and-TCIA-approved safe private tags
  - Based on DICOM Part 15, Table E.3.10-1 and on TCIA (The Cancer Imaging Archive) Private Tag Knowledge Base
  - Removes potentially identifying private tags
  - Retains safe private tags for research/clinical use

#### Retention Options (can be combined with basic profiles)

- **`retain_uid`**: Preserves original UIDs
  - Useful for maintaining study relationships

- **`retain_device_id`**: Keeps device-related information
  - Preserves manufacturer, model, software version
  - Maintains equipment traceability

- **`retain_institution_id`**: Keeps institution information
  - Preserves institution name, department
  - Maintains organizational context

- **`retain_patient_chars`**: Keeps non-identifying patient data
  - Preserves age, sex, body part examined
  - Maintains clinical context without identification

- **`retain_long_full_dates`**: Keeps complete date information
  - Preserves original dates without shifting
  - Useful for temporal analysis studies

- **`retain_long_modified_dates`**: Applies date shifting instead of removal
  - Shifts dates by consistent offset
  - Maintains temporal relationships while anonymizing

#### Advanced Cleaning Options

- **`clean_descriptors`**: Enhanced cleaning of text fields
  - Removes potentially identifying text descriptions using a large language model (LLM)
  - If PHI/PII is detected in a descriptor, the corresponding DICOM element is deleted from the file and replaced with an empty value

- **`clean_recognizable_visual_features`**: Uses ML-based defacing for images
  - Applies defacing to CT images to remove recognizable features
  - Uses specialized models to detect and anonymize faces


#### Recipe Combination Examples

You can combine multiple recipes for customized anonymization:

```json
{
  "recipes": ["basic_profile", "retain_safe_private_tags", "retain_patient_chars"]
}
```

```json
{
  "recipes": ["basic_profile", "retain_long_modified_dates", "clean_descriptors"]
}
```

```json
{
  "recipes": ["retain_uid", "retain_device_id"]
}
```

#### Recipe Priority and Conflicts

When multiple recipes are specified:

1. **Action Priority**: `keep` > `replace` > `remove` 
3. **Conflict Resolution**: Most restrictive action wins (keep > replace > others)

## Manual Revision of Tag Templates

You can manually revise the standard and private tag templates used for anonymization. This is useful for handling special cases, nested tags (sequences), or custom anonymization requirements.

### How to Revise Tag Templates

1. **Export the current template**  
   - Standard tags: `standard_tags_template.csv`
   - Private tags: `private_tags_template.csv`
   - These files are generated in the `data/TagsArchive` directory by default.

2. **Edit the CSV files**  
   - Open the CSV in a spreadsheet editor or text editor.
   - For **nested tags** (sequences, e.g. VR=`SQ`), you can add or modify rows to specify how child elements should be handled.  
     - Example: For a sequence tag, add rows for each nested element with the parent tag using the following syntax for group/element column: xxxx__item0__xxxx__item1__xxxx where each xxxx represents the group/elemnt. Ex: for a child tag (cccc,dddd) with parent (aaaa,bbbb) that follows this relation `(aaaa,bbbb)__0__(cccc,dddd)` you should fill in the group column with `aaaa__0__cccc` and the element column with `bbbb__0__dddd`.
     - Note: this functionality is currently developed only for standard tags.
   - Modify the action of the profile column desired with `keep`, `remove`, `blank`, `replace` to replace with dummy value based on vr -- you must fill in the vr column, if it is a nested tag specify only the vr of the child tag, `func:generate_hmacuid` if the tag requires UID generation, `func:set_fixed_datetime` if the tag has a vr =DA,DT and you want to apply a dummy value or `func:generate_hmacdate_shift` if you want to apply a date-shift, `func:clean_descriptors_with_llm` if the tag is a text that can contain PHI.

3. **Save your revised files**  
   - Save the edited CSVs to a location of your choice, e.g.:
     - `./data/custom_standard_tags.csv`
     - `./data/custom_private_tags.csv`

4. **Update your config to use the revised files**  
   Add the following to your config JSON:
   ```json
   "customTags": {
     "standard": "./data/custom_standard_tags.csv",
     "private": "./data/custom_private_tags.csv"
   }
   ```
   - You can specify only one of the two if needed.
   - If the file path is invalid or missing, the default template will be used.

## Usage

### Command Line Interface

Run the script using a JSON configuration file:

```bash
python luwakx.py --config_path /path/to/config.json
```

#### Command Line Options

- **`--config_path`** (required): Path to the JSON configuration file
- **`--no-console`** (optional): Disable console logging output; logs will only be written to the log file

#### Logging Behavior

The logging level is controlled by the `logLevel` setting in the configuration file (defaults to "INFO" if not specified). Both command-line and programmatic interfaces use the same config file setting for consistency.

#### Examples

```bash
# Basic usage
python luwakx.py --config_path config.json

# Log only to file (no console output)
python luwakx.py --config_path config.json --no-console
```

**Note**: To change the logging level, modify the `logLevel` property in your configuration file (e.g., set it to "DEBUG" for verbose logging).

### Programmatic Interface

Use the `LuwakAnonymizer` class directly in your Python code:

```python
from anonymize import LuwakAnonymizer

# Initialize with config file
anonymizer = LuwakAnonymizer("/path/to/config.json")

# Run anonymization
result = anonymizer.anonymize()
```

### Example Configurations

**Retain safe private tags:**
```json
{
  "inputFolder": "/data/dicom_files",
  "patientIdPrefix": "Pt",
  "outputDeidentifiedFolder": "/data/anonymized",
  "outputPrivateMappingFolder": "/data/anonymized/privateMapping",
  "recipesFolder": "./recipes",
  "recipes": ["retain_safe_private_tags"],
  "projectHashRoot": "my_secure_key"
}
```

## Logging

LuwakX includes comprehensive logging functionality to track the anonymization process and troubleshoot issues.

### Logging Configuration

The logger automatically creates log files in the output directory structure:
- Log files are saved to `{outputFolder}/recipes/luwak.log` by default
- Centralized logging across all modules using the `luwak_logger` system
- Configurable log levels and output destinations

### Log Levels

You can control logging verbosity using the `--log_level` command line option:

```bash
# Private level (most verbose - includes sensitive data)
python luwakx.py --config_path config.json --log_level PRIVATE

# Debug level (verbose)
python luwakx.py --config_path config.json --log_level DEBUG

# Info level (default)
python luwakx.py --config_path config.json --log_level INFO

# Warning level
python luwakx.py --config_path config.json --log_level WARNING

# Error level (least verbose)
python luwakx.py --config_path config.json --log_level ERROR
```

#### PRIVATE Log Level

The PRIVATE log level (level 5) is a custom log level that captures sensitive information during the anonymization process:

- **Original DICOM element values** before anonymization
- **Patient identifiers** (PatientID, PatientName, PatientBirthDate) used for processing
- **UID mappings** showing original → anonymized transformations
- **Date shift calculations** showing computed offset values
- **Private tag contents** before removal

⚠️ **Security Warning**: Use PRIVATE logging only in secure environments as it logs sensitive patient data. This level is intended for debugging and audit purposes in controlled settings.

### Programmatic Logging

When using the programmatic interface, the logger is automatically configured based on your configuration:

```python
from anonymize import LuwakAnonymizer

# Logger is automatically set up during initialization
anonymizer = LuwakAnonymizer("/path/to/config.json")

# Log file will be created at {outputFolder}/deidentified/luwak.log
result = anonymizer.anonymize()
```

For debugging or audit purposes, you can also configure PRIVATE logging programmatically:

```python
from luwak_logger import setup_logger

# Setup PRIVATE logging (includes sensitive data)
setup_logger(log_level='PRIVATE', log_file='audit.log', console_output=False)

# Then use the anonymizer
anonymizer = LuwakAnonymizer("/path/to/config.json")
result = anonymizer.anonymize()
```

### Log Content

The logs include:
- Anonymization process progress and status
- Recipe loading and application details
- File processing information and statistics
- Error messages and troubleshooting information
- **PRIVATE level only**: Original DICOM values, patient identifiers, UID mappings, and other sensitive data for audit/debugging purposes

## Features

- **Config-driven architecture**: Use JSON files for flexible configuration
- **Multiple recipe support**: Apply multiple anonymization recipes simultaneously
- **Comprehensive logging**: Detailed process tracking with configurable log levels
- **Path resolution**: Supports both relative and absolute paths with `{shared_config}` placeholders
- **Performance optimized**: Efficient processing of large DICOM datasets
- **Test coverage**: Comprehensive test suite with automated CI/CD
- **Metadata export**: Exports anonymized metadata to Parquet format for analysis
- **UID mapping**: Maintains mappings between original and anonymized UIDs for re-identification

## Notes

- The script supports both single DICOM files and directories containing multiple DICOM files
- Output directories are created automatically if they don't exist
- Recipe files are located in `./recipes` by default
- The system supports both string and list formats for recipe specifications
- Path resolution allows for relocatable configurations using placeholders

## Testing

Run the test suite to validate functionality:

```bash
# Run all tests
python -m unittest discover test -v

# Run specific test
python -m unittest test.test_anonymize.TestAnonymizeScript.test_keep_specific_private_tags_should_be_original_value -v
```

## Troubleshooting

- **Missing dependencies**: Ensure all requirements are installed with `pip install -r requirements.txt`
- **Path issues**: Use absolute paths in configuration files or ensure relative paths are correct
- **Recipe errors**: Verify recipe files exist in the specified `recipesFolder`
- **Permission errors**: Ensure write permissions for output directories
- **Performance issues**: For large datasets, monitor the `replace_identifiers` operation which can be time-intensive

## License

