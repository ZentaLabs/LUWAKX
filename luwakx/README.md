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
  "outputDeidentifiedFolder": "/path/to/output/directory",
  "outputPrivateMappingFolder": "/path/to/output/directory/privateMapping",
  "recipesFolder": "./recipes",
  "recipes": ["basic_profile"],
  "outputFolderHierarchy": "copy_from_input",
  "projectHashRoot": "your_encryption_key"
}
```

### Configuration Parameters

- **`inputFolder`**: Path to input DICOM file or directory
- **`outputDeidentifiedFolder`**: Output directory for anonymized files
- **`outputPrivateMappingFolder`**: Directory for private tag mappings
- **`recipesFolder`**: Directory containing deid recipe files
- **`recipes`**: List of recipe names to apply (e.g., `["basic_profile", "retain_safe_private_tags"]`)
- **`outputFolderHierarchy`**: How to structure output (`"copy_from_input"` or `"flat"`)
- **`projectHashRoot`**: Encryption key for anonymization

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

- **`retain_uids`**: Preserves original UIDs
  - Useful for maintaining study relationships

- **`retain_device_identifiers`**: Keeps device-related information
  - Preserves manufacturer, model, software version
  - Maintains equipment traceability

- **`retain_institution_identifiers`**: Keeps institution information
  - Preserves institution name, department
  - Maintains organizational context

- **`retain_patient_characteristics`**: Keeps non-identifying patient data
  - Preserves age, sex, body part examined
  - Maintains clinical context without identification

- **`retain_long_full_dates`**: Keeps complete date information
  - Preserves original dates without shifting
  - Useful for temporal analysis studies

- **`retain_long_modified_dates`**: Applies date shifting instead of removal
  - Shifts dates by consistent offset
  - Maintains temporal relationships while anonymizing

#### Advanced Cleaning Options (manually set)

- **`clean_descriptors`**: Enhanced cleaning of text fields
  - Removes potentially identifying text descriptions
  - Applies advanced text cleaning algorithms

- **`clean_structured_content`**: Cleans structured report content
  - Processes SR (Structured Report) DICOM objects
  - Removes identifying information from structured data

- **`clean_graphics`**: Removes graphic annotations
  - Strips overlay data that might contain identifying information
  - Removes graphic annotations and text overlays

#### Recipe Combination Examples

You can combine multiple recipes for customized anonymization:

```json
{
  "recipes": ["basic_profile", "retain_safe_private_tags", "retain_patient_characteristics"]
}
```

```json
{
  "recipes": ["basic_profile", "retain_long_modified_dates", "clean_descriptors"]
}
```

```json
{
  "recipes": ["retain_uids", "retain_device_identifiers"]
}
```

#### Recipe Priority and Conflicts

When multiple recipes are specified:

1. **Action Priority**: `keep` > `replace` > `remove` 
3. **Conflict Resolution**: Most restrictive action wins (keep > replace > others)

## Usage

### Command Line Interface

Run the script using a JSON configuration file:

```bash
python luwakx.py --config_path /path/to/config.json
```

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
  "outputDeidentifiedFolder": "/data/anonymized",
  "outputPrivateMappingFolder": "/data/anonymized/privateMapping",
  "recipesFolder": "./recipes",
  "recipes": ["retain_safe_private_tags"],
  "outputFolderHierarchy": "copy_from_input",
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
# Debug level (most verbose)
python luwakx.py --config_path config.json --log_level DEBUG

# Info level (default)
python luwakx.py --config_path config.json --log_level INFO

# Warning level
python luwakx.py --config_path config.json --log_level WARNING

# Error level (least verbose)
python luwakx.py --config_path config.json --log_level ERROR
```

### Programmatic Logging

When using the programmatic interface, the logger is automatically configured based on your configuration:

```python
from anonymize import LuwakAnonymizer

# Logger is automatically set up during initialization
anonymizer = LuwakAnonymizer("/path/to/config.json")

# Log file will be created at {outputFolder}/recipes/luwak.log
result = anonymizer.anonymize()
```

### Log Content

The logs include:
- Anonymization process progress and status
- Recipe loading and application details
- File processing information and statistics
- Error messages and troubleshooting information

## Features

- **Config-driven architecture**: Use JSON files for flexible configuration
- **Multiple recipe support**: Apply multiple anonymization recipes simultaneously
- **Comprehensive logging**: Detailed process tracking with configurable log levels
- **Path resolution**: Supports both relative and absolute paths with `{shared_config}` placeholders
- **Hierarchical output**: Preserve or flatten directory structures
- **Performance optimized**: Efficient processing of large DICOM datasets
- **Test coverage**: Comprehensive test suite with automated CI/CD

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

This project is licensed under the MIT License. See the LICENSE file for details.
