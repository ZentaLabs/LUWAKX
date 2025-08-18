# LuwakX - Config-Driven DICOM Anonymization

LuwakX is a powerful, config-driven tool for anonymizing DICOM files using the `deid` library. It supports flexible configuration through JSON files and provides both command-line and programmatic interfaces.

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
  "recipesFolder": "./scripts/anonymization_recipes",
  "recipes": ["remove_private_tags"],
  "outputFolderHierarchy": "copy_from_input",
  "projectHashRoot": "your_encryption_key"
}
```

### Configuration Parameters

- **`inputFolder`**: Path to input DICOM file or directory
- **`outputDeidentifiedFolder`**: Output directory for anonymized files
- **`outputPrivateMappingFolder`**: Directory for private tag mappings
- **`recipesFolder`**: Directory containing deid recipe files
- **`recipes`**: List of recipe names to apply (e.g., `["remove_private_tags", "retain_safe_private_tags"]`)
- **`outputFolderHierarchy`**: How to structure output (`"copy_from_input"` or `"flat"`)
- **`projectHashRoot`**: Encryption key for anonymization

### Built-in Recipes


- **`retain_safe_private_tags`**: Keeps safe private tags while removing others

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
  "recipesFolder": "./scripts/anonymization_recipes",
  "recipes": ["retain_safe_private_tags"],
  "outputFolderHierarchy": "copy_from_input",
  "projectHashRoot": "my_secure_key"
}
```

## Features

- **Config-driven architecture**: Use JSON files for flexible configuration
- **Multiple recipe support**: Apply multiple anonymization recipes simultaneously
- **Path resolution**: Supports both relative and absolute paths with `{shared_config}` placeholders
- **Hierarchical output**: Preserve or flatten directory structures
- **Performance optimized**: Efficient processing of large DICOM datasets
- **Test coverage**: Comprehensive test suite with automated CI/CD

## Notes

- The script supports both single DICOM files and directories containing multiple DICOM files
- Output directories are created automatically if they don't exist
- Recipe files are located in `./scripts/anonymization_recipes/` by default
- The system supports both string and list formats for recipe specifications
- Path resolution allows for relocatable configurations using placeholders

## Testing

Run the test suite to validate functionality:

```bash
# Run all tests
python -m unittest discover test -v

# Run specific test
python -m unittest test.test_anonymize.TestAnonymizeScript.test_private_tags_removed -v
```

## Troubleshooting

- **Missing dependencies**: Ensure all requirements are installed with `pip install -r requirements.txt`
- **Path issues**: Use absolute paths in configuration files or ensure relative paths are correct
- **Recipe errors**: Verify recipe files exist in the specified `recipesFolder`
- **Permission errors**: Ensure write permissions for output directories
- **Performance issues**: For large datasets, monitor the `replace_identifiers` operation which can be time-intensive

## License

This project is licensed under the MIT License. See the LICENSE file for details.