# Anonymize DICOM Files with anonymize.py

The `anonymize.py` script is a tool for anonymizing DICOM files using the `deid` library. It supports removing private tags and retaining safe private tags based on a recipe.

## Prerequisites

1. **Python**: Ensure you have Python installed on your system.
2. **Dependencies**: Install the required dependencies by running:
   ```bash
   pip install -r requirements.txt
   ```
3. **deid Repository**: The script automatically clones and installs the `deid` repository if not already present.

## Usage

Run the script using the following command:

```bash
python anonymize.py --base <input_path> --output <output_path> [options]
```

### Arguments

- `--base`: Path to the input DICOM file or directory containing DICOM files. (Default: `/path/to/default/input`)
- `--output`: Path to the output directory where anonymized files will be saved. (Default: `~/luwak_output_files`)
- `--deid_recipe`: Path to the deid recipe file. (Default: `deid.dicom`)
- `--safe_private_tags`: Path to the safe private tags recipe file. (Default: `./scripts/anonymization_recipes/deid.dicom.safe-private-tags`)
- `--retain_safe_private_tags`: Whether to retain safe private tags. Accepts `True` or `False`. (Default: `True`)

### Example

To anonymize a single DICOM file and retain safe private tags:

```bash
python anonymize.py \
  --base /path/to/dicom/file.dcm \
  --output /path/to/output/directory \
  --retain_safe_private_tags True
```

To anonymize all DICOM files in a directory and remove all private tags:

```bash
python anonymize.py \
  --base /path/to/dicom/directory \
  --output /path/to/output/directory \
  --retain_safe_private_tags False
```

## Notes

- The script supports both single DICOM files and directories containing multiple DICOM files.
- If the output directory does not exist, it will be created automatically.
- Ensure the `safe_private_tags` recipe is correctly configured to retain the desired private tags.

## Troubleshooting

- If the script fails to run, ensure all dependencies are installed and the `deid` repository is properly set up.
- Check the console output for error messages and verify the input paths and arguments.

## License

This project is licensed under the MIT License. See the LICENSE file for details.