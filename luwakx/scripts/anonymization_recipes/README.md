# deid.dicom Recipe Files

This directory contains deid-compatible recipe files for DICOM anonymization workflows, based on the 2025b DICOM standards.

## What are deid.dicom recipes?

Deid recipe files define rules for anonymizing DICOM data using the [deid](https://github.com/pydicom/deid) toolkit. These recipes specify which tags to remove, modify, or retain to ensure privacy and compliance.

## Provided Recipes


- `deid.dicom.safe-private-tags`: Generated using the `make_deid_private_tag_file.py` script from the Safe Private Tags CSV (DICOM Table E.3.10-1).
  - **How to generate:**
    ```bash
    python make_deid_private_tag_file.py --input DICOM_SAFE_PRIVATE_TAGS.csv --output deid.dicom.safe-private-tags
    ```
- `deid.dicom.basic-profile`: Generated using the `make_deid_basic_recipe.py` script for the basic DICOM de-identification profile.
  - **How to generate:**
    ```bash
    python make_deid_basic_recipe.py --output deid.dicom.basic-profile
    ```

## Build Recipe Tool

To create or customize deid recipes, use the build recipe tool script:

```bash
python build_recipe_tool.py --input <input_csv> --output <recipe_file>
```

- `--input`: Path to the CSV file containing tag information (e.g., DICOM_SAFE_PRIVATE_TAGS.csv)
- `--output`: Path to save the generated deid recipe file

## Usage

These recipe files can be used directly with the deid toolkit or integrated into your anonymization pipeline.

For more details, refer to the main project documentation or script help messages.
