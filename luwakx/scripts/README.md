# DICOM Private Tag Formatter and Annotator

This script (`format_private_tags.py`) processes and annotates DICOM private tag information from TCIA and the official DICOM standard. It can:
- Download and format the TCIA private tag CSV file
- Download and parse the DICOM safe private tags table (Table E.3.10-1)
- Annotate the TCIA tags with safe private attribute information from the DICOM standard
- Merge and output additional columns (VM, tag meaning) for matching tags
- Optionally output DICOM tags not present in TCIA

## Installation

Install requirements with:

```bash
pip install -r requirements.txt
```

## Usage

### Basic usage (download, format, annotate)

```bash
python format_private_tags.py --input_tcia TCIAPrivateTagKB-02-01-2024-formatted.csv --annotated TCIAPrivateTagKB-annotated.csv
```

### Download DICOM table and annotate, saving non-matching DICOM tags

```bash
python format_private_tags.py --input_tcia TCIAPrivateTagKB-02-01-2024-formatted.csv --annotated TCIAPrivateTagKB-annotated.csv --dicom_table_csv DICOM_SAFE_PRIVATE_TAGS.csv --save_dicom_std_not_in_tcia
```

### Save reformatted TCIA CSV

```bash
python format_private_tags.py --input_tcia TCIAPrivateTagKB-02-01-2024-formatted.csv --reformatted TCIAPrivateTagKB-reformatted.csv --save_reformatted
```

### Full example with all options

```bash
python format_private_tags.py --input_tcia TCIAPrivateTagKB-02-01-2024-formatted.csv --tcia_url <TCIA_CSV_URL> --reformatted TCIAPrivateTagKB-reformatted.csv --annotated TCIAPrivateTagKB-annotated.csv --dicom_url <DICOM_TABLE_URL> --dicom_table_csv DICOM_SAFE_PRIVATE_TAGS.csv --save_dicom_std_not_in_tcia --save_reformatted
```

## Arguments
- `--input_tcia`: Path to TCIA private tag CSV
- `--tcia_url`: URL to download TCIA CSV if not present
- `--reformatted`: Output path for reformatted TCIA CSV
- `--annotated`: Output path for annotated CSV
- `--dicom_url`: URL for DICOM safe private tags table
- `--dicom_table_csv`: Output path for downloaded DICOM table
- `--save_dicom_std_not_in_tcia`: Save DICOM tags not in TCIA
- `--save_reformatted`: Save reformatted TCIA CSV

## Output
- Reformatted TCIA CSV (optional)
- Annotated CSV with safe private attribute info
- DICOM tags not in TCIA (optional)

## Additional Scripts

### Format Standard Tags (`format_standard_tags.py`)

This script processes and formats standard DICOM tags for further use.

#### Usage

```bash
python format_standard_tags.py --input <input_csv> --output <output_csv>
```

#### Arguments
- `--input`: Path to the input CSV file containing standard DICOM tags.
- `--output`: Path to save the formatted standard tags CSV.

#### Example

```bash
python format_standard_tags.py --input dicom_standard_tags.csv --output formatted_standard_tags.csv
```

### Generate Deid Private Tag File (`make_deid_private_tag_file.py`)

This script generates a deid-compatible private tag file from the provided input.

#### Usage

```bash
python make_deid_private_tag_file.py --input <input_csv> --output <output_file>
```

#### Arguments
- `--input`: Path to the input CSV file containing private tag information.
- `--output`: Path to save the generated deid private tag file.

#### Example

```bash
python make_deid_private_tag_file.py --input DICOM_SAFE_PRIVATE_TAGS.csv --output deid.dicom.safe-private-tags
```

## Notes
- Ensure the input files are correctly formatted before running the scripts.
- Output files will be overwritten if they already exist.
