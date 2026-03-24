# DICOM IOD Validation Script: `validate_dciodvfy.py`

This script validates DICOM series using [dciodvfy](https://www.dclunie.com/dicom3tools.html) by comparing the output between original and anonymized datasets. It highlights new errors and warnings introduced by anonymization.

## Features
- Runs `dciodvfy` on both original and anonymized DICOM series (matched via `uid_mappings.csv`)
- Reports only **new** errors/warnings found in the anonymized data (not present in the original)
- Outputs:
  - `dciodvfy_validation.csv`: Per-series table of new issues
  - `dciodvfy_summary.log`: Deduplicated summary of unique issues

## Requirements
- Python 3.7+
- [dciodvfy](https://www.dclunie.com/dicom3tools.html) installed and available on your PATH
- [pydicom](https://pydicom.github.io/) (optional, for Modality/SeriesNumber extraction)

## Usage
```sh
python validate_dciodvfy.py \
    --uid_mapping /path/to/uid_mappings.csv \
    --original_folder /path/to/original_data \
    --anonymized_folder /path/to/anonymized_data
```

### Arguments
- `--uid_mapping` : Path to the `uid_mappings.csv` file produced by luwak
- `--original_folder` : Base directory for original (pre-anonymization) DICOM files. `original_file_path` in the CSV is resolved relative to this folder.
- `--anonymized_folder` : Base directory for anonymized DICOM files. `anonymized_file_path` in the CSV is resolved relative to this folder.

### Output
- `dciodvfy_validation.csv` : Table of new errors/warnings per series (saved in the same folder as the `uid_mappings.csv` file)
- `dciodvfy_summary.log` : Deduplicated summary of unique issues (saved in the same folder as the `uid_mappings.csv` file)

## Notes
- Log files (`.log`) and NRRD files (`.nrrd`) are automatically excluded.
- Only new issues in the anonymized series are reported; issues already present in the original are ignored.
- If a series is clean (no new issues), a row with "No new issues found" is written.

## Example
```sh
python validate_dciodvfy.py \
    --uid_mapping /data/privateMapping/uid_mappings.csv \
    --original_folder /data/full_dataset/ \
    --anonymized_folder /data/deidentified/
```

## Troubleshooting
- If you see `dciodvfy` errors about missing files, check that the folder arguments and CSV paths are correct.
