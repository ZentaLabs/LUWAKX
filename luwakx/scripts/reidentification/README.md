# Reidentification Lookup Scripts

This directory contains utilities to map deidentified identifiers back to original patient/study/series identifiers using `uid_mappings.db`.

## Scripts

### `reidentify.py`

General-purpose lookup tool that resolves original IDs from one or more deidentified inputs.

Supported input types:
- rendered defacing filenames
- deidentified series folders
- anonymized SeriesInstanceUID values
- anonymized StudyInstanceUID values
- anonymized PatientID values

The script accepts multiple values for each input option and can output either human-readable text or CSV.

#### Usage

```bash
python reidentify.py --db /path/to/privateMapping/uid_mappings.db <input-options>
```

#### Examples

```bash
# 1) Rendered filenames (stem, .jpg, or full path)
python reidentify.py \
  --db /data/20260515-deidentification-project/privateMapping/uid_mappings.db \
  --deidentified-rendering-filename \
    Patient0001_N87sJg_mLwD8LECi_FONvkMP-Y9FsfbCM_image_defaced \
    Patient0001_N87sJg_mLwD8LECi_FONvkMP-Y9FsfbCM_image_defaced.jpg

# 2) Deidentified folders (absolute and relative)
python reidentify.py \
  --db /data/20260515-deidentification-project/privateMapping/uid_mappings.db \
  --deidentified-root /data/20260515-deidentification-project/deidentified \
  --deidentified-folder \
    /data/20260515-deidentification-project/deidentified/Patient0001/N87sJg_mLwD8LECi/7tHKG3kGa4A0CdAL \
    deidentified/Patient0001/N87sJg_mLwD8LECi/gqiX3pU4VVYWVSoD

# 3) UID and patient lookups (multiple values)
python reidentify.py \
  --db /data/20260515-deidentification-project/privateMapping/uid_mappings.db \
  --series-instance-uid 1.2.826.0.1.3680043.8.498.36853424709594650976742490222855776628 \
  --study-instance-uid 1.2.826.0.1.3680043.8.498.54702968860461091216502878401246253993 \
  --patient-id Patient0001 Patient0009

# 4) CSV output
python reidentify.py \
  --db /data/20260515-deidentification-project/privateMapping/uid_mappings.db \
  --deidentified-rendering-filename Patient0001_N87sJg_mLwD8LECi_FONvkMP-Y9FsfbCM_image_defaced \
  --output-format csv
```

#### Arguments

- `--db`: Path to `uid_mappings.db` (required)
- `--deidentified-root`: Root folder containing `PatientXXXX/<study>/<series>` (optional)

Input selectors (all optional, at least one selector is required):
- `--deidentified-rendering-filename`: One or more rendered names/paths, e.g. `Patient0001_..._image_defaced` or `.jpg`
- `--deidentified-folder`: One or more deidentified series folders
- `--series-instance-uid`: One or more anonymized SeriesInstanceUID values
- `--study-instance-uid`: One or more anonymized StudyInstanceUID values
- `--patient-id`: One or more anonymized PatientID values

Output options:
- `--output-format {txt,csv}`: Output mode (`txt` default)

#### Output

- `txt` (default): grouped per input, showing parsed anonymized identifiers and matching original identifiers/paths.
- `csv`: one row per match with:
  - input source and value
  - anonymized patient/study/series identifiers
  - original patient/study/series identifiers
  - original file folder
  - status (`ok`, `no_match`, or `error`)

## Requirements

- Python 3.10+
- `pydicom`
- SQLite mapping database with `Instance` table (typically `privateMapping/uid_mappings.db`)
