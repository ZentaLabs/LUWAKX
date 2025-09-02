# LLM-based PHI/PII Detector

This script `detector.py` processes individual DICOM tags and assesses whether a given tag contains Protected Health Information (PHI) and/or Personally Identifiable Information (PII). It uses a Large Language Model (gpt-oss-20b) as engine.

## Installation
Install requirements with:

```bash
pip install -r requirements.txt
```

## Usage
Basic usage:

```bash
python detector.py \
    --fpath /path/to/file.dcm \
    [--dev_mode]
```

## Arguments
- `--fpath`: Path to the single DICOM file (.dcm) you want to analyze.
- `--dev_mode`: Set this flag to run in development mode (LLM is not triggered, every tag is classified as 0 (no PHI/PII)). For development purposes.

## Output
Table (csv) containing the Tag, Attribute, Value, Value Representation (VR), and PII/PHI classification result.