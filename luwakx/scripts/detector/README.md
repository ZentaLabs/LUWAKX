# LLM-based PHI/PII Detector

This script `detector.py` processes individual DICOM tags and assesses whether a given tag contains Protected Health Information (PHI) and/or Personally Identifiable Information (PII). It uses a Large Language Model (gpt-oss-20b) as engine.

## Installation
Install Python requirements with:

```bash
pip install -r requirements.txt
```

The LLM requires a RestAPI and local server to run. Therefore, download and install LM Studio (LMS): https://lmstudio.ai/download

## Hardware
To run the `gpt-oss-20b` model locally, you need at least <b>16GB of VRAM</b>.

## Usage
Open terminal, and download the `gpt-oss-20b` model via LMS:
```
lms get openai/gpt-oss-20b
```
Load the model:
```
lms load openai/gpt-oss-20b
```
Start the local server:
```
lms server start
```
Then, run the detector:
```bash
python detector.py \
    --fpath /path/to/file.dcm \
    [--dev_mode]
```
You can stop the local server with:
```
lms server stop
```
For the development mode (`--dev_mode`), no local server is needed.

## Arguments
- `--fpath`: Path to the single DICOM file (.dcm) you want to analyze.
- `--dev_mode`: Set this flag to run in development mode (LLM is not triggered, every tag is classified as 0 (no PHI/PII)). For development purposes.

## Output
Table (csv) containing the Tag, Attribute, Value, Value Representation (VR), and PII/PHI classification result.