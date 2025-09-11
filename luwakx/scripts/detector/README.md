# LLM-based PHI/PII Detector

This script `detector.py` processes individual DICOM tags and assesses whether a given tag contains Protected Health Information (PHI) and/or Personally Identifiable Information (PII). It uses a Large Language Model as engine.

## Installation
Install Python requirements with:

```bash
pip install -r requirements.txt
```

There are <b>two options</b> to run the detector:
- Using a local model: In that case, the LLM requires a RestAPI and local server to run. Therefore, download and install LM Studio (LMS): https://lmstudio.ai/download
- Using the OpenAI API Platform with your own API key.

## Hardware for local usage
To run the `gpt-oss-20b` model locally (recommended), you need at least <b>16GB of VRAM</b>.

## Local usage
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
    --model gpt-oss-20b \
    [--dev_mode] \
    --use_local
```
You can stop the local server with:
```
lms server stop
```
For the development mode (`--dev_mode`), no local server is needed.

## API usage
Make sure your API key is set as environment variable:

On macOS/Linux:
```bash
export OPENAI_API_KEY="your_api_key_here"
```
On Windows (Powershell):
```powershell
$env:OPENAI_API_KEY="your_api_key_here"
```

Then, run the detector:
```bash
python detector.py \
    --fpath /path/to/file.dcm \
    --model gpt-4o-mini \
    [--dev_mode] \
```
For the development mode (`--dev_mode`), no API requests will be sent.

## Arguments
- `--fpath`: Path to the single DICOM file (.dcm) you want to analyze.
- `--model`: Name of model (`gpt-oss-20b` recommended for local usage, `gpt-4o-mini` recommended for API usage)
- `--dev_mode`: Set this flag to run in development mode (LLM is not triggered, every tag is classified as 0 (no PHI/PII)). For development purposes.
- `--use_local`: Set this flag to run a local model via a local host instead of using the API.

## Output
Table (csv) containing the Tag, Attribute, Value, Value Representation (VR), PII/PHI classification result, and the runtime in miliseconds (time per tag).
