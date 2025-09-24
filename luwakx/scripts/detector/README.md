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

## Benchmarks
#### Original Dataset
The validation (n=216 studies, XXX series) and test (n=322 studies, XXX series) datasets from the MIDI-B De-identification Challenge were used.

Download: https://www.cancerimagingarchive.net/collection/midi-b-test-midi-b-validation/

#### Benchmark Dataset
Starting from the original dataset, we treated the first Dicom file of each series (within each study) as the representative file for that series. From each representative file we extracted all free-text/annotations fields and any private or vendor-specific Dicom tags. Finally, any Dicom field whose Value Representation (VR) is UI (Unique Identifier) was removed from the extracted data.

The following <b>free-text</b> tags were extracted if present:

| Tag | Attribute |
|:---|---|
| (0x0008, 0x103E) | Series Description |
| (0x0008, 0x1030) | Study Description |
| (0x0020, 0x4000) | Image Comments |
| (0x0040, 0x0254) | Performed Procedure Step Description |
| (0x0040, 0x0275) | Request Attributes Sequence |
| (0x0010, 0x4000) | Patient Comments |
| (0x0038, 0x0040) | Admitting Diagnoses Description |
| (0x0032, 0x1060) | Requested Procedure Description |
| (0x0018, 0x0015) | Body Part Examined |

<b>Private tags</b> were defined by the `pydicom.is_private()` function from the `pydicom` library. It returns `True` for private-element tags (odd group number) and `False` otherwise. For example: `(0019, xxxx)` or `(0029, xxxx)` are private tags.

The <b>resulting benchmark dataset</b> contains n=14,989 (free-text: 1,106 free-text, private: 13,883) and n=21,793 (free-text: 1,651 free-text, private: 20,142) Dicom tags in the validation and test dataset.

#### Ground truths
The classificationt task is to detect any PHI and/or PII in Dicom tags.

Ground truths for PHI/PII were derived by comparing the MIDI-B Challenge Dataset with its curated (by the TCIA curation teams "cleaned") version. 

There are two classes:
- No PHI/PII (Label 0): Dicom tag value was not changed by the TCIA curation team
- PHI/PII (Label 1): Dicom tag value was changed/removed/replaced by the TCIA curation team

As this metric is not 100% perfect (e.g., due to curation artifacts), we performed additionally a manual consistency check of the ground truths.

<b>Why is it not 100% perfect?</b> For example: The private dicom tag (0013,1010) has the value "MIDI-B-Synthetic-Test" in its test Dicom file and "MIDI-B-Curated-Test" in its curated version. Hence, its generated ground truth label would indiciate PHI/PII as the tag was changed by the curation team, however, this tag does not contain any PHI/PII and its change is a result of the curation process (curation artifact).   


#### Small Benchmark (fast option for testing)
Benchmark performance of the detector on a subset of 500 randomly sampled Dicom tags with <b>50% free-text</b> and <b>50% private tags</b> from the validation dataset.

| Model       | Tags/Batch | Tags not processed | Sensivitity | Specificity | Positive predictive value | Negative predictive value | **Balanced Accuracy** | **F2-Score** |
|-------------|:----------:|:------------------:|:-----------:|:-----------:|:-------------------------:|:-------------------------:|:---------------------:|:------------:|
| gpt-oss-20b |      1     |          0         |     0.97    |     0.99    |            0.96           |            0.99           |        **0.98**       |   **0.97**   |
|             |      5     |          0         |     0.85    |     0.98    |            0.95           |            0.94           |        **0.91**       |   **0.86**   |
|             |     10     |         10         |     0.81    |     0.99    |            0.97           |            0.93           |        **0.9**        |   **0.84**   |
| gpt-4       |      1     |          0         |     0.65    |     0.99    |            0.98           |            0.89           |        **0.82**       |    **0.7**   |
|             |      5     |          0         |     0.72    |     0.98    |            0.92           |            0.9            |        **0.85**       |   **0.75**   |
|             |     10     |          0         |     0.7     |     0.97    |            0.9            |            0.9            |        **0.84**       |   **0.73**   |
| gpt-4o-mini |      1     |          0         |     0.66    |     1.0     |            0.99           |            0.89           |        **0.83**       |   **0.71**   |
|             |      5     |          0         |     0.65    |     0.99    |            0.96           |            0.88           |        **0.82**       |   **0.69**   |
|             |     10     |         70         |     0.68    |     0.99    |            0.95           |            0.89           |        **0.84**       |   **0.72**   |

#### Full Benchmark (slow, but more representative)
Benchmark performance of the detector on the entire benchmark dataset.

<b>Validation dataset (n=14,989):</b>
| Model       | Tags/Batch | Tags not processed | Sensitivity | Specificity | Positive predictive value | Negative predictive value | **Balanced Accuracy** | **F2-Score** |
|-------------|:----------:|:------------------:|:-----------:|:-----------:|:-------------------------:|:-------------------------:|:---------------------:|:------------:|
| gpt-oss-20b |      1     |          0         |     0.95    |     0.98    |            0.81           |            1.0            |        **0.97**       |   **0.92**   |
|             |      5     |          5         |     0.89    |     0.99    |            0.87           |            0.99           |        **0.94**       |   **0.89**   |
|             |     10     |        1000        |     0.84    |     0.99    |            0.9            |            0.99           |        **0.91**       |   **0.85**   |

<b>Test dataset (n=21,793):</b>

In progress/Running