# LuwakX - Config-Driven DICOM Anonymization

LuwakX is a powerful, config-driven tool for anonymizing DICOM files using the `deid` library. It supports flexible configuration through JSON files and provides both command-line and programmatic interfaces.
It is based on the 2025b DICOM standards.

## Installation

### From PyPI

```bash
pip install luwakx
```

### From source

```bash
git clone https://github.com/ZentaLabs/luwak.git
cd luwak
pip install -e ".[test]"
```

### Using requirements.txt (development)

```bash
pip install -r requirements.txt
pip install -e ".[test]"
```

> **Note:** `deid` is a custom fork not available on PyPI. It is automatically installed from GitHub the first time anonymization is run.

### Prerequisites

- Python >= 3.12

## Usage

### Command Line Interface

```bash
luwakx --config_path /path/to/config.json
```

Options:
- `--config_path`: Path to the JSON configuration file
- `--no-console`: Disable console logging (only log to file)

### Programmatic Interface

```python
from luwakx.anonymize import LuwakAnonymizer

anonymizer = LuwakAnonymizer("/path/to/config.json")
result = anonymizer.anonymize()
```

## Configuration

Create a JSON configuration file:

```json
{
  "inputFolder": "/path/to/input/dicom/files",
  "patientIdPrefix": "Patient",
  "outputDeidentifiedFolder": "/path/to/output/deidentified",
  "outputPrivateMappingFolder": "/path/to/output/privateMapping",
  "recipesFolder": "/path/to/output/recipes",
  "recipes": ["basic_profile"],
  "projectHashRoot": "your_encryption_key",
  "maxDateShiftDays": 1095,
  "logLevel": "INFO"
}
```

For full configuration documentation see [luwakx/README.md](luwakx/README.md).

## License

Apache License 2.0 — see [LICENSE.txt](LICENSE.txt).

