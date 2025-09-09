# DICOM Tag Processing Scripts

This directory contains scripts for processing and formatting DICOM tag information from various sources including TCIA and official DICOM standards.

## Retrieve Tags (`retrieve_tags.py`)

This is the main script for generating comprehensive tag templates by combining data from multiple sources. It can create both private and standard tag templates for anonymization workflows.

### Basic Usage

#### Generate Private Tag Template

```bash
python retrieve_tags.py --create_private_tag_template
```

This creates `../data/TagsArchive/private_tags_template.csv` by:
1. Downloading TCIA Private Tag Knowledge Base
2. Fetching DICOM Safe Private Tags Table E.3.10-1
3. Merging and cross-referencing the data
4. Generating unified anonymization recommendations

#### Generate Standard Tag Template

```bash
python retrieve_tags.py --create_standard_tag_template
```

This creates `../data/TagsArchive/standard_tags_template.csv` by:
1. Scraping TCIA standard tags table
2. Fetching DICOM standard anonymization table
3. Processing VR (Value Representation) information
4. Applying anonymization profile rules
5. Generating specific anonymization actions

#### Generate Standard Tag Template and Private Tag Template

```bash
python retrieve_tags.py --create_standard_tag_template --create_private_tag_template
```

### Advanced Usage

#### Custom Output Paths

```bash
python retrieve_tags.py --create_private_tag_template --merged_private_tags /path/to/custom_private_tags.csv
python retrieve_tags.py --create_standard_tag_template --merged_standard_tags /path/to/custom_standard_tags.csv
```

#### Save Intermediate Files

```bash
python retrieve_tags.py --create_private_tag_template --save_reformatted --save_dicom_std_not_in_tcia
python retrieve_tags.py --create_standard_tag_template --save_standard_tcia_csv --save_standard_dicom_csv
```

### Arguments

#### Private Tag Template Options
- `--create_private_tag_template`: Generate private tag template
- `--input_tcia`: Input TCIA CSV file path (default: "TCIAPrivateTagKB-02-01-2024-formatted.csv")
- `--tcia_url`: URL to download TCIA CSV (default: TCIA download URL)
- `--reformatted`: Output formatted TCIA CSV path (default: "TCIAPrivateTagKB-reformatted.csv")
- `--merged_private_tags`: Output merged private tags CSV (default: "../data/TagsArchive/private_tags_template.csv")
- `--dicom_url`: URL for DICOM safe private tags table (default: DICOM Part 15 URL)
- `--dicom_table_csv`: Local CSV for DICOM safe private tags
- `--save_dicom_std_not_in_tcia`: Save DICOM tags not in TCIA
- `--save_reformatted`: Save reformatted CSV

#### Standard Tag Template Options
- `--create_standard_tag_template`: Generate standard tag template
- `--standard_tcia_url`: URL for TCIA standard tags table (default: TCIA wiki URL)
- `--standard_dicom_url`: URL for DICOM standard tags table (default: DICOM Part 15 Chapter E)
- `--standard_tcia_csv`: Output CSV for TCIA standard tags (default: "tcia_standard_tags.csv")
- `--standard_dicom_csv`: Output CSV for DICOM standard tags (default: "dicom_standard_tags.csv")
- `--merged_standard_tags`: Output merged standard tags CSV (default: "../data/TagsArchive/standard_tags_template.csv")
- `--save_standard_tcia_csv`: Save TCIA CSV
- `--save_standard_dicom_csv`: Save DICOM CSV

### Output Files

#### Private Tag Template
- **private_tags_template.csv**: Comprehensive private tag anonymization template with columns:
  - Group, Element, Private Creator, VR, VM, Meaning
  - Rtn. Safe Priv. Opt.: Anonymization recommendation
  - IsInDICOMRetainSafePrivateTags: Whether tag is in DICOM safe list

#### Standard Tag Template
- **standard_tags_template.csv**: Comprehensive standard tag anonymization template with columns:
  - Group, Element, Name, VR, VM
  - Basic Prof.: Basic anonymization profile action
  - Rtn. UIDs Opt., Rtn. Dev. Id. Opt., etc.: Retention options
  - Clean Desc. Opt., Clean Struct. Cont. Opt., etc.: Cleaning options



## Installation

Install requirements with:

```bash
pip install -r requirements.txt
```

