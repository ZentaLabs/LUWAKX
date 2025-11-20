# DICOM Deidentification Conformance Statement

**Luwak DICOM Deidentification System**  
Version: 1.0  
Date: November 19, 2025  
Based on: DICOM Standard 2025b

---

## 1. Introduction

### 1.1 Purpose
This document describes the deidentification process implemented in the Luwak project. It provides comprehensive details on recipe creation, tag template generation, deidentification profiles, and the complete deidentification workflow.

### 1.2 Scope
This conformance statement applies to the deidentification features provided by the Luwak DICOM processing pipeline, including:
- Standard and private DICOM tag anonymization
- Recipe generation from configurable templates

**Core Deidentification Engine:** Luwak uses the [deid](https://github.com/pydicom/deid) library (maintained in the [pydicom](https://github.com/pydicom) organization in GitHub) for DICOM metadata deidentification. The deid library provides the recipe processing engine that applies tag-level transformations according to configurable rules. Luwak extends deid with custom anonymization functions for HMAC-based UID generation, date shifting, LLM-based descriptor cleaning, and face defacing capabilities. It additionally provides support for a list of deidentification profiles from the DICOM Standard 2025b PS3.15 Appendix E, Table E.1-1.


### 1.3 Audience
This document is intended for:
- Clinical researchers implementing DICOM anonymization
- Data protection officers ensuring HIPAA/GDPR compliance
- Software developers integrating with the Luwak pipeline
- Quality assurance teams validating anonymization procedures

### 1.4 Revision History

| Document Version | Date of Revision | Code Version | Description                                      |
|-----------------|------------------|-------------|--------------------------------------------------|
| 1.0             | 2025-11-19       | v1.0         | Initial release for DICOM 2025b   ||


---


## 2. Document Index

This chapter provides a clickable index for all major sections of this conformance statement. Click any entry to jump directly to the relevant section.

| Section | Link |
|---------|------|
| **Profiles and Options** |
| Supported Deidentification Profiles/Options | [§3](#3-supported-deidentification-profilesoptions) |
| Profile Overview | [§3.1](#31-overview) |
| **Image Pixel Data Deidentification** |
| Image Pixel Data Deidentification | [§4](#4-image-pixel-data-deidentification) |
| Clean Recognizable Visual Features (Defacing) | [§4.1](#41-clean-recognizable-visual-features-defacing----pipeline-stage-2) |
| Burned-In Pixel Annotation Detection | [§4.2](#42-burned-in-pixel-annotation-detection) |
| **Metadata Deidentification - Tags and Profiles Templates** |
| Tags and Profiles Templates | [§5](#5-metadata-deideintification----tags-and-profiles-templates) |
| Standard Tags Template | [§5.1](#51-standard-tags-template) |
| Private Tags Template | [§5.2](#52-private-tags-template) |
| Tag/Profile Specific Actions | [§5.3](#53-tagprofile-specific-actions) |
| Profile/Options Description | [§5.4](#54-profileoptions-description) |
| **Recipe Creation** |
| Deidentification Recipe Creation | [§6](#6-deidentification-recipe-creation-pipeline-stage-3---4) |
| Recipe Builder Overview | [§6.1](#61-recipe-builder-overview) |
| Deidentification Profiles | [§6.2](#62-deidentification-profiles) |
| Dummy Value Replacement Rules | [§6.3](#63-dummy-value-replacement-rules) |
| Recipe Builder Action-to-Recipe Translation | [§6.4](#64-recipe-builder-action-to-recipe-translation) |
| Action Priority Rules | [§6.5](#65-action-priority-rules) |
| Generated Recipe File Format | [§6.6](#66-generated-recipe-file-format) |
| **Deidentification Process and Export** |
| Deidentification Process | [§5 (duplicate)](#5-deidentification-process) |
| DeidentificationMethodCodeSequence Injection | [§7](#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-5) |
| Data and Metadata Export | [§8](#8-deidentified-data-and-metadata-export-pipeline-stage-6) |
| Output Files Generated | [§8.1](#81-output-files-generated-by-luwak) |
| **Configuration and Usage** |
| Configuration, Code Design, and Usage | [§9](#9-configuration-code-design-and-usage) |
| Configuration File | [§9.1](#91-configuration-file) |
| Code Architecture and Design | [§9.2](#92-code-architecture-and-design) |
| Core Classes and Relationships | [§9.2.2](#922-core-classes-and-relationships) |
| Data Flow | [§9.2.3](#923-data-flow) |
| Threading and Parallelization | [§9.2.4](#924-threading-and-parallelization) |
| Running Luwak | [§9.3](#93-running-luwak) |
| Command-Line Usage | [§9.3.1](#931-command-line-usage) |
| Programmatic Usage | [§9.3.2](#932-programmatic-usage) |
| Output File Structure | [§9.4](#94-output-file-structure) |
| **Limitations and Testing** |
| Limitations and Constraints | [§10](#10-limitations-and-constraints) |
| Known Limitations | [§10.1](#101-known-limitations) |
| Dependencies | [§10.2](#102-dependencies) |
| Validation and Testing | [§11](#11-validation-and-testing) |
| Test Suite Overview | [§11.1](#111-test-suite-overview) |
| Key Test Cases | [§11.2](#112-key-test-cases) |
| Running Tests | [§11.3](#113-running-tests) |
| **References and Appendices** |
| References | [§12](#12-references) |
| DICOM Standards | [§12.1](#121-dicom-standards) |
| Community Resources | [§12.2](#122-community-resources) |
| Software Libraries | [§12.3](#123-software-libraries) |
| Appendix A: Configuration Schema Reference | [Appendix A](#appendix-a-configuration-schema-reference) |
| Appendix B: Tag Template Maintenance | [Appendix B](#appendix-b-tag-template-maintenance) |

---

## 3. Supported deidentification Profiles/Options

### 3.1 Overview

Luwak allows the user to select profile and options through a list of "recipes" specified in a config file (see [§9.1](#91-configuration-file) for details on how to use the config file). 
The following DICOM PS3.15 Appendix E profiles and options are supported by Luwak:

| Profile/Option | Recipe Name | CID 7050 Code | Description |
|----------------|-------------|---------------|-------------|
| Basic Application Confidentiality Profile | `basic_profile` | 113100 | Removes or replaces all attributes that could identify the patient |
| Retain UIDs | `retain_uid` | 113111 | Retains original Study, Series, and SOP Instance UIDs |
| Retain Device Identity | `retain_device_id` | 113110 | Retains device and manufacturer information |
| Retain Institution Identity | `retain_institution_id` | 113109 | Retains institution name and address |
| Retain Patient Characteristics | `retain_patient_chars` | 113108 | Retains patient age, size, weight, sex |
| Retain Longitudinal Temporal Information with Full Dates | `retain_long_full_dates` | 113106 | Retains all original dates without modification |
| Retain Longitudinal Temporal Information with Modified Dates | `retain_long_modified_dates` | 113107 | Retains dates but shifts them consistently |
| Clean Descriptors | `clean_descriptors` | 113105 | Cleans textual descriptors of PHI using LLM |
| Retain Safe Private | `retain_safe_private_tags` | 113112 | Retains DICOM-specified safe private attributes |
| Clean Recognizable Visual Features | `clean_recognizable_visual_features` | 113101 | Applies face defacing to imaging pixel data |

**Note on Unsupported Profiles:**

Luwak currently does not provide automated support for the following DICOM PS3.15 Appendix E profiles:

- *Clean Structured Content Option (CID 7050 code 113104):* Tags requiring structured content cleaning are flagged with `clean_manually` actions in the recipe for manual review. Automated PHI detection in complex nested structures is not currently implemented.

- *Clean Graphics Option (CID 7050 code 113103):* Tags with graphic annotations are flagged with `clean_manually` actions in the recipe for manual review. Users must manually inspect and clean text annotations overlaid on images.

These profiles require manual intervention to ensure PHI is properly removed from structured content sequences and graphic annotations.

**Note on Clean Pixel Data Option:**

- *Clean Pixel Data Option:* Luwak includes detection rules for identifying burned-in pixel annotations that cannot be removed through header anonymization. The pixel cleaning functionality using the deid library is currently in active development and will be integrated into the automated pipeline in a future release. For now, users should manually review images (see [§4.2](#42-clean-pixel-data-option)).

**Reference:** DICOM PS3.16 CID 7050 - De-identification Method  
URL: https://dicom.nema.org/medical/dicom/current/output/chtml/part16/sect_CID_7050.html

DICOM Standard PS3.15 Appendix E, Table E.1-1  
   URL: https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E  

#### 3.2 Pipeline Architecture

The Luwak deidentification pipeline consists of the following stages:

1. **Organization** - Group files by series, study, patients
2. **Defacing** (optional) - Remove recognizable facial features from imaging data
3. **Recipe Generation** - Build DEID recipe from selected profiles
4. **Metadata Deidentification** - Apply recipe rules to each tag using DEID library
5. **Sequence Injection** - Add DeidentificationMethodCodeSequence Attribute
6. **Export** - Save deidentified files and metadata

**Metadata Deidentification Implementation:**  
Luwak uses the [DEID library](https://github.com/pydicom/deid) for DICOM header deidentification (pipeline stage 4). The DEID library's `replace_identifiers()` function applies recipe-based transformations to DICOM tags. Luwak injects custom deidentification functions into the DEID processing pipeline to extend its capabilities beyond its built-in operations:

- `generate_hmacuid` - Cryptographic UID deidentification
- `generate_patient_id` - Sequential patient ID generation with database persistence
- `generate_hmacdate_shift` - HMAC-based deterministic date shifting
- `clean_descriptors_with_llm` - AI-powered PHI detection in textual tags
- `set_fixed_datetime` - Fixed epoch datetime replacement
- `is_tag_private` - Private tag identification for removal
- `is_curve_or_overlay_tag` - Curve/overlay data identification for removal

These custom functions are injected into DEID's item processing dictionary and called during recipe execution when specified in recipe files.

In the following sections we explain the deidentification pipeline in detail:

1. **Image Pixel Data Deidentification ([§4](#4-image-pixel-data-deidentification))** - Describes facial feature removal using MOOSE framework ([§4.1](#41-clean-recognizable-visual-features-defacing----pipeline-stage-2)) and burned-in pixel annotation detection ([§4.2](#42-clean-pixel-data-option)).

2. **Metadata Deidentification ([§5](#5-metadata-deideintification----tags-and-profiles-templates))** - Details the tag template generation ([§5.1](#51-standard-tags-template), [§5.2](#52-private-tags-template)), deidentification actions ([§5.3](#53-tagprofile-specific-actions)), and profile implementation ([§5.4](#54-profileoptions-description)).

3. **Recipe Creation ([§6](#6-deidentification-recipe-creation-pipeline-stage-3---4))** - Explains how CSV templates are converted into DEID recipes, including dummy value generation ([§6.3](#63-dummy-value-replacement-rules)) and action translation logic ([§6.4](#64-recipe-builder-action-to-recipe-translation)).

4. **DeidentificationMethodCodeSequence Injection ([§7](#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-5))** - Documents how applied deidentification methods are recorded in DICOM standard format.

5. **Data and Metadata Export ([§8](#8-deidentified-data-and-metadata-export-pipeline-stage-6))** - Describes all output files generated by Luwak, including deidentified DICOM files, UID mappings, metadata exports, and audit logs.

6. **Configuration and Code Architecture ([§9](#9-configuration-code-design-and-usage))** - Provides comprehensive guide to configuration options ([§9.1](#91-configuration-file)), system architecture ([§9.2](#92-code-architecture-and-design)), and usage instructions ([§9.3](#93-running-luwak)).

7. **Limitations, Testing, and References ([§10](#10-limitations-and-constraints)-[§12](#12-references))** - Covers known limitations, validation procedures, and external references.

## 4. Image Pixel Data Deidentification 

This section describes Luwak's implementation of image pixel data deidentification, specifically addressing the Clean Recognizable Visual Features profile and the Clean Pixel Data Option.

### 4.1 Clean Recognizable Visual Features (Defacing -- pipeline stage 2)

#### 4.1.1 Overview
Luwak implements automated face detection and pixelation for medical imaging volumes (CT, PET) to remove identifiable facial features from pixel data, complying with DICOM CID 7050 code 113101.
Currently only CT is supported, but we are soon extenting this to PET and we plan in the future to add also MRI.

**Implementation Module:** `luwakx/deface_service.py`  
**ML Defacing Module:** `luwakx/scripts/defacing/image_defacer/image_anonymization.py`

**Defacing Model Reference:** The defacing functionality in Luwak leverages the MOOSE framework and its pre-trained models for medical image segmentation and facial feature detection. MOOSE (https://github.com/ENHANCE-PET/MOOSE) provides robust deep learning models specifically designed for medical imaging tasks, enabling accurate and automated identification of facial regions in CT and PET scans. By integrating MOOSE, Luwak ensures state-of-the-art performance and reliability in the defacing process.

#### 4.1.2 Defacing Pipeline

**Step 1: Volume Reconstruction**
- Loads DICOM series files as 3D volume using SimpleITK
- Uses GDCM to properly sort files by ImagePositionPatient
- Preserves spatial relationships for accurate 3D face detection

**Step 2: Face Detection/Segmentation**
- Uses CT-optimized face detection models
- Automatically segments facial features in 3D volume
- Generates binary segmentation mask
- GPU-accelerated with automatic memory cleanup

**Step 3: Pixelation**
- Applies pixelation to face-segmented regions
- Preserves diagnostic image quality in non-face regions

**Step 4: DICOM Export**
- Converts defaced 3D volume back to individual DICOM files
- Applies inverse rescale (RescaleSlope/RescaleIntercept) to restore raw pixel values
- Preserves original DICOM tags and structure

#### 4.1.3 Modality Support
- **CT (Computed Tomography):** Fully supported with modality-specific ML models
- **PET :** Soon to be developed as : 
  - If the input image is a PET/CT, project CT-based face mask on PET and run the defacing algorithm on resampled face mask to pixelate (or blur) the face in the PET image.
  - If an input image has no associated CT, run a custom PET face segmentation model on the PET and deface the image based on the PET-derived face mask. 
- **MR (Magnetic Resonance):** Planned for future implementation
- **Other modalities:** Not currently supported for defacing

#### 4.1.4 Configuration

**Enable Face Defacing:**
To add the defacing option to the deidentification pipeline the correct recipe must be added in the recipes option in the config file ([§9.1](#91-configuration-file)).

```json
{
  "recipes": ["clean_recognizable_visual_features"]
}
```

#### 4.1.5 Conditional Processing
Face defacing is only performed when:
1. `clean_recognizable_visual_features` profile is selected in recipes
2. Modality is CT (currently only CT is supported; PET support in progress)

**Decision Logic:** `ProcessingPipeline._needs_defacing()`

**Behavior when defacing is not needed:**
- If defacing is not needed (modality is not CT or profile not selected), the defacing stage is skipped entirely
- Files remain in organized directory and are read directly for metadata deidentification
- No files are copied to defaced directory

**Behavior when defacing fails:**
- If defacing is attempted but fails (e.g., ML model error, file read error)
- Original organized files are copied to defaced directory without modification via `_copy_without_defacing()`
- Error messages are issues in the log. 
- Processing continues with undefaced files.
- CID 7050 code 113101 is NOT added to DeidentificationMethodCodeSequence Attribute

#### 4.1.6 DeidentificationMethodCodeSequence Integration
- CID 7050 code 113101 is conditionally added to DeidentificationMethodCodeSequence
- Only included if defacing was successfully performed
- Absent if defacing was skipped or failed

#### 4.1.7 Output Artifacts
For each defaced series:
- `image.nrrd` - Original 3D volume (temporary, for validation)
- `image_defaced.nrrd` - Defaced 3D volume (temporary)
- `*.dcm` - Defaced DICOM files (final output)

Temporary NRRD files are stored in `defaced_base_path` during processing.

#### 4.1.8 Performance Considerations
- GPU acceleration recommended for ML face detection
- Memory cleanup after each series to prevent GPU OOM
- Typical processing time with GPU: 30-90 seconds per series

#### 4.1.9 Current limitations
- Defacing is supported only for CT modality
- The time/resource consuming ML model will run even when no face is included in the data, because we can't rely on BodyPartExamined correct labeling.
- No modification on non-face data has been observed from the defacing model so far (David/Sebastian: tot number of tests??), but care must be taken to check the defacing result after each deidentification project. 

### 4.2 Clean Pixel Data Option

#### 4.2.1 Overview
Luwak includes DEID-based detection rules for burned-in pixel annotations that cannot be removed through header deidentification or face defacing.

**Recipe File:** `deid.dicom.burnedin-pixel-recipe`

**Pixel Cleaning Implementation:** Burned-in pixel cleaning is performed using the `deid` library, which provides tools for detecting and masking sensitive pixel regions in DICOM images. The process involves identifying known areas which typically include annotations (such as patient names or IDs) and applying pixel masking to remove them. For more details, see the [deid pixel cleaning documentation](https://pydicom.github.io/deid/getting-started/dicom-pixels/).

This functionality will soon be integrated and called within Luwak, ensuring that images are cleaned of burned-in identifiers. We also plan to extend this approach for more general automatic detection of sensitive pixel regions in future releases.

#### 4.2.2 Detection Rules
- **Whitelist filters:** Known clean image types/modalities
- **Graylist filters:** Specific pixel regions with common annotations
- **Equipment-specific rules:** Manufacturer-specific patterns

#### 4.2.3 Common Burned-In Locations
- Dose reports in CT (coordinates 0,0,512,121)
- Localizer images
- Enhancement curves
- Reconstruction metadata overlays

TODO: ADD description of how this is integrated in luwak, once I do it.

Once the pixel data have been cleaned of all possible identification risks, the pipeline proceeds to stage 3 for metadata deidentification.

---

## 5. Metadata Deidentification - Tags and Profiles Templates

Luwak uses two primary CSV template files that define deidentification actions per profile/option for all DICOM tags based on the DICOM 2025b standard:
- `standard_tags_template.csv` - Standard DICOM tags
- `private_tags_template.csv` - Private DICOM tags

**Location:** `luwakx/data/TagsArchive/`

### 5.1 Standard Tags Template

#### 5.1.1 Source and Provenance
The `standard_tags_template.csv` file combines data from two authoritative sources:

1. **DICOM Standard PS3.15 Appendix E, Table E.1-1**  
   URL: https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E  
   Provides the official DICOM deidentification profile with basic profile and retention options.

2. **TCIA (The Cancer Imaging Archive) Submission and De-identification Overview, Table 1**  
   URL: https://wiki.cancerimagingarchive.net/display/Public/Submission+and+De-identification+Overview  
   Provides community best practices and CTP script mappings for deidentifictaion.

#### 5.1.2 Generation Process
The template is already in the repository and has been generated using the `retrieve_tags.py` script:

```bash
python retrieve_tags.py --create_standard_tag_template \
    --merged_standard_tags ../data/TagsArchive/standard_tags_template.csv
```

**Steps performed:**
1. Scrapes TCIA standard tags table from their wiki
2. Fetches DICOM standard deidentification table (PS3.15 Table E.1-1)
3. Merges data with VR (Value Representation) information from DICOM data dictionaries
4. Writes specific deidentification actions per profile (more details at [§5.3](#53-tagprofile-specific-actions) and [§5.4](#54-profileoptions-description))
5. Generates unified template.

#### 5.1.3 Template Structure
CSV columns include:

| Column | Description |
|--------|-------------|
| `Group` | DICOM tag group (hex) |
| `Element` | DICOM tag element (hex) |
| `Name` | Tag name/description |
| `VR` | Value Representation (DA, UI, PN, etc.) |
| `VM` | Value Multiplicity |
| `Basic Prof.` | Action for Basic Application Confidentiality Profile |
| `Rtn. UIDs Opt.` | Action for Retain UIDs option |
| `Rtn. Dev. Id. Opt.` | Action for Retain Device Identity option |
| `Rtn. Inst. Id. Opt.` | Action for Retain Institution Identity option |
| `Rtn. Pat. Chars. Opt.` | Action for Retain Patient Characteristics option |
| `Rtn. Long. Full Dates Opt.` | Action for Retain Longitudinal Full Dates option |
| `Rtn. Long. Modif. Dates Opt.` | Action for Retain Longitudinal Modified Dates option |
| `Clean Desc. Opt.` | Action for Clean Descriptors option |
| `Clean Struct. Cont. Opt.` | Action for Clean Structured Content option |
| `Clean Graph. Opt.` | Action for Clean Graphics option |
| `TCIA element_sig_pattern` | TCIA tag signature pattern |
| `Final CTP Script` | CTP anonymizer script equivalent |

The last two columns are placed in the table to always provide a way to compare to the final action chosen by the TCIA deidentification for that tag. 
Keep in mind that the TCIA final action comes from a combination of profiles: "Basic Application Confidentiality Profile" which is amended by inclusion of "Clean Pixel Data Option", "Clean Descriptors Option", "Retain Longitudinal With Modified Dates Option", "Retain Patient Characteristics Option", and "Retain Safe Private Option".

#### 5.1.4 Nested Sequence Support
Luwak supports nested DICOM sequences using double-underscore (`__`) notation:

**Example:**
```
Group: 0018__0__0008
Element: 9346__0__0104
```
This represents: `(0018,9346)[0](0008,0104)` - the Referenced SOP Instance UID within the first item of the Referenced Series Sequence.

### 5.2 Private Tags Template

#### 5.2.1 Source and Provenance
The `private_tags_template.csv` file combines data from:

1. **DICOM Standard PS3.15 Appendix E.3.10, Table E.3.10-1 (Safe Private Attributes)**  
   URL: https://dicom.nema.org/medical/dicom/current/output/chtml/part15/sect_E.3.10.html  
   Official list of private tags considered safe to retain.

2. **TCIA Private Tag Knowledge Base**  
   URL: https://wiki.cancerimagingarchive.net/download/attachments/3539047/TCIAPrivateTagKB-02-01-2024-formatted.csv  
   Community-validated private tag database from real-world imaging archives.

#### 5.2.2 Generation Process
The template is already in the repoitory and is generated using the `retrieve_tags.py` script:

```bash
python retrieve_tags.py --create_private_tag_template \
    --merged_private_tags ../data/TagsArchive/private_tags_template.csv
```

**Steps performed:**
1. Downloads TCIA Private Tag Knowledge Base CSV
2. Fetches DICOM Safe Private Tags table from PS3.15
3. Merges and cross-references the data
4. Generates unified template with deidentification recommendations [§5.3](#53-tagprofile-specific-actions)
#### 5.2.3 Template Structure
CSV columns include:

| Column | Description |
|--------|-------------|
| `Group` | DICOM tag group (hex) |
| `Element` | Element with `xx` placeholder for private creator code |
| `Private Creator` | Private creator identifier string |
| `VR` | Value Representation |
| `VM` | Value Multiplicity |
| `Meaning` | Tag description/meaning |
| `Rtn. Safe Priv. Opt.` | Action for Retain Safe Private option |
| `IsInDICOMRetainSafePrivateTags` | Boolean: tag is in official DICOM safe list |
| `TCIA element_sig_pattern` | TCIA tag signature pattern |

#### 5.2.4 Private Tag Notation
Private tags use `xx` notation where `xx` represents the private creator code block:
- `(2001,xxc1)` with creator `"Philips Imaging DD 001"` represents tag `(2001,10c1)` if the private creator is at `(2001,0010)`.

### 5.3 Tag/Profile Specific Actions

For each Tag/Profile the csv template files specify actions that will be applied to the tag during the process of deidentification. These actions will be used to generate the deidentification recipe in the format accepted by DEID.

The following table lists the actions present in the csv tags templates and supported in recipe generation.
For completeness, we have added the equivalent actions in the DICOM Standard PS3.15 Appendix E, Table E.1-1, the CTP script action used by TCIA, and the actions accepted by the pydicom/deid recipe:

| Luwak's Action | Description | Example Usage | DICOM Table Action | TCIA/CTP Action | pydicom/deid Action |
|--------|-------------|---------------|--------------------|-------------|-------------|
| `keep` | Retain original value unchanged | Patient's age in retain_patient_chars | K | @keep() | KEEP |
| `remove` | Delete the tag entirely | Patient's name in basic_profile | X | @remove() | REMOVE |
| `blank` | Set to empty value | Accession Number | Z | @empty() | BLANK |
| `replace` | Replace with generic text or computed value | PatientID with anonymized ID | D | | REPLACE |
| `func:generate_hmacuid` | Generate cryptographic anonymized UID | Study/Series/SOP Instance UIDs | U | @hashuid(@UIDROOT,this) | REPLACE |
| `func:set_fixed_datetime` | Set to fixed epoch datetime | Dates in basic_profile | D |  | REPLACE |
| `func:generate_hmacdate_shift` | Apply consistent date shift | Dates in retain_long_modified_dates | C | @incrementdate(this,@DATEINC) | JITTER |
| `func:clean_descriptors_with_llm` | Clean text with LLM PHI detection | Study/Series descriptions | C | | REPLACE |
| `func:generate_patient_id` | Generate consistent anonymized patient ID | PatientID in basic_profile | Z/D | LOOKUP(this,ptid) | REPLACE | 

Luwak supports custom deidentification functions that extend pydicom/deid's recipe syntax. These functions are injected into the recipe as action arguments (e.g., `REPLACE (0020,000d) func:generate_hmacuid`) and executed by the pipeline for tags requiring advanced deidentification ([§6](#6-deidentification-recipe-creation-pipeline-stage-3---4)). The main custom functions are: `func:generate_hmacuid`, `func:generate_hmacdate_shift`, `func:clean_descriptors_with_llm`, `func:generate_patient_id`.

In the following we detail the purpose and description of each Luwak's action.


#### 5.3.1 `keep`
Retain the original value of the tag unchanged. Used when the tag is allowed to be preserved according to the selected profile or option.
**Example:** Patient's age in `retain_patient_chars` profile.


#### 5.3.2 `remove`
Delete the tag entirely from the DICOM file. Used for tags that must be eliminated to ensure confidentiality.
**Example:** Patient's name in `basic_profile`.


#### 5.3.3 `blank`
Set the tag value to an empty value (zero-length string or empty bytes, depending on VR). Used when the tag must be present but without any identifying information.
**Example:** Accession Number in `basic_profile`.


#### 5.3.4 `replace`
Replace the tag value with a generic text or computed value, such as a dummy string or zero for numeric VRs. Used to maintain DICOM validity while removing PHI, for the Basic Application Confidentiality Profile.
**Example:** PatientID with anonymized ID.


#### 5.3.4 UID Generation `func:generate_hmacuid`

**Purpose:** Generate cryptographically secure, deterministic anonymized UIDs.
**Example:** Study/Series/SOP Instance UIDs.

**Method:**
- For each patient, a cryptographically secure random token is created using Python's `secrets` module.
- The token is 32 bytes (256 bits) generated from the OS CSPRNG, encoded as a hexadecimal string.
- This token is generated once per patient and stored in the UID database, ensuring deterministic anonymization for all UIDs associated with that patient.
- The token is never reused across patients and is not derivable from patient data, ensuring strong isolation and non-reversibility.
- Example code:
  ```python
  import secrets
  token = secrets.token_hex(32)  # 64 hex characters, 256 bits
  ```
- This token is used as key for HMAC-SHA512.
- The HMAC-SHA512 additionally combines a project hash (provided in the config file) and the original UID, as data.
- The HMAC output (hex string) is used as `entropy_src` argument to `pydicom.uid.generate_uid`.
- `generate_uid` uses the entropy to create a globally unique, standards-compliant UID.
- Example code:
  ```python
  from pydicom.uid import generate_uid
  uid = generate_uid(entropy_src=hmac_digest)
  ```
- Ensures same original UID always maps to same anonymized UID per patient.
- Different patients get different anonymized UIDs even for identical original UIDs.

This method ensures the anonymized UID is valid, globally unique, and deterministic for each patient, while conforming to DICOM UID length and format requirements.
At the end of the deidentification projects the uid database containing the random token per patient is deleted unless differently declared in the config file (see [§9.1](#91-configuration-file)).

**Implementation:** `DicomProcessor.generate_hmacuid()`

**Recipe Usage:**
```
REPLACE (0020,000d) func:generate_hmacuid
```

**Current limitations**
- This method is designed to provide cryptographic security sufficient to withstand technological advances projected for the next 50 years; however, progress in cryptanalysis or computing could occur faster than anticipated. Users should therefore remain vigilant and periodically review and update the method to ensure it continues to meet current safety standards.
- If the database containing the random keys is saved and reused, it must be stored in an extremely secure environment, as it becomes the single point of vulnerability for the entire deidentification process.
- 

#### 5.3.5 Date Shifting `fun:generate_hmacdate_shift`

**Purpose:** Shift dates consistently for longitudinal studies while maintaining temporal relationships. **Example:** Dates in `retain_long_modified_dates`.

**Method:**
- Computes an HMAC-SHA512 digest using the patient-specific random token as key, generated as described in [§5.3.4 UID Generation `func:generate_hmacuid`](#53.4-uid-generation-funcgenerate_hmacuid), and the project hash root as data. 
  - The first 16 hex digits of the digest are converted to an integer, providing entropy for the shift.
  - The resulting integer is mapped (using modulo) to the allowed date shift range (1 to `maxDateShiftDays`, default 1095), ensuring the shift is always at least 1 day (never zero).
  - The same patient always gets the same shift value, ensuring longitudinal consistency.
  - The shift is deterministic, non-reversible, and patient-isolated: identical dates for different patients will be shifted by different amounts.

**Implementation:** `DicomProcessor.generate_hmacdate_shift()`

**Recipe Usage:**
```
JITTER (0008,0020) func:generate_hmacdate_shift
```

**Configuration:**
```json
{
  "maxDateShiftDays": 1095
}
```
**Current limitations**
- Same limitations for [§5.3.4](#53.4-uid-generation-funcgenerate_hmacuid) apply here.
- Date shifting is only applied to VR types `DA` (Date) and `DT` (DateTime). Time values with VR `TM` are kept unchanged. Any other VR type is set to `remove` in accordance with TCIA actions.

#### 5.3.6 Patient ID Generation `func:generate_patient_id`

**Purpose:** Generate consistent anonymized patient IDs using UID database. **Example:** PatientID in `basic_profile`.

**Method:**
- Computes a SHA-256 hash from the combination of original PatientID, PatientName, and PatientBirthDate (including project hash root for project isolation)
- Queries SQLite UID database for existing mapping using this hash
- If found, returns the cached anonymized patient ID
- If not found, generates a new sequential ID using a configurable prefix (the `patientIdPrefix`, see [§9.1](#91-configuration-file)) and a zero-padded 6-digit number (e.g., "Zenta000000", "Zenta000001", "Zenta000002")
- Stores the mapping along with a cryptographically secure random token (256 bits) for HMAC operations
- Thread-safe with write locking to prevent race conditions in parallel processing
- Note: the UID database is created/updated for the entire dataset at the beginning of the deidentification process. The call for generate_patient_id, within the metadata deidentification process, should never effectively require to generate a new sequential ID, but just lookup the existing one. 

**Implementation:** `DicomProcessor.generate_patient_id()`

**Recipe Usage:**
```
REPLACE (0010,0020) func:generate_patient_id
```

**Current limitations**
- Because the Patient ID and random key are stored per patient in the database using a hash of the original PatientID, PatientName, and PatientBirthDate, any missing field, typo, or variation in these identifiers will result in the metadata being treated as belonging to a completely different patient.


#### 5.3.7 LLM Descriptor Cleaning `func:clean_descriptors_with_llm`

**Purpose:** Remove PHI/PII from textual descriptors using large language model. **Example:** Study/Series descriptions.

**Method:**
- Sends descriptor text to LLM API (OpenAI-compatible) for PHI/PII detection
- Uses a binary classifier that returns 0 (no PHI/PII) or 1 (PHI/PII detected)
- Caches results in shared SQLite database to avoid redundant API calls
- If PHI detected (result = 1): attempts to delete the tag entirely from the DICOM dataset; if deletion fails, replaces with "ANONYMIZED"
- If no PHI detected (result = 0): keeps the original text value unchanged
- Thread-safe for parallel processing with shared cache

**Implementation:** `DicomProcessor.clean_descriptors_with_llm()`

**Detector Module:** `luwakx/scripts/detector/detector.py`

**Detection Process:**
1. **System Prompt:** Instructs LLM to act as PHI/PII detector with binary classification
2. **User Prompt:** Sends tag path and content (e.g., "(0008,1030) StudyDescription: CT Chest with contrast")
3. **LLM Classification:** Returns 1 (contains PHI/PII) or 0 (clean)
4. **Cache Storage:** Result cached in SQLite database for future lookups
5. **Tag Action:** If PHI detected, tag is removed from dataset (or replaced with "ANONYMIZED" if deletion fails); otherwise kept unchanged

**LLM System Prompt:**
```
You are an accurate and helpful protected health information (PHI) 
and personally identifiable information (PII) detector. Based on a DICOM tag 
description and DICOM tag content, you will classify if the tag contains PHI or PII. 
The output is only binary, nothing else. Return 1 if it contains PHI or PII and 0 if not.
```

**Temperature Settings:**
- `temperature=0` - Greedy decoding, removes randomness
- `top_p=1` - Disables nucleus sampling for deterministic results

**Recipe Usage:**
```
REPLACE (0008,1030) func:clean_descriptors_with_llm
```

**Configuration for local model:**
```json
{
  "cleanDescriptorsLlmBaseUrl": "http://localhost:1234/v1",
  "cleanDescriptorsLlmModel": "gpt-oss-20b",
  "cleanDescriptorsLlmApiKeyEnvVar": "",
  "analysisCacheFolder": "./analysis_cache"
}
```

**Supported LLM Providers:**
- OpenAI (gpt-4o, gpt-4o-mini, gpt-oss-20b)
- any OpenAI-compatible model
- Local LLM servers (LM Studio)

**Cache Management:**
- Cache stored in: `{analysisCacheFolder}/llm_cache.db` (SQLite)
- Thread-safe for parallel processing with shared access across workers
- If `analysisCacheFolder` is specified: cache persists across anonymization runs
- If not specified: temporary cache created in private mapping folder and deleted after processing

**Current limitations**
- This profile requires a LLM compatible with OpenAI, this implies either the usage of important local resources (GPU with VRAM > 8GB allows deidentification at speed ~5s/tag when run for first time), or the usage of an API KEY with pay access. 
- The LLM provides a binary output for either keeping or removing the tag, no other action is currently supported.
- The LLM can have false negatives, so a final review of the content of the leftover tags is always advised.

#### 5.3.8 Fixed DateTime `func:set_fixed_datetime`

**Purpose:** Set date/time tags to fixed epoch values. Used to remove temporal information while maintaining DICOM compliance. These replacement values are the same as the ones used in KitwareMedical/dicomanonymizer.
**Example:** Dates in `basic_profile`.

**Method:**
- Replaces date/time values based on the tag's Value Representation (VR):
  - DA (Date): Returns "00010101" (January 1, year 1)
  - DT (DateTime): Returns "00010101010101.000000+0000" (January 1, year 1, 01:01:01 AM UTC)
  - TM (Time): Returns "000000.00" (00:00:00)
- Check: For unknown VR types, returns the original value with a warning

**Implementation:** `DicomProcessor.set_fixed_datetime()`

**Recipe Usage:**
```
REPLACE (0008,0012) func:set_fixed_datetime
```
**Current limitations**
- These dummy values will be assigned to all the patients, all series and all studies. This might create issues to 4D data loading and some DICOM viewer. This action is specified only for the Basic Profile, so if you don't want to have these issues, combine the profile with other options that keep/or shift the dates consistently (see [§6](#6-deidentification-recipe-creation-pipeline-stage-3---4) ).

### 5.4 Profile/Options description

The profiles and options are columns in the standard and private tags template ([§5.1](#51-standard-tags-template) and [§5.2](#52-private-tags-template)). For each tag and profile column we provide a specific action ([§5.3](#53-tagprofile-specific-actions)), that will be applied to the tag during the deidentification process. 
In this section we give more details on the actions specifically required for the single profiles.

#### 5.4.1 Basic Application Confidentiality Profile - Action Mapping Logic

The Basic Application Confidentiality Profile applies different actions based on the DICOM standard's basic profile codes and the tag's Value Representation (VR). The `retrieve_tags.py` script implements the following logic to convert DICOM standard codes into Luwak actions:

**DICOM Code 'U' (UID Replacement):**
- If VR is `UI`: Maps to `func:generate_hmacuid`

**DICOM Code 'D' (Dummy Value Replacement):**
- If VR is date/time (`DA`, `DT`, `TM`): Maps to `func:set_fixed_datetime` (same action as in KitwareMedical/dicomanonymizer)
- If VR is `UI`: Maps to `func:generate_hmacuid` (same action as in KitwareMedical/dicomanonymizer)
- If VR is replaceable type (`AE`, `LO`, `LT`, `SH`, `PN`, `CS`, `ST`, `UT`, `UC`, `UR`, `DS`, `IS`, `FD`, `FL`, `SS`, `US`, `SL`, `UL`, `AS`, `SQ`, `OD`, `OL`, `OV`, `SV`, `UV`): Maps to `replace`
- If VR is binary (`OB`, `OW`, `OF`, `UN`): Maps to `remove` (same action as TCIA)

**DICOM Code 'Z' (Empty Replacement):**
- If VR is date/time (`DA`, `DT`, `TM`): Maps to (ref)`func:set_fixed_datetime` (same action as in KitwareMedical/dicomanonymizer)
- Otherwise: Maps to `blank`

**DICOM Codes 'Z/D', 'X/Z', 'X/D', 'X/Z/D', 'X/Z/U (Modality Dependent Action Codes):**
- If VR is date/time (`DA`, `DT`, `TM`): Maps to `func:set_fixed_datetime` (same action as in KitwareMedical/dicomanonymizer)
- If code includes UID handling (`X/Z/U*`, `X/D`, `Z/D`, `X/Z/D`) and VR is `UI`: Maps to `func:generate_hmacuid` (same action as in KitwareMedical/dicomanonymizer)
- If code is `X/Z/U*` or `X/Z`: Maps to `blank` (same action as in KitwareMedical/dicomanonymizer)
- If code is `X/D`, `Z/D`, or `X/Z/D`:
  - If VR is binary (`OB`, `OW`, `OF`, `UN`): Maps to `remove` (same action as TCIA)
  - Otherwise: Maps to `replace` (same action as in KitwareMedical/dicomanonymizer)
- Any unhandled case: Maps to `manual_review`

**Special Cases:**
- PatientID (0010,0020): Overridden to `func:generate_patient_id`
- PatientName (0010,0010): Overridden to `func:generate_patient_id`

**Implementation:** `luwakx/scripts/retrieve_tags.py` function that processes DICOM standard table

**References:** 
- DICOM PS3.15 Appendix E, Table E.1-1:  
  https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E
- KitwareMedical/dicomanonymizer:  
  https://github.com/KitwareMedical/dicom-anonymizer

#### 5.4.2 Retain UIDs Option

The Retain UIDs option preserves original Study, Series, and SOP Instance UIDs instead of generating anonymized replacements. The template generation logic is straightforward:

**DICOM Code 'K' (Keep):**
- Maps to `keep` action

**Implementation:** `generate_retain_uid_profile()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** When this option is selected, the `keep` action preserves original UIDs for longitudinal tracking.

#### 5.4.3 Retain Device Identity Option

The Retain Device Identity option preserves device and manufacturer identification information. The template generation logic:

**DICOM Code 'K' (Keep):**
- Maps to `keep` action

**DICOM Code 'C' (Clean):**
- Maps to `clean_manually` action (requires manual review)

**Implementation:** `retain_device_id_option()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Retains device serial numbers, station names, and manufacturer information that would otherwise be removed or replaced by Basic Profile.

#### 5.4.4 Retain Institution Identity Option

The Retain Institution Identity option preserves institution name and address information. The template generation logic:

**DICOM Code 'K' (Keep):**
- Maps to `keep` action

**Implementation:** `retain_institution_id_option()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Retains institution name, institution address, and institutional department name that would otherwise be removed or replaced by Basic Profile.

#### 5.4.5 Retain Patient Characteristics Option

The Retain Patient Characteristics option preserves demographic patient information. The template generation logic:

**DICOM Code 'K' (Keep):**
- Maps to `keep` action

**Implementation:** `generate_retain_patient_characteristics_profile()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Retains patient age, sex, size, and weight that would otherwise be removed or replaced by Basic Profile.

#### 5.4.6 Retain Longitudinal Temporal Information with Full Dates Option

The Retain Longitudinal Full Dates option preserves all original date and time values without modification. The template generation logic:

**DICOM Code 'K' (Keep):**
- Maps to `keep` action

**Implementation:** `generate_retain_long_full_dates_profile()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Retains all original dates and times that would otherwise be replaced with fixed epoch values by Basic Profile. Essential for longitudinal studies requiring accurate temporal relationships.

#### 5.4.7 Retain Longitudinal Temporal Information with Modified Dates Option

The Retain Longitudinal Modified Dates option applies consistent date shifting to maintain temporal relationships while obscuring absolute dates. The template generation logic:

**DICOM Code 'C' (Clean/Modify):**
- If VR is `TM` (Time): Maps to `keep` (times are preserved) (same action as TCIA)
- If VR is `DA` or `DT`: Maps to `func:generate_hmacdate_shift` (dates are shifted)

**Implementation:** `generate_retain_long_modified_dates_profile()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Applies HMAC-based deterministic date shifting to all date fields while preserving time fields. All dates for the same patient are shifted by the same amount, maintaining relative temporal relationships for longitudinal analysis.

#### 5.4.8 Clean Descriptors Option

The Clean Descriptors option applies LLM-based PHI detection to textual descriptor fields. The template generation logic:

**DICOM Code 'C' (Clean):**
- Maps to `func:clean_descriptors_with_llm`

**Implementation:** `clean_profiles()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Sends textual descriptors (Study Description, Series Description, Protocol Name, etc.) to an LLM for PHI/PII detection. Tags containing PHI are removed or replaced; clean tags are retained unchanged.

#### 5.4.9 Clean Structured Content Option

The Clean Structured Content option identifies structured content sequences requiring manual PHI review. The template generation logic:

**DICOM Code 'C' (Clean):**
- Maps to `clean_manually`

**Implementation:** `clean_profiles()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Flags structured content sequences (e.g., Content Sequence) for manual review.

**Note:** This option is not currently automated in Luwak and requires manual intervention.

#### 5.4.10 Clean Graphics Option

The Clean Graphics option identifies graphic annotation sequences requiring manual PHI review. The template generation logic:

**DICOM Code 'C' (Clean):**
- Maps to `clean_manually`

**Implementation:** `clean_profiles()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Flags graphic annotation sequences for manual review, as text annotations overlaid on images may contain PHI.

**Note:** This option is not currently automated in Luwak and requires manual intervention.

#### 5.4.11 Retain Safe Private Option

The Retain Safe Private option preserves private DICOM tags that have been identified as safe to retain according to DICOM PS3.15 Appendix E.3.10 and TCIA guidelines. This option is a column of actions in the private tags template for each tag, like it was for the standard tags template. 
Here are the available actions for this option:

**Action: `keep`**
- Retains the original value of safe private tags
- TCIA specifies this action as `k`
- Note: when TCIA specifies an action as `d` remove, but the DICOM standard table lists it as safe, we keep it.

**Action: `func:generate_hmacuid`**
- Applies HMAC-based UID anonymization to private UID tags
- TCIA specifies this action as `h`

**Action: `func:generate_hmacdate_shift`**
- Applies consistent date shifting to private date tags
- TCIA specifies this action as `o`

**Effect:** 
- Preserves vendor-specific tags deemed safe by DICOM standards and TCIA review
- All other private tags are removed 
- Private UIDs and dates are anonymized using the same cryptographic methods as standard tags

**Notes**
- Private tags can have several VR for the same tag, hence it is possible that the action of `func:generate_hmacuid` and `func:generate_hmacdate_shift` could be prescribed to VRs which are not `UI` or `DA`/`DT`. This is handled by the deidentification process directly, by issuing a warning (see [§6.4.1](#641-translation-logic-by-action)).

**Reference:** 
- DICOM PS3.15 Appendix E.3.10 - Safe Private Attributes
- TCIA Private Tag Knowledge Base

#### 5.4.12 Profile Generation Error Handling

During template generation, the `retrieve_tags.py` script attempts to handle unexpected or malformed data:

**Unrecognized Profile Values:**
- When a profile column contains unrecognized values (not matching expected DICOM action codes), a `print()` warning is issued to stderr
- Warning format: `"Warning: Unrecognized profile value '<value>' for tag <tag> in profile <profile_name>"`
- Affected functions include all profile generators: `generate_basic_profile()`, `generate_retain_uid_profile()`, `generate_retain_device_id_profile()`, `generate_retain_institution_id_profile()`, `generate_retain_patient_chars_profile()`, `generate_retain_long_full_dates()`, `generate_retain_long_modified_dates()`, `generate_clean_descriptors()`, `generate_clean_structured_content()`, `generate_clean_graphics()`

**Fallback Behaviors:**
- **Non-Jitterable VRs:** In `generate_retain_long_modified_dates()`, if a VR type cannot support date jittering (e.g., VR types other than `DA`, `DT`, `TM`), the action is set to `remove` rather than attempting an invalid jitter operation
  - Example: A tag with VR `OB` in the modified dates column will be set to `remove` instead of `func:generate_hmacdate_shift`
- **Missing Columns:** If required columns (like `Final CTP Script`) are missing from input templates, warnings are printed and processing continues with available data

**Implementation:** Print statements throughout profile generation functions in `luwakx/scripts/retrieve_tags.py`

**Note:** Print statements are used instead of logger calls in `retrieve_tags.py` because this script runs as a standalone utility during template generation, separate from the main Luwak logging infrastructure.

### 5.5 Custom Tag Templates

Users can override default tag templates via configuration:

```json
{
  "customTags": {
    "standard": "/path/to/custom_standard_tags.csv",
    "private": "/path/to/custom_private_tags.csv"
  }
}
```

If specified paths don't exist, default templates are used with a warning logged.


---

## 6. Deidentification Recipe Creation (pipeline stage 3 - 4)

### 6.1 Recipe Builder Overview
The `anonymization_recipe_builder.py` module generates DEID recipe (check https://pydicom.github.io/deid/getting-started/dicom-config/ for more info on DEID) files by processing the CSV tags templates based on selected deidentification profiles.
These recipes are used for the deidentifications of dicom headers.
Typically the actions in the recipes are written by specifying the action with capital letters, e.g. `REPLACE`, then the tag or keyword representing the tag, e.g. `(0010,1010)`, and the replacement value (or date shift value) for action `REPLACE` (or `JITTER`), or nothing for action `KEEP`, `REMOVE`, `BLANK`. 

**Function:** `make_recipe_file(recipes_to_process, recipe_folder, config)`

### 6.2 Deidentification Profiles

The recipe builder maps profile names from config file, to CSV columns:

```python
recipe_column_map = {
    'basic_profile': 'Basic Prof.',
    'retain_uid': 'Rtn. UIDs Opt.',
    'retain_device_id': 'Rtn. Dev. Id. Opt.',
    'retain_institution_id': 'Rtn. Inst. Id. Opt.',
    'retain_patient_chars': 'Rtn. Pat. Chars. Opt.',
    'retain_long_full_dates': 'Rtn. Long. Full Dates Opt.',
    'retain_long_modified_dates': 'Rtn. Long. Modif. Dates Opt.',
    'clean_descriptors': 'Clean Desc. Opt.',
    'clean_structured_content': 'Clean Struct. Cont. Opt.',
    'clean_graphics': 'Clean Graph. Opt.'
}
```

### 6.3 Dummy Value Replacement Rules

When the `replace` or `blank` actions are applied during recipe generation, Luwak generates VR-specific dummy values to maintain DICOM file validity while removing PHI. The replacement values are determined by the tag's Value Representation (VR) as defined in DICOM PS3.5 Section 6.2.

**Reference:** DICOM PS3.5 2025d, Section 6.2, Table 6.2-1  
URL: https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html

#### 6.3.1 Replace Action - Dummy Value Generation

When the tags template contains a `replace` action, the recipe builder generates VR-specific dummy values:

- **Text VRs** (`AE`, `CS`, `LO`, `LT`, `SH`, `ST`, `UC`, `UR`, `UT`): `"ANONYMIZED"`
- **Person Name** (`PN`): `"Anonymized^Anonymized"`
- **Numeric String VRs** (`DS`, `IS`): `"0"`
- **Age String** (`AS`): `"000D"` (0 days)
- **Binary Numeric VRs** (`FD`, `FL`, `SL`, `SS`, `UL`, `US`, `SV`, `UV`, `OD`, `OL`, `OV`): struct-packed zeros (byte length varies by VR)
- **Sequences** (`SQ`): 
  - If the TCIA removes the tag (`Final CTP Script` column contains `@remove()` or `removed`) we remove it too
  - Otherwise: Commented out with for manual review
  - A warning is logged for sequences requiring manual review

**Reference:**
- The substitution of these dummy value is structured as it was for https://laurelbridge.com/pdf/Dicom-Anonymization-Conformance-Statement.pdf 

#### 6.3.2 Blank Action - Empty Value Generation

When the tags template contains a `blank` action, the recipe builder generates empty values:

- **Text VRs**: Empty string `""`
- **Numeric String VRs** (`DS`, `IS`): Empty string `""`
- **Binary VRs**: Empty bytes `b''`
- **Other VRs**: Empty string `""` (fallback)

**Distinction:**
- **`replace`**: Writes meaningful dummy values (e.g., "ANONYMIZED", 0)
- **`blank`**: Writes empty/null values (empty string or empty bytes)

The following table summarizes the dummy values generated for each VR type when the `replace` or `blank` action is applied:

| VR Code | VR Name | Replace Value | Blank Value | Generation Function |
|---------|---------|---------------|-------------|---------------------|
| **Text String VRs** |
| `AE` | Application Entity | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| `CS` | Code String | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| `LO` | Long String | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| `LT` | Long Text | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| `SH` | Short String | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| `ST` | Short Text | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| `UC` | Unlimited Characters | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| `UR` | Universal Resource | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| `UT` | Unlimited Text | `"ANONYMIZED"` | `""` (empty string) | Direct string |
| **Person Name VR** |
| `PN` | Person Name | `"Anonymized^Anonymized"` | `""` (empty string) | Direct string |
| **Numeric String VRs** |
| `DS` | Decimal String | `"0"` | `""` (empty string)  | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `IS` | Integer String | `"0"` | `""` (empty string)  | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| **Age String VR** |
| `AS` | Age String | `"000D"` (0 days) | `""` (empty string) | Direct string |
| **Binary Numeric VRs (8 bytes)** |
| `FD` | Floating Point Double | `0.0` (8 bytes, little-endian) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `SV` | Signed 64-bit Very Long | `0` (8 bytes signed) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `UV` | Unsigned 64-bit Very Long | `0` (8 bytes unsigned) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `OD` | Other Double | `0.0` (8 bytes) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `OV` | Other 64-bit Very Long | `0` (8 bytes) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| **Binary Numeric VRs (4 bytes)** |
| `FL` | Floating Point Single | `0.0` (4 bytes, little-endian) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `SL` | Signed Long | `0` (4 bytes signed) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `UL` | Unsigned Long | `0` (4 bytes unsigned) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `OL` | Other Long | `0` (4 bytes) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| **Binary Numeric VRs (2 bytes)** |
| `SS` | Signed Short | `0` (2 bytes signed) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| `US` | Unsigned Short | `0` (2 bytes unsigned) | `b''` (empty bytes) | `set_values_to_zero(vr)` / `set_empty_value(vr)` |
| **Sequence VR** |
| `SQ` | Sequence | Removed or commented out (conditional) | N/A | Conditional based on `Final CTP Script` |

**Notes:**
- All binary numeric values are encoded in little-endian format following DICOM conventions
- `set_values_to_zero(vr)` uses Python's `struct.pack()` to generate properly formatted binary zeros
- `set_empty_value(vr)` generates empty bytes (`b''`) for binary VRs, empty string for text VRs
- Person Name follows DICOM component group structure: `FamilyName^GivenName`
- For VR = SQ, the recipe builder checks the `Final CTP Script` column from the tags template to determine sequence disposition:
  - Sequences explicitly marked for removal (`@remove()` or `removed`) generate `REMOVE` directives
  - All other sequences are commented out to prevent automated processing and require manual review, and an warning is logged for that tag.
  - Child tags within sequences can be individually specified when necessary, by using the syntax from [§5.1.4](#514-nested-sequence-support) in a custom tags template (see [§5.5](#55-custom-tag-templates))
- When a tag is listed in the recipe with an action, DEID will apply this action even if that tag is inside a sequence.

**Current limitations**
- The dummy assignment logic for `replace` and `blank` actions currently supports a limited set of VRs, if the DICOM PS3.15 Appendix E, Table E.1-1 will include more in the future, support for the additional VRs will have to be added.

### 6.4 Recipe Builder Action-to-Recipe Translation

The recipe builder (`anonymization_recipe_builder.py`) translates the final determined actions from the CSV templates into deid-compatible recipe format. The translation logic processes each tag based on its action and VR type:

#### 6.4.1 Translation Logic by Action

**`keep` Action:**
```
KEEP (tag)
```
- Retains original tag value unchanged

**`remove` Action:**
```
REMOVE (tag)
```
- Deletes tag entirely from DICOM file

**`replace` Action:**
- Already discussed at [§6.3.1](#631-replace-action---dummy-value-generation)

**`blank` Action:**
- Already discussed at [§6.3.2](#632-blank-action---empty-value-generation)

**`func:generate_hmacuid` Action:**
```
REPLACE (tag) func:generate_hmacuid
```

**`func:set_fixed_datetime` Action:**
```
REPLACE (tag) func:set_fixed_datetime
```

**`func:generate_hmacdate_shift` Action:**
```
JITTER (tag) func:generate_hmacdate_shift
```

**`func:clean_descriptors_with_llm` Action:**
```
REPLACE (tag) func:clean_descriptors_with_llm
```

**`func:generate_patient_id` Action:**
```
REPLACE (tag) func:generate_patient_id
```

**`clean_manually` Action:**
```
# REPLACE (tag) CLEANED NEEDS MANUAL REVIEW
```
Commented out, requires manual intervention

**`manual_review` Action:**
```
# REPLACE (tag) MANUAL REVIEW NEEDED
```
Commented out, requires manual intervention

**Private tag handling:**

If `retain_safe_private_tags` selected: Processes private_tags_template.csv and generates directives for safe private tags

- `keep`:
  -generates: 
`KEEP (group,"private_creator",element)`
- `func:generate_hmacuid`:
  - Generates: `REPLACE (group,"private_creator",element) func:generate_hmacuid`
- `func:generate_hmacdate_shift`
  - Generates: `JITTER (group,"private_creator",element) func:generate_hmacdate_shift`
  - Conditional: Only applied if `retain_long_modified_dates` is also selected in the recipe list
  - If `retain_long_modified_dates` is not selected, these tags are not kept
  - Example: `JITTER (0009,"GEMS_IDEN_01",27) func:generate_hmacdate_shift`
- Note: Private tags may have different VRs for a single tag. The custom methods, `generate_hmacuid` and `generate_hmacdate_shift`, will issue a single warning per series if the date shift is attempted to VRs that are not `DA` or `DT`, and UID generation on VRs that are not `UI`. Please check the logs and verify the correct functioning for those tags.

*Private Tag Notation:*
- Private tags use the format: `(group,"private_creator",element)`
- The element uses only the last two hex digits (e.g., `10` from `(0019,1010)`)
- The private creator string identifies which vendor/device created the tag

*Implementation:* `make_recipe_file()` in `luwakx/anonymization_recipe_builder.py`

*Reference:* 
- DICOM PS3.15 Appendix E.3.10 - Safe Private Attributes
- TCIA Private Tag Knowledge Base

**Current limitations:**
- If a final action code is not recognized during recipe generation, a `logger.error` is issued and the tag is skipped. Check the logs for "Unrecognized final action" messages.
- Certain VR types and sequence tags may require manual review:
  - VR types that cannot be automatically processed generate `logger.warning` messages
  - Manual cleaning requirements are logged with `logger.warning` 
- Unrecognized private tag disposition actions trigger `logger.warning` messages indicating the need for manual verification
- Unknown VR types in value generation functions trigger `logger.warning` messages (e.g., in `set_values_to_zero()` and `set_empty_value()`)

#### 6.4.2 Additional Recipe Directives

After processing all tags from the templates (or custom files), the recipe builder adds additional directives, based on selected profiles, through the `ADD` directive in the recipe. This action allows to add new DICOM tags (either listed with their tag or their keyword) to the data, or replace them if they already exist.

**If `basic_profile` is selected:**
```
ADD PatientIdentityRemoved YES
REMOVE ALL func:is_tag_private
REMOVE ALL func:is_curve_or_overlay_tag
ADD DeidentificationMethod LUWAK_ANONYMIZER
```
The remove action here uses the custom method `is_curve_or_overlay_tag`
*Purpose:* Remove curve data and overlay annotations per DICOM PS3.15.

*Method:*
- Checks if tag is:
  - Curve Data: Group (50xx,xxxx) where xx is even
  - Overlay Data: (60xx,3000) where xx is even
  - Overlay Comments: (60xx,4000) where xx is even
- Used with REMOVE ALL to delete all Curve Data, Overlay Data, Overlay Comments tags

*Implementation:* `DicomProcessor.is_curve_or_overlay_tag()`


*Private Tag Removal:*

The basic profile always removes private tags with:
```
REMOVE ALL func:is_tag_private
```
This ensures that only explicitly retained safe private tags are preserved when selecting `retain_safe_private_tags` option; all other private tags are removed.

*Purpose:* Identify and remove private tags.

*Method:*
- Checks if tag has private creator attribute
- Returns True for private tags with valid creator
- Used with REMOVE ALL to delete all private tags

**Implementation:** `DicomProcessor.is_tag_private()`

**Longitudinal Temporal Information:**
- If `basic_profile` is selected and neither `retain_long_full_dates` nor `retain_long_modified_dates` selected:
  ```
  ADD LongitudinalTemporalInformationModified REMOVED
  ```
- If `retain_long_full_dates` selected:
  ```
  ADD LongitudinalTemporalInformationModified UNMODIFIED
  ```
- If `retain_long_modified_dates` selected:
  ```
  ADD LongitudinalTemporalInformationModified MODIFIED
  ```

**If `clean_recognizable_visual_features` is selected and defacing is performed:**
```
ADD RecognizableVisualFeatures NO
```
(Modify in code to do it only if the action was performed)

**DeidentificationMethodCodeSequence removal:**
```
REMOVE (0012,0064)
```
This tag is removed from the data in case some pre-deidentification was already applied to the data. The tag is injected to the data again only after the entire deidentification process is complete ([§7](#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-5)).

### 6.5 Action Priority Rules

When multiple profiles are selected, actions are prioritized in the following order:

1. **`keep`** - Highest priority (retention options override removal)
2. **`func:generate_hmacdate_shift`** - Date shifting for longitudinal consistency
3. **`func:generate_hmacuid`** - UID anonymization
4. **`func:clean_descriptors_with_llm`** - LLM-based cleaning
5. **`replace`** - Generic replacement
6. **`func:set_fixed_datetime`** - Fixed datetime
7. **`blank`** - Blanking/emptying
8. **`remove`** - Removal (lowest priority)

#### 6.5.1

This action priority is based on the action priority in DEID, for which e.g., if a tag is specified with an action `KEEP`, i will always be kept even if somewhere in the recipe that same tag has the action of `REMOVE`.

Luwak has a testing suite that allows to verify that this logic is kept also when mixing different options and profiles together. 
Examples of these tests are:
- `test_keep_specific_private_tags_should_be_original_value`: Test that when specific private tags are marked to be retained, their original values are preserved in the anonymized output.
- `test_basic_retain_uid_should_have_original_uid` : Test that mixing basic profile and retain uid option keeps original UID for retained fields
- `test_basic_retain_date_should_have_original_date`: Test that mixing retain and date shift keeps original date for retain fields
- `test_basic_modified_date_should_have_modified_date`: Test that mixing basic profile and date shift modifies original date.


### 6.6 Generated Recipe File Format

Output file: `deid.dicom.recipe`

**Example content:**
```
FORMAT dicom

%header

# Patient's Name
REPLACE (0010,0010) Anonymized^Anonymized

# Patient ID
REPLACE (0010,0020) func:generate_patient_id

# Study Instance UID
REPLACE (0020,000d) func:generate_hmacuid

# Study Date
JITTER (0008,0020) func:generate_hmacdate_shift

# Study Description
REPLACE (0008,1030) func:clean_descriptors_with_llm

ADD PatientIdentityRemoved YES
REMOVE ALL func:is_curve_or_overlay_tag
ADD DeidentificationMethod LUWAK_ANONYMIZER
ADD LongitudinalTemporalInformationModified MODIFIED
REMOVE (0012,0064)
REMOVE ALL func:is_tag_private
```

---

## 7. DeidentificationMethodCodeSequence Attribute Injection (pipeline stage 5)

### 7.1 Purpose
After deidentification, Luwak injects the DeidentificationMethodCodeSequence (0012,0064) tag to document which deidentification methods were applied per DICOM PS3.15 requirements.

### 7.2 Implementation
**Module:** `DicomProcessor.inject_deidentification_method_code_sequence()`

**Process:**
1. Maps selected recipe profiles to CID 7050 codes
2. Conditionally includes defacing code (113101) only if defacing was performed
3. Sorts sequence items by CodeValue for consistency
4. Injects sequence into all anonymized files in series

### 7.3 Code Mapping
Each recipe profile maps to a specific DICOM CID 7050 code (see [§3.1](#31-overview)).

**Example:**
For profiles: `['basic_profile', 'retain_long_modified_dates', 'clean_recognizable_visual_features']` with successful defacing:

```python
DeidentificationMethodCodeSequence = [
    Dataset({
        'CodeValue': '113100',
        'CodingSchemeDesignator': 'DCM',
        'CodeMeaning': 'Basic Application Confidentiality Profile'
    }),
    Dataset({
        'CodeValue': '113101',
        'CodingSchemeDesignator': 'DCM',
        'CodeMeaning': 'Clean Recognizable Visual Features Option'
    }),
    Dataset({
        'CodeValue': '113107',
        'CodingSchemeDesignator': 'DCM',
        'CodeMeaning': 'Retain Longitudinal Temporal Information Modified Dates Option'
    })
]
```
**Reference:** DICOM PS3.16 CID 7050 - De-identification Method  
URL: https://dicom.nema.org/medical/dicom/current/output/chtml/part16/sect_CID_7050.html

---

## 8. Deidentified Data and Metadata Export (pipeline stage 6)


### 8.1 Output Files Generated by Luwak

Luwak produces several output files during the deidentification pipeline, each serving a specific purpose for data integrity, traceability, and downstream analysis:

**1. Deidentified DICOM Files (`*.dcm`)**
  - Location: Output directory specified in configuration
  - Content: Fully deidentified DICOM files, with all PHI removed or replaced according to the selected recipe and profiles
  - Generation: Created at the end of the deidentification pipeline stage 5.

**2. UID Mappings CSV (`uid_mappings.csv`)**
  - Location: Private mapping folder
  - Content: Table mapping original UIDs (e.g., StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID) to anonymized UIDs for each file, including patient identifiers
  - Generation: Incrementally appended after each series is processed, using the `MetadataExporter.append_series_uid_mappings()` method

**3. DICOM Metadata Parquet (`metadata.parquet`)**
  - Location: Private mapping folder
  - Content: Tabular export of selected DICOM metadata fields for all processed series, excluding sensitive tags as configured
  - Generation: Appended after each series using `MetadataExporter.append_series_metadata()` and finalized with `export_metadata_to_parquet()`
  - Structure: Each row contains metadata extracted from one DICOM file per series. Each column corresponds to a DICOM tag retained in the export.
  - Column Naming: 
    - **Standard tags**: Use the official DICOM keyword (e.g., `PatientID`, `StudyInstanceUID`)
    - **Private tags**: Constructed as `{PrivateCreator}_{TagName}` if tag name is known (e.g., `GEMS_IDEN_01_AcquisitionProtocolName`), or `{PrivateCreator}_{group}xx{element}` if tag name is unknown (e.g., `GEMS_IDEN_01_0019xx10`). Spaces in private creator names are replaced with underscores.
  - The metadata.parquet file can be opened and read using Python libraries such as pandas (pandas.read_parquet) or pyarrow, which support efficient loading and analysis of Parquet-format tabular data

**4. NRRD Volumes (`image.nrrd`, `image_defaced.nrrd`)**
  - Location: `image.nrrd` in private mapping folder, `image_defaced.nrrd` in output directory
  - Content: 3D reconstructed volumes of the original and defaced image data, used for validation and further analysis
  - Generation: Created during defacing stage and moved to final destinations by `MetadataExporter._move_nrrd_files()`

**5. Log Files**
  - Location: As configured (default: output directory)
  - Content: Detailed logs of processing steps, errors, and warnings for traceability
  - Generation: Written throughout the pipeline by the configured logger

**6. Recipe File (`deid.dicom.recipe`)**
  - Location: Recipes folder as configured in `recipesFolder` (default: output directory)
  - Content: Generated deidentification recipe file in deid format, specifying all tag-level transformations to be applied
  - Generation: Created during recipe generation stage (pipeline stage 3) by `anonymization_recipe_builder.py` based on selected profiles and options
  - Purpose: Input file for deid library's `replace_identifiers()` function during metadata deidentification
  - Persistence: Retained after processing for audit and reproducibility purposes

**7. Patient UID Database (`patient_uid.db`)**
  - Location: As configured in the optional `analysisCacheFolder` (if not specified: private mapping folder, temporary)
  - Content: SQLite database storing patient ID mappings and cryptographically secure random tokens for HMAC-based UID and date anonymization
  - Generation: Created and updated during anonymization stage by `PatientUidDatabase` class
  - Persistence: Removed after processing unless `analysisCacheFolder` is specified in configuration
  - Structure: Contains mappings from original patient identifiers (hashed) to anonymized patient IDs and per-patient cryptographic tokens

**8. LLM Cache Database (`llm_cache.db`)**
  - Location: As configured in the optional `analysisCacheFolder` (if not specified: private mapping folder, temporary)
  - Content: SQLite database caching LLM API responses for descriptor cleaning to avoid redundant API calls
  - Generation: Created and updated during descriptor cleaning if `clean_descriptors` profile is selected
  - Persistence: Removed after processing unless `analysisCacheFolder` is specified; when specified, retained across multiple anonymization runs for performance optimization
  - Thread-safe: Supports concurrent access from parallel processing workers

#### Export Logic
The export of metadata and mappings is performed in a streaming, memory-efficient manner. After each series is processed, results are immediately written to the corresponding output files. The `MetadataExporter` class manages all export operations, including incremental appending and finalization of CSV and Parquet files, and movement of NRRD volumes.

For more details, see:
- `luwakx/metadata_exporter.py` (export logic)
- `luwakx/processing_pipeline.py` (pipeline orchestration)

---

## 9. Configuration, Code Design, and Usage

### 9.1 Configuration File

Luwak uses a JSON configuration file (`luwak-config.json`) to control all aspects of the deidentification pipeline. The configuration file path should be specified via command-line argument.

#### 9.1.1 Required Configuration Options

| Option | Type | Description |
|--------|------|-------------|
| `inputFolder` | string | Path to folder containing DICOM files to process |
| `outputDeidentifiedFolder` | string | Path to folder for anonymized DICOM output |
| `outputPrivateMappingFolder` | string | Path to folder for private mapping files (UID mappings, metadata) |
| `recipesFolder` | string | Path to folder where recipe files will be generated |
| `recipes` | array | List of deidentification profiles/options to apply (e.g., `["basic_profile", "retain_uid"]`) |

#### 9.1.2 Optional Configuration Options

**Path Resolution:**
- All paths can be absolute or relative to the config file location
- `~` in paths is expanded to user home directory

**Deidentification Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `projectHashRoot` | string | "myproject2025" | Root hash for deterministic anonymization across project (required for HMAC-based anonymization) |
| `maxDateShiftDays` | integer | 1095 | Maximum days for date shifting (3 years default) |
| `patientIdPrefix` | string | "Zenta" | Prefix for generated patient IDs (e.g., "Zenta000001") |

**Database and Cache Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `analysisCacheFolder` | string | none | Path to folder for persistent analysis databases (`patient_uid.db` and `llm_cache.db`). If not specified, temporary databases are created in the private mapping folder and deleted after processing. If specified and folder exists with databases, they will be loaded and updated; if not, new databases will be created. Databases persist across anonymization runs to ensure consistent patient ID and UID mappings and to cache LLM results for performance. |

**LLM Descriptor Cleaning Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `cleanDescriptorsLlmBaseUrl` | string | "https://api.openai.com/v1" | Base URL for LLM API (OpenAI-compatible) |
| `cleanDescriptorsLlmModel` | string | "gpt-4o-mini" | Model name for LLM service |
| `cleanDescriptorsLlmApiKeyEnvVar` | string | "" | Environment variable name containing API key (empty by default) |

**Custom Tag Templates:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `customTags.standard` | string | built-in | Path to custom CSV for standard DICOM tags |
| `customTags.private` | string | built-in | Path to custom CSV for private DICOM tags |

**Logging Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `logLevel` | string | "INFO" | Logging level: `PRIVATE`, `DEBUG`, `INFO`, `WARNING`, `ERROR` (PRIVATE includes sensitive data) |

**Metadata Export Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `excludedTagsFromParquet` | array | ["(7FE0,0010)"] | List of DICOM tags to exclude from Parquet export (accepts integer, hex string, or bracketed formats) |

#### 9.1.3 Example Configuration

```json
{
  "inputFolder": "/data/dicom/input",
  "outputDeidentifiedFolder": "/data/dicom/output",
  "outputPrivateMappingFolder": "/data/dicom/private",
  "recipesFolder": "/data/dicom/recipes",
  "recipes": [
    "basic_profile",
    "retain_long_modified_dates",
    "clean_descriptors",
    "clean_recognizable_visual_features"
  ],
  "projectHashRoot": "my_secure_project_key_12345",
  "maxDateShiftDays": 1095,
  "patientIdPrefix": "Study",
  "analysisCacheFolder": "./analysis_cache",
  "cleanDescriptorsLlmBaseUrl": "https://openrouter.ai/api/v1",
  "cleanDescriptorsLlmModel": "openai/gpt-4o-mini",
  "cleanDescriptorsLlmApiKeyEnvVar": "OPENROUTER_API_KEY",
  "logLevel": "INFO",
  "excludedTagsFromParquet": ["(7FE0,0010)"]
}
```

### 9.2 Code Architecture and Design

#### 9.2.1 Overview

Luwak follows an object-oriented design with clear separation of concerns:

1. **Configuration Layer** (`LuwakAnonymizer`) - Config loading, validation, and path resolution
2. **Data Model Layer** (`DicomFile`, `DicomSeries`) - DICOM file and series representation
3. **Processing Layer** (`ProcessingPipeline`, `DicomProcessor`, `DefaceService`) - Core anonymization logic
4. **Storage Layer** (`PatientUIDDatabase`, `LLMResultCache`) - Persistent data management
5. **Export Layer** (`MetadataExporter`) - Output file generation

#### 9.2.2 Core Classes and Relationships

**`LuwakAnonymizer` (Main Entry Point)**
- **Purpose:** Configuration management and pipeline orchestration
- **Key Responsibilities:**
  - Load and validate configuration
  - Resolve and create directory paths
  - Initialize shared resources (UID database, LLM cache)
  - Generate deidentification recipes
  - Coordinate the anonymization workflow
- **Key Methods:**
  - `__init__(config_path)` - Initialize with config file
  - `anonymize()` - Execute full anonymization pipeline
  - `load_config()` - Load and validate configuration
  - `setup_paths()` - Resolve and create directories

**`DicomFile`**
- **Purpose:** Represents a single DICOM file with all its path variants
- **Key Attributes:**
  - `original_path` - Original file location
  - `organized_path` - Path in organized temp structure
  - `defaced_path` - Path after defacing (if applicable)
  - `anonymized_path` - Final anonymized file path
- **Key Methods:**
  - `get_current_path()` - Get most recent path based on processing status
  - `get_relative_original_path(input_folder)` - Calculate relative path from input
  - `get_relative_anonymized_path(output_folder)` - Calculate relative path from output

**`DicomSeries`**
- **Purpose:** Represents a collection of DICOM files belonging to one series
- **Key Attributes:**
  - `files` - List of DicomFile objects
  - `original_series_uid` - Original SeriesInstanceUID
  - `anonymized_series_uid` - Anonymized SeriesInstanceUID
  - `modality` - Series modality (CT, MR, etc.)
  - `processing_status` - Current processing stage
  - `metadata` - Additional series-level information
- **Key Methods:**
  - `get_file_count()` - Count files in series
  - `update_base_paths()` - Set paths for organized/defaced/output locations

**`DicomSeriesFactory`**
- **Purpose:** Creates DicomSeries objects from directory scanning
- **Key Responsibilities:**
  - Scan input directory for DICOM files
  - Group files by SeriesInstanceUID
  - Assign anonymized UIDs to series
  - Build output directory structure
- **Key Methods:**
  - `discover_files(input_path)` - Scan directory for DICOM files
  - `create_series_from_files(dicom_files)` - Group files into series

**`PipelineCoordinator`**
- **Purpose:** Manages multiple ProcessingPipeline instances for parallel processing (no actual parallel processing is implemented yet)
- **Key Responsibilities:**
  - Distribute series across multiple pipeline workers
  - Coordinate parallel or sequential execution
  - Aggregate results from all workers
  - Manage shared resources (UID database, LLM cache, recipe)
- **Key Methods:**
  - `create_from_dicom_files()` - Factory method to create coordinator from input
  - `run_all_pipelines_sequential()` - Execute all workers sequentially
  - `finalize_exports()` - Verify and finalize all export files

**`ProcessingPipeline`**
- **Purpose:** Orchestrates processing of DicomSeries through all stages
- **Key Attributes:**
  - `series_collection` - Dictionary of series being processed
  - `current_stage` - Pipeline stage (INPUT_SCANNING, ORGANIZED, DEFACED, etc.)
  - `processor` - DicomProcessor instance
  - `deface_service` - DefaceService instance
  - `exporter` - MetadataExporter instance
- **Key Methods:**
  - `run_full_pipeline()` - Process all series through all stages
  - `_process_single_series(series)` - Process one series completely
  - `_organize_series(series)` - Copy files to organized structure
  - `_deface_series(series)` - Apply face defacing
  - `_anonymize_series(series)` - Apply metadata anonymization
  - `_export_series_results_incremental(series)` - Export results

**`DicomProcessor`**
- **Purpose:** Core DICOM metadata anonymization using deid library
- **Key Responsibilities:**
  - Apply recipe rules to DICOM tags
  - Execute custom anonymization functions (UID generation, date shifting, LLM cleaning)
  - Inject DeidentificationMethodCodeSequence
  - Track UID mappings
- **Key Methods:**
  - `process_series(series, recipe)` - Anonymize all files in series
  - `generate_hmacuid()` - Generate anonymized UIDs
  - `generate_patient_id()` - Generate anonymized patient IDs
  - `generate_hmacdate_shift()` - Shift dates consistently
  - `clean_descriptors_with_llm()` - Clean text descriptors
  - `inject_deidentification_method_code_sequence()` - Add CID 7050 codes

**`DefaceService`**
- **Purpose:** Face defacing for CT/PET imaging volumes
- **Key Responsibilities:**
  - Load DICOM series as 3D volume
  - Apply MOOSE-based face segmentation
  - Pixelate detected facial regions
  - Export defaced DICOM files
- **Key Methods:**
  - `process_series(series)` - Deface entire series and return result metadata

**`PatientUIDDatabase`**
- **Purpose:** Thread-safe SQLite database for patient ID and UID mappings
- **Key Responsibilities:**
  - Store patient identifier mappings
  - Generate cryptographic tokens for HMAC operations
  - Ensure consistent anonymization across multiple runs
- **Key Methods:**
  - `get_cached_patient_id()` - Retrieve existing patient ID mapping
  - `store_patient_id()` - Store new patient ID mapping with token
  - `get_stats()` - Database statistics

**`LLMResultCache`**
- **Purpose:** Thread-safe SQLite cache for LLM API responses
- **Key Responsibilities:**
  - Cache descriptor cleaning results
  - Avoid redundant API calls
  - Support concurrent access from parallel workers
- **Key Methods:**
  - `get_cached_result(input_text, model)` - Retrieve cached result
  - `store_result(input_text, model, phi_result)` - Store result
  - `get_cache_stats()` - Cache statistics

**`MetadataExporter`**
- **Purpose:** Export UID mappings, metadata, and NRRD volumes
- **Key Responsibilities:**
  - Stream UID mappings to CSV
  - Stream metadata to Parquet
  - Move NRRD files to final destinations
- **Key Methods:**
  - `append_series_uid_mappings()` - Append UID mappings for one series
  - `append_series_metadata()` - Append metadata for one series
  - `extract_dicom_metadata()` - Extract metadata from anonymized file

#### 9.2.3 Data Flow

```
1. Configuration Loading (LuwakAnonymizer)
   ↓
2. Directory Scanning (DicomSeriesFactory)
   → Creates DicomSeries objects with DicomFile objects
   ↓
3. Recipe Generation (anonymization_recipe_builder)
   → Generates deid.dicom.recipe
   ↓
4. Pipeline Coordination (PipelineCoordinator)
   → Distributes series across workers
   → Manages shared resources (UID DB, LLM cache, recipe)
   ↓
5. Pipeline Processing (ProcessingPipeline) - per worker
   → For each DicomSeries:
      a. Organization Stage
         → Copy files to organized temp structure
      b. Defacing Stage (optional)
         → DefaceService: Volume reconstruction → ML defacing → DICOM export
      c. Anonymization Stage
         → DicomProcessor: Apply recipe → Custom functions → Write files
      d. Injection Stage
         → Add DeidentificationMethodCodeSequence
      e. Export Stage
         → MetadataExporter: Stream UID mappings and metadata
   ↓
6. Result Aggregation (PipelineCoordinator)
   → Finalize exports and verify files
   ↓
7. Cleanup
   → Remove temp directories
   → Close databases
   → Delete temp UID database (if configured)
```

#### 9.2.4 Threading and Parallelization

- **Current Implementation:** Sequential series-by-series processing
- **Thread Safety:** 
  - `PatientUIDDatabase` uses write locks
  - `LLMResultCache` uses write locks
  - Both support concurrent read access
- **Memory Management:** 
  - Series data cleared after export
  - GPU memory cleaned after each series
  - LM Studio worker processes terminated after each series completes processing

### 9.3 Running Luwak

#### 9.3.1 Command-Line Usage

**Basic Usage:**
```bash
python luwakx.py --config_path /path/to/luwak-config.json
```

**Command-Line Arguments:**

| Argument | Description | Default |
|----------|-------------|---------|
| `--config_path` | Path to configuration JSON file | `data/luwak-config.json` |
| `--no-console` | Disable console logging (file only) | False |

**Examples:**

```bash
# Use default config
python luwakx.py

# Use custom config
python luwakx.py --config_path /data/my-config.json

# Run with file logging only
python luwakx.py --config_path /data/my-config.json --no-console
```

#### 9.3.2 Programmatic Usage

```python
from anonymize import LuwakAnonymizer

# Initialize with config file
anonymizer = LuwakAnonymizer('/path/to/luwak-config.json')

# Run anonymization
anonymizer.anonymize()

# Access results
print(f"Processed {anonymizer.processed_series_count} series")
```

#### 9.3.3 Environment Variables

**Required for LLM Descriptor Cleaning:**
```bash
export OPENROUTER_API_KEY="your_api_key_here"
# Or use custom variable name specified in cleanDescriptorsLlmApiKeyEnvVar
```

**Optional for deid Library:**
```bash
export MESSAGELEVEL="DEBUG"  # Set automatically by Luwak based on logLevel
```

#### 9.3.4 Typical Workflow

1. **Prepare Configuration:**
   - Copy example config to `luwak-config.json`
   - Set input/output paths
   - Select deidentification profiles
   - Configure options (date shift, patient ID prefix, etc.)

2. **Set API Keys (if using LLM cleaning):**
   ```bash
   export OPENROUTER_API_KEY="your_key"
   ```

3. **Run Anonymization:**
   ```bash
   python luwakx.py --config_path luwak-config.json
   ```

4. **Review Outputs:**
   - Check `outputDeidentifiedFolder` for anonymized DICOM files
   - Review `outputPrivateMappingFolder` for UID mappings and metadata
   - Check log file in output folder

#### 9.3.5 Log Files

Log files are created in `outputDeidentifiedFolder/luwak.log` and contain:
- Configuration validation results
- Processing progress for each series
- Warning messages for skipped or problematic files
- Error messages with stack traces
- Performance statistics

**Log Levels:**
- `PRIVATE`: Includes sensitive data (original/anonymized values) for audit
- `DEBUG`: Detailed processing information
- `INFO`: Standard processing information (recommended)
- `WARNING`: Warnings about non-critical issues
- `ERROR`: Critical errors

### 9.4 Output File Structure

After processing, the output directory structure looks like:

```
outputDeidentifiedFolder/
├── luwak.log
└── {AnonymizedPatientID}/
    └── {HashedAnonymizedStudyUID}/
        └── {HashedAnonymizedSeriesUID}/
            ├── 000001.dcm
            ├── 000002.dcm
            ├── ...
            └── image_defaced.nrrd (if defacing performed)

outputPrivateMappingFolder/
├── uid_mappings.csv
├── metadata.parquet
├── patient_uid.db (if persistent database configured)
└── {AnonymizedPatientID}/
    └── {HashedAnonymizedStudyUID}/
        └── {HashedAnonymizedSeriesUID}/
            └── image.nrrd (if defacing performed)

recipesFolder/
└── deid.dicom.recipe

analysisCacheFolder/ (if specified in config)
├── patient_uid.db
└── llm_cache.db (if descriptor cleaning used)
```

**Directory Naming Convention:**
- `{AnonymizedPatientID}`: Sequential patient ID (e.g., "Zenta000000", "Zenta000001")
- `{HashedAnonymizedStudyUID}`: Base64-encoded SHA1 hash of anonymized Study UID (first 16 chars)
- `{HashedAnonymizedSeriesUID}`: Base64-encoded SHA1 hash of anonymized Series UID (first 16 chars)
- Files within series are sequentially numbered (000001.dcm, 000002.dcm, etc.)

---

## 10. Limitations and Constraints

### 10.1 Known Limitations
1. **Burned-in annotations:** Cannot remove PHI permanently embedded in pixel data; detection only flags for manual review
2. **Sequence depth:** Nested sequences beyond 3 levels may not be fully processed
3. **Large files:** Files >2GB may impact memory performance in parallel processing
4. **Private tags:** Non-standard private tags may not have complete VR information
5. **Directory naming collisions:** The directory structure uses 16-character truncated hashes of anonymized UIDs for study and series folders. While collision probability is negligible for typical institutional or national-scale datasets, theoretical collisions become possible at extremely large dataset scales (multi-country aggregations exceeding hundreds of millions of studies). The 96 bits of entropy provide strong collision resistance but are not sufficient to guarantee uniqueness at population scales approaching billions of studies. Users managing multi-institutional or international data aggregations should monitor for directory collisions and consider extending hash length if operating at scales exceeding 100 million unique studies.

### 10.2 Dependencies

**Core Dependencies:**
- Python >=3.12
- pydicom - DICOM file reading and writing
- **deid (custom fork)** - DICOM deidentification recipe engine  
  `git+https://github.com/ZentaLabs/deid.git@speed-optimization`
- pandas - Data manipulation for metadata export
- pyarrow - Parquet file format support
- jsonschema - Configuration validation

**Image Processing:**
- SimpleITK - 3D volume reconstruction and DICOM manipulation
- **moosez** - MOOSE framework for medical image segmentation and face detection
- vedo - Visualization and mesh processing

**LLM Integration (Optional):**
- openai==1.103.0 - OpenAI-compatible API client for descriptor cleaning
- httpx, httpcore - HTTP client libraries

**Utilities:**
- requests, beautifulsoup4 - Web scraping for DICOM standard tables
- psutil - System resource monitoring
- tqdm - Progress bars

**Testing:**
- pytest - Test framework

**Note:** The DEID library uses a custom fork with speed optimizations. See `luwakx/requirements.txt` for complete dependency list.

---

## 11. Validation and Testing

### 11.1 Test Suite Overview

Luwak includes a comprehensive test suite validating core functionality and conformance to DICOM standards. Tests are organized by functional area and can be run using pytest.

**Test Modules:**
- `test_anonymize.py` - Core anonymization functionality and custom functions
- `test_config_options.py` - Configuration validation and option handling
- `test_defacer_profile.py` - Face defacing with MOOSE integration
- `test_exports.py` - UID mappings, Parquet metadata, and file exports
- `test_logger.py` - Logging system and custom log levels
- `test_paths.py` - Path resolution and working directory independence

### 11.2 Key Test Cases

**Anonymization Function Tests:**
- `test_uid_generation` - HMAC-based UID anonymization determinism
- `test_generate_hmacdate_shift` - Date shifting consistency and bounds
- `test_fixed_datetime_generation` - Fixed epoch datetime replacement
- `test_generate_patient_id_method` - Sequential patient ID generation with database
- `test_basic_clean_descriptors_should_have_clean_value` - LLM-based descriptor cleaning

**Profile Combination Tests:**
- `test_basic_retain_uid_should_have_original_uid` - Basic profile + retain UID option
- `test_basic_retain_date_should_have_original_date` - Basic profile + retain full dates
- `test_basic_modified_date_should_have_modified_date` - Basic profile + date shifting
- `test_keep_specific_private_tags_should_be_original_value` - Safe private tag retention

**Export and Integration Tests:**
- `test_uid_mapping_file_creation` - UID mappings CSV generation
- `test_parquet_metadata_export` - Parquet metadata export with dynamic schema
- `test_csv_and_parquet_consistency` - Consistency between export formats
- `test_defacer_service_makes_defacing` - Face defacing pipeline integration

**System Tests:**
- `test_script_runs_on_first_file` - Single-file processing
- `test_luwakx_wrapper_script` - Command-line interface
- `test_path_resolution` - Config-relative path resolution

### 11.3 Running Tests

**Prerequisites:**
```bash
pip install pytest
```

**Run All Tests:**
```bash
cd /path/to/luwak
python -m pytest test/
```

**Run Specific Test Module:**
```bash
python -m pytest test/test_anonymize.py
```

**Run Specific Test:**
```bash
python -m pytest test/test_anonymize.py::TestAnonymizeScript::test_uid_generation
```

**Run with Verbose Output:**
```bash
python -m pytest test/ -v
```

### 11.4 Test Data

Test data is automatically downloaded from a private GitHub repository during test setup:
- **Dataset:** A collection of DICOM data from the NCI Medical Image De-identification Benchmark (Midi-B) Challenge. These data were selected for containing a subset of private tags used for testing.
- **Private Tags:** Includes vendor-specific tags for private tag handling tests
- **Format:** Compressed tarball extracted to `test_data/` directory

**Reference:**  
NCI Medical Image De-identification Benchmark Challenge (Midi-B):  
https://www.cancer.gov/about-nci/organization/cbiit/news-events/news/2024/participate-nci-medical-image-de-identification-benchmark-challenge-miccai-2024

**Note:** Test data download requires a GitHub token set in the `TEST_DATA_TOKEN` environment variable.

### 11.5 Continuous Integration

Tests are designed to be run in CI/CD pipelines with:
- Automatic test data download and extraction
- Isolated temporary directories for each test
- Cleanup of test artifacts after execution

---

## 12. References

### 12.1 DICOM Standards
- **PS3.3:** Information Object Definitions  
  https://dicom.nema.org/medical/dicom/current/output/chtml/part03/
- **PS3.15 Appendix E:** Attribute Confidentiality Profiles  
  https://dicom.nema.org/medical/dicom/current/output/chtml/part15/chapter_E.html
- **PS3.16 CID 7050:** De-identification Method Codes  
  https://dicom.nema.org/medical/dicom/current/output/chtml/part16/sect_CID_7050.html

### 12.2 Community Resources
- **TCIA Submission Guidelines:**  
  https://wiki.cancerimagingarchive.net/display/Public/Submission+and+De-identification+Overview
- **TCIA Private Tag Knowledge Base:**  
  https://wiki.cancerimagingarchive.net/display/Public/TCIA+Private+Tag+Knowledge+Base
- **Laurel Bridge Anonymization Conformance:**  
  https://laurelbridge.com/pdf/Dicom-Anonymization-Conformance-Statement.pdf
- **NCI Medical Image De-identification Benchmark Challenge (Midi-B):**  
  https://www.cancer.gov/about-nci/organization/cbiit/news-events/news/2024/participate-nci-medical-image-de-identification-benchmark-challenge-miccai-2024


### 12.3 Software Libraries
- **deid - DICOM Deidentification Library:**  
  GitHub: https://github.com/pydicom/deid  
  Documentation: https://pydicom.github.io/deid/  
  Citation: Sochat, V. (2022). deid: Best-effort anonymization for medical images in Python (Version 0.3.22) [Computer software]. https://doi.org/10.5281/zenodo.7436347
- **KitwareMedical/dicom-anonymizer:**  
  GitHub: https://github.com/KitwareMedical/dicom-anonymizer
---

## 13. Document Linking for Code References

### 13.1 GitHub Anchor Links
To reference specific sections from code comments or documentation:

**Format:**
```
https://github.com/ZentaLabs/luwak/blob/uid-lookup-db/docs/deidentification_conformance.md#section-anchor
```

**Examples:**
- Recipe Creation: `#3-deidentification-recipe-creation`
- UID Generation: `#421-uid-generation-generate_hmacuid`
- Tag Templates: `#2-tag-template-files`
- CID 7050 Mapping: `#32-deidentification-profiles`

### 13.2 Usage in Code
Add links in code comments:

```python
# For details on UID anonymization methodology, see:
# https://github.com/ZentaLabs/luwak/blob/uid-lookup-db/docs/deidentification_conformance.md#421-uid-generation-generate_hmacuid
def generate_hmacuid(self, item, value, field, dicom):
    ...
```

---

## Appendix A: Configuration Schema Reference

See `luwakx/data/README.md` for complete JSON schema documentation of `luwak-config.json`.

## Appendix B: Tag Template Maintenance

Tag templates should be regenerated when:
- DICOM standard is updated (annually)
- TCIA Knowledge Base is updated
- New vendor-specific private tags are discovered

Use `retrieve_tags.py` script as documented in [§5](#5-metadata-deideintification----tags-and-profiles-templates) .

