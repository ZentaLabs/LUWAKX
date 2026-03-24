# DICOM Deidentification Conformance Statement

**Luwak DICOM Deidentification System**  
Version: 1.1  
Date: March 6, 2026  
Based on: DICOM Standard 2025b

---

## 1. Introduction

This document describes the deidentification process implemented in the Luwak project. It provides comprehensive details on recipe creation, tag template generation, deidentification profiles, and the complete deidentification workflow.

---

## 2. Document Scope

### 2.1 Scope
This conformance statement applies to the deidentification features provided by the Luwak DICOM processing pipeline, including:
- Standard and private DICOM tag anonymization
- Recipe generation from configurable templates

**Core Deidentification Engine:** Luwak uses the [deid](https://github.com/pydicom/deid) library (maintained in the [pydicom](https://github.com/pydicom) organization in GitHub) for DICOM metadata deidentification. The deid library provides the recipe processing engine that applies tag-level transformations according to configurable rules. Luwak extends deid with custom anonymization functions for HMAC-based UID generation, date shifting, LLM-supported descriptor cleaning, and image-defacing capabilities. It additionally provides support for a list of deidentification profiles from the [DICOM Standard 2025b PS3.15 Appendix E, Table E.1-1](https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E).


### 2.2 Audience
This document is intended for:
- Clinical researchers implementing DICOM anonymization
- Data protection officers ensuring HIPAA/GDPR compliance
- Software developers integrating with the Luwak pipeline
- Quality assurance teams validating anonymization procedures

---

## 3. Supported deidentification Profiles/Options

### 3.1 Overview

Luwak employs a standards-based approach to DICOM image de-identification to ensure that images are free of protected health information (PHI), following the HIPAA Safe Harbor Method as defined in section 164.514(b)(2) of the HIPAA Privacy Rule. Compliance is achieved through a user-configurable combination of DICOM PS3.15 Appendix E profiles and options. To satisfy the HIPAA Safe Harbor de-identification standard, users should select the **Basic Application Confidentiality Profile** together with the following profile options: **Clean Pixel Data Option**, **Clean Descriptors Option**, **Retain Longitudinal With Modified Dates Option**, **Retain Patient Characteristics Option**, and **Retain Safe Private Option**.

Luwak allows the user to select profiles and options through a list of "recipes" specified in a config file (see [§9.1](#91-configuration-file) for details on how to use the config file).
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
| Clean Recognizable Visual Features | `clean_recognizable_visual_features` | 113101 | Applies defacing to imaging pixel data |

**Note on Unsupported Profiles:**

Luwak currently does not provide automated support for the following DICOM PS3.15 Appendix E profiles:

- *Clean Structured Content Option (CID 7050 code 113104):* Tags requiring structured content cleaning are flagged with `clean_manually` actions in the recipe for manual review. Automated PHI detection in complex nested structures is not currently implemented.

- *Clean Graphics Option (CID 7050 code 113103):* Tags with graphic annotations are flagged with `clean_manually` actions in the recipe for manual review. Users must manually inspect and clean text annotations overlaid on images.

These profiles require manual intervention to ensure PHI is properly removed from structured content sequences and graphic annotations.

Before running the deidentification pipeline, it is recommended to assess whether the input dataset contains tags relevant to these profiles. The `luwakx/scripts/dicom_curation/analyze_graphics_structured_content.py` curation script can detect the presence of such tags across the entire dataset. It checks for, among others, `GraphicAnnotationSequence` (0070,0001), `OverlayData` (6000,3000), `ContentSequence` (0040,A730), and `AcquisitionContextSequence` (0040,0555) - the main tags that fall under the Clean Graphics and Clean Structured Content profiles. Series that contain these tags are reported in the analysis output, allowing the user to decide whether to exclude them from the deidentification project or perform manual cleaning before running Luwak. For full usage instructions, see `luwakx/scripts/dicom_curation/DICOM_PROCESSING_WORKFLOW.md`.

**Note on Clean Pixel Data Option:**

- *Clean Pixel Data Option:* Luwak includes detection rules for identifying burned-in pixel annotations that cannot be removed through header anonymization. The pixel cleaning functionality using the deid library is currently in active development (https://github.com/ZentaLabs/luwak/issues/47) and will be integrated into the automated pipeline in a future release. For now, users should manually review images (see [§4.2](#42-clean-pixel-data-option)).

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
- `clean_descriptors_with_llm` - LLM-based PHI detection in free-text/annotation tags
- `set_fixed_datetime` - Fixed epoch datetime replacement
- `check_patient_age` - Patient age retention with HIPAA-compliant capping (>89Y -> 090Y)
- `sq_keep_original_with_review` - Sequence retention with structured review flag emission
- `is_tag_private` - Private tag identification for removal
- `is_curve_or_overlay_tag` - Curve/overlay data identification for removal

These custom functions are injected into DEID's item processing dictionary and called during recipe execution when specified in recipe files.

In the following sections we explain the deidentification pipeline in detail:

1. **Image Pixel Data Deidentification ([§4](#4-image-pixel-data-deidentification))** - Describes facial feature removal using MOOSE framework ([§4.1](#41-clean-recognizable-visual-features-defacing----pipeline-stage-3)) and burned-in pixel annotation detection ([§4.2](#42-clean-pixel-data-option)).

2. **Metadata Deidentification ([§5](#5-metadata-deideintification----tags-and-profiles-templates))** - Details the tag template generation ([§5.1](#51-standard-tags-template), [§5.2](#52-private-tags-template)), deidentification actions ([§5.3](#53-tagprofile-specific-actions)), and profile implementation ([§5.4](#54-profileoptions-description)).

3. **Recipe Creation ([§6](#6-deidentification-recipe-creation-pipeline-stage-4---5))** - Explains how CSV templates are converted into DEID recipes, including dummy value generation ([§6.3](#63-dummy-value-replacement-rules)) and action translation logic ([§6.4](#64-recipe-builder-action-to-recipe-translation)).

4. **DeidentificationMethodCodeSequence Injection ([§7](#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-6))** - Documents how applied deidentification methods are recorded in DICOM standard format.

5. **Data and Metadata Export ([§8](#8-deidentified-data-and-metadata-export-pipeline-stage-7))** - Describes all output files generated by Luwak, including deidentified DICOM files, UID mappings, metadata exports, and audit logs.

6. **Configuration and Code Architecture ([§9](#9-configuration-code-design-and-usage))** - Provides comprehensive guide to configuration options ([§9.1](#91-configuration-file)), system architecture ([§9.2](#92-code-architecture-and-design)), and usage instructions ([§9.3](#93-running-luwak)).

7. **Limitations, Testing, and References ([§10](#10-limitations-and-constraints)-[§12](#12-references))** - Covers known limitations, validation procedures, and external references.

## 4. Image Pixel Data Deidentification 

This section describes Luwak's implementation of image pixel data deidentification, specifically addressing the Clean Recognizable Visual Features profile and the Clean Pixel Data Option.

### 4.1 Clean Recognizable Visual Features (Defacing -- pipeline stage 3)

#### 4.1.1 Overview
Luwak implements automated face detection and pixelation for medical imaging volumes (CT, PET) to remove identifiable facial features from pixel data, complying with DICOM CID 7050 code 113101. 
This is achieved by generating a segmentation of the face, with subsequent downsampling followed by upsampling to the original resolution of the image within the segmented region to voxelize the facial features. The process was implemented akin to Selfridge et al [10.2967/jnumed.122.265280]. 
Currently only CT is supported, but we are soon extending this to PET and we plan in the future to add also MRI (https://github.com/ZentaLabs/luwak/issues/31).

**Implementation Module:** `luwakx/deface_service.py`  
**ML Defacing Module:** `luwakx/scripts/defacing/image_defacer/image_anonymization.py`

**Defacing Model Reference:** The defacing functionality in Luwak leverages the MOOSE framework and its pre-trained AI models for medical image segmentation and facial feature detection. MOOSE (https://github.com/ENHANCE-PET/MOOSE) provides robust deep learning models specifically designed for medical imaging tasks, enabling accurate and automated identification of facial regions in CT and PET scans. By integrating MOOSE, Luwak ensures performance and reliability in the defacing process.

#### 4.1.2 Defacing Pipeline

The defacing pipeline processes DICOM series through four main stages while maintaining complete spatial fidelity throughout. Luwak's implementation ensures that no reorientation or geometric transformation occurs at any stage, preserving all spatial metadata from the original DICOM files.

**Step 1: Volume Reconstruction**
- Loads DICOM series as a 3D volume using `SimpleITK.ImageSeriesReader`
- Uses GDCM's `GetGDCMSeriesFileNames()` to properly sort files by Image Position Patient (0020,0032)
- Preserves spatial metadata: Origin, Direction (from Image Orientation Patient 0020,0037), and Spacing
- Creates in-memory SimpleITK.Image object with native patient coordinate system intact
- No reorientation is performed; volume retains original acquisition plane and orientation

**Step 2: Face Detection/Segmentation**
- Passes SimpleITK.Image object to MOOSE framework
- MOOSE performs face segmentation in the original patient coordinate system
- Uses CT-optimized deep learning models (clin_ct_face) for facial feature detection
- Generates binary segmentation mask that inherits spatial metadata (Origin, Direction, Spacing) from input volume
- GPU-accelerated with automatic memory cleanup after inference

**Step 3: Pixelation**
- Applies pixelation algorithm to face-segmented regions using nearest-neighbor downsampling/upsampling
- Uses configurable physical block size (default 8.5mm, configurable via `physicalFacePixelationSizeMm` in config)
- Block size determines the resolution of pixelation: larger values provide stronger anonymization
- All resampling operations use identity transform with explicit preservation of spatial metadata:
  - `outputOrigin=image.GetOrigin()` (preserves Image Position Patient)
  - `outputDirection=image.GetDirection()` (preserves Image Orientation Patient)
  - `outputSpacing=image.GetSpacing()` (maintains voxel dimensions)
- Preserves diagnostic image quality and spatial accuracy in non-face regions

**Step 4: DICOM Export**
- Extracts defaced pixel data from 3D volume, stored in the SimpleITK.Image object, as 2D slices
- Each slice is located in the 3D volume using its spatial metadata (ImagePositionPatient, ImageOrientationPatient, PixelSpacing) rather than relying on slice order, ensuring robustness for non-axis-aligned volumes
  - For axis-aligned volumes, the nearest slice along the primary axis is selected (`_is_volume_axis_aligned()`)
  - For arbitrarily oriented volumes, the correct slice is identified via spatial coordinates (`_extract_slice_from_volume()`)
- Reads original DICOM files to preserve all header metadata
- Applies inverse rescale transformation: `raw_pixels = (defaced_pixels - RescaleIntercept) / RescaleSlope`
- Replaces only PixelData attribute; all spatial tags remain unchanged
- Adds CID 7050 code 113101 to DeidentificationMethodCodeSequence (0012,0064) to document defacing method

**Implementation:** `DefaceService._is_volume_axis_aligned()`, `DefaceService._extract_slice_from_volume()`

#### 4.1.3 Modality Support
- **CT (Computed Tomography):** Fully supported with modality-specific AI models.
- **PET (Positron Emission Tomography):** Supported automatically when a co-registered CT is available in the same study and `FrameOfReferenceUID`:
  - **PET/CT (co-registered CT available):** When the `clean_recognizable_visual_features` recipe is active, Luwak automatically detects PET series that share a `FrameOfReferenceUID` with a CT series in the same study. The AI face segmentation model runs on the CT only; the resulting mask is resampled onto the PET geometry and applied directly - no ML inference on the PET data. No extra configuration is required. See [§4.1.8](#418-petct-defacing-via-ct-mask-projection) for full implementation details.
  - **Standalone PET (no co-registered CT):** Support via a dedicated PET face segmentation model is planned for a future release (https://github.com/ZentaLabs/luwak/issues/31).
- **MR (Magnetic Resonance):** Planned for future implementation.
- **Other modalities:** Not currently supported for defacing.

#### 4.1.4 Configuration

**Enable Defacing:**
To add the defacing option to the deidentification pipeline the correct recipe must be added in the recipes option in the config file ([§9.1](#91-configuration-file)).

```json
{
  "recipes": ["clean_recognizable_visual_features"]
}
```

**Configure Pixelation Block Size:**
The physical block size for face pixelation can be customized (default: 8.5mm):

```json
{
  "physicalFacePixelationSizeMm": 8.5
}
```

This parameter controls the resolution of pixelation blocks. Larger values provide stronger anonymization but may affect diagnostic image quality. The default value of 8.5mm works well across different resolutions.

#### 4.1.5 Conditional Processing
Defacing is only performed when:
1. `clean_recognizable_visual_features` profile is selected in recipes
2. Modality is CT **or** the series is a PET series whose `primary_ct_series` has been set by `DefacePriorityElector` (i.e., a co-registered CT exists in the same study and `FrameOfReferenceUID`)

**Decision Logic:** `ProcessingPipeline._needs_defacing()`

**Behavior when defacing is not needed:**
- If defacing is not needed (modality is neither CT nor paired PET, or profile not selected), the defacing stage is skipped entirely
- Files remain in organized directory and are read directly for metadata deidentification
- No files are copied to defaced directory

**Behavior when defacing fails:**
- If defacing is attempted but fails (e.g., AI model error, file read error)
- Original organized files are copied to defaced directory without modification via `_copy_without_defacing()`
- Error messages are issued in the log. 
- Processing continues with undefaced files.
- CID 7050 code 113101 is NOT added to DeidentificationMethodCodeSequence Attribute

#### 4.1.6 DeidentificationMethodCodeSequence Integration
- CID 7050 code 113101 is conditionally added to DeidentificationMethodCodeSequence
- Only included if defacing was successfully performed
- Absent if defacing was skipped or failed

#### 4.1.7 Performance Considerations
- GPU acceleration recommended for AI face detection.
- Memory cleanup after each series to prevent GPU OOM.
- Typical processing time with GPU: 30-90 seconds per series (Linux, Ubuntu), 3 minutes (MacOS ARM).

#### 4.1.8 PET/CT Defacing via CT Mask Projection

For PET/CT studies, Luwak implements PET defacing by projecting the CT-derived face mask onto the PET geometry rather than running a separate ML model on the PET data. This approach exploits the spatial co-registration intrinsic to PET/CT acquisitions: the CT face segmentation model runs once, its output mask is stored, and that mask is resampled onto any PET series sharing the same `FrameOfReferenceUID` within the same study. This avoids redundant ML inference and ensures anatomically consistent face pixelation across both the CT and PET volumes.

**Implementation Modules:** `luwakx/deface_mask_database.py`, `luwakx/deface_priority_elector.py`

##### 4.1.8.1 CT Primary Series Selection

Before processing begins, `DefacePriorityElector.elect_and_sort()` identifies, for each PET series, a *primary* CT series within the same `(patient, study, FrameOfReferenceUID)` group that will serve as the source of the face mask. This pairing runs automatically whenever the `clean_recognizable_visual_features` recipe is active - no extra configuration is required.

For each PET series, `DefacePriorityElector` selects the CT series whose `AcquisitionDateTime` (0008,002A) is closest to that individual PET series' `AcquisitionDateTime`. This per-PET proximity criterion ensures the most temporally - and therefore geometrically - aligned CT scan is chosen for the mask resampling step. `AcquisitionDateTime` is read from tag (0008,002A), falling back to `AcquisitionDate` (0008,0022) + `AcquisitionTime` (0008,0032) when the combined attribute is absent.

The selected primary CT series is placed first in the processing order so that its face mask is computed before any associated PET series are processed. The pairing is recorded in the `deface_series_pairing` table of `DefaceMaskDatabase`; the `mask_path` column is filled in once the CT mask has been computed.

##### 4.1.8.2 Mask Database

`DefaceMaskDatabase` is a thread-safe SQLite database that stores the segmentation mask (as an NRRD file path) computed for each primary series. The database key is a SHA-256 hash of:

```
project_hash_root || PatientID || PatientName || PatientBirthDate || FrameOfReferenceUID
```

The study UID is included in the key so masks are scoped to a single study and are not shared across studies.

**Persistence:** The mask database follows the same rules as `analysisCacheFolder`:
- If `analysisCacheFolder` is configured, `deface_mask.db` persists across runs.
- Otherwise, it is created in the private mapping folder and deleted after processing.

##### 4.1.8.3 PET Series Defacing via Projected CT Mask

For each PET series paired with a primary CT series in the same group:
1. The CT face mask whose `AcquisitionDateTime` (0008,002A) is closest to the PET series acquisition date/time is retrieved from the mask database (relevant when multiple CT masks exist for the same `FrameOfReferenceUID`).
2. The selected CT mask is resampled onto the PET series geometry using the PET spatial metadata (origin, direction, spacing).
3. The pixelation step is applied to the PET volume using the resampled mask.

##### 4.1.8.4 Configuration

PET/CT mask projection requires no extra configuration: it runs automatically whenever the `clean_recognizable_visual_features` recipe is active and a CT series shares the same `(patient, study, FrameOfReferenceUID)` with a PET series.

The optional `saveDefaceMasks` boolean (default: `false`) controls mask persistence beyond the current run:

```json
{
  "saveDefaceMasks": true
}
```

When `true`, every series that runs ML inference has its face mask saved to the private mapping folder and the database persists after the run, enabling full re-run cache hits for all modalities. When `false` (default), only the CT masks paired with a PET series are kept for the duration of the run (just long enough to project onto the PET). See [§9.1.2](#912-optional-configuration-options) for full option documentation.

#### 4.1.9 Output Artifacts
For each defaced series:
- `image.nrrd` - Original 3D volume (temporary, for validation)
- `image_defaced.nrrd` - Defaced 3D volume (temporary)
- `*.dcm` - Defaced DICOM files (final output)

Temporary NRRD files (`image.nrrd`, `image_defaced.nrrd`) are stored in `defaced_base_path` during processing and moved to the private mapping folder after the series completes.

**Face mask files (`deface_mask_<modality>.nrrd`):**

A compressed NRRD mask file is written to the private mapping folder - mirroring the series output path structure - and recorded in `DefaceMaskDatabase`. The mask file is saved when **either** of the following conditions is met:

| Condition | When it applies |
|-----------|-----------------|
| CT is paired with a PET (`series.is_primary_deface_candidate = True`) | Always, regardless of `saveDefaceMasks`; the mask must persist long enough to be projected onto the PET within the same run |
| `saveDefaceMasks: true` in config | Every series that runs ML inference gets its mask saved |

**File location:**
```
<outputPrivateMappingFolder>/<rel_series_path>/deface_mask_<modality>.nrrd
```
where `<rel_series_path>` mirrors the anonymized output directory structure (e.g. `<patientID>/<studyUID>/<seriesUID>/`).

**Examples:**
- `deface_mask_CT.nrrd` - saved for a CT that is primary candidate for a paired PET, or when `saveDefaceMasks: true`
- `deface_mask_PT.nrrd` - saved only when `saveDefaceMasks: true` (PET mask derived from CT projection)

When `saveDefaceMasks: false` (default) and no PET pairing is detected, no mask file is written.

#### 4.1.10 Current limitations
- Defacing via ML is supported only for CT modality. PET defacing is supported via CT mask projection when a co-registered CT is available in the same study (see [§4.1.8](#418-petct-defacing-via-ct-mask-projection)); standalone PET and MR are not yet supported (planned for PET https://github.com/ZentaLabs/luwak/issues/31).
- When the MOOSE model returns no segmentation (e.g., the volume does not contain a head/face region), the series is copied without defacing and flagged for manual review. This can occur for chest CTs or other non-head volumes.
- The time/resource consuming AI model will run even when no face is included in the data, because we can't rely on BodyPartExamined correct labeling. For the future we plan to develop a functionality to determine from a coronal 2D slice whether the head is present and skip this step accordingly.
- No modification on non-face data has been observed from the defacing model so far, but care must be taken to check the defacing result after each deidentification project.
- The defacer does not modify the ears region; there is ongoing discussion to include this part of the face in the future (https://github.com/ZentaLabs/luwak/issues/71).

### 4.2 Clean Pixel Data Option

#### 4.2.1 Overview

Luwak includes DEID-based detection rules for burned-in pixel annotations that cannot be removed through header deidentification or defacing. This part is still under actibve developement (https://github.com/ZentaLabs/luwak/issues/47) and will be included in future release.

**Implementation Module:** `luwakx/pixel_cleaner_service.py`  
**Recipe File:** `luwakx/data/BurnedPixelLocation/deid.dicom.burnedin-pixel-recipe`

**Pixel Cleaning Implementation:** Burned-in pixel cleaning is performed using the `deid` library's `DicomCleaner` class, which provides tools for detecting and masking sensitive pixel regions in DICOM images based on rule-based pattern matching. The process involves identifying known areas which typically include annotations (such as patient names or IDs) and applying pixel masking to remove them. For more details, see the [deid pixel cleaning documentation](https://pydicom.github.io/deid/getting-started/dicom-pixels/).

**Future Development:** There are recent plans to develop an automatic burned-in pixel detection system using machine learning techniques (OCR, image analysis) to identify and mask PHI in arbitrary locations without requiring pre-configured coordinates. This enhancement will complement the current rule-based approach and provide more comprehensive protection against burned-in annotations in unknown equipment configurations or custom text overlays.

**Detection Method:**

- **Rule-Based Pattern Matching:** The deid library uses header metadata matching against known patterns, NOT actual image analysis, OCR, or machine learning  
- **Pre-Configured Coordinates:** Rectangular regions where annotations are known to appear for specific manufacturer/model combinations are defined in the recipe  
- **Header-Based Triggers:** Detection is triggered by specific DICOM attributes such as:  
  - `BurnedInAnnotation` tag value  
  - `ImageType` containing "SAVE" keyword  
  - Specific manufacturer/model/series description combinations

**Cleaning Process:**

1. **Detection Phase:** `DicomCleaner.detect()` evaluates each DICOM file against the recipe rules  
2. **Flagging:** Files matching known patterns are flagged with specific coordinates  
3. **Masking Phase:** `DicomCleaner.clean()` replaces flagged pixel regions with black pixels (value 0\)  
4. **Preservation:** All DICOM metadata headers remain unchanged; only pixel data is modified

**Modified DICOM Attributes:**

- **Pixel Data (7FE0,0010)** at the top-level dataset only is modified  
- Masked regions are replaced with black pixels (value 0\)  
- All DICOM metadata is preserved unchanged  
- Pixel Data within private attributes is NOT processed (those attributes are removed by the basic\_profile as they are not known to be safe)

**Icon Image Sequence Handling:** The Icon Image Sequence (0088,0200) attribute is not modified or cleaned by the pixel cleaning process. Instead, this attribute is completely removed during deidentification as specified in the Basic Application Confidentiality Profile. This ensures that any potential PHI present in thumbnail or icon images is eliminated rather than attempting pixel-level cleaning.

**Private Tag Pixel Data:** Luwak does not perform pixel cleaning on private tag Pixel Data attributes. All private tags, including those containing pixel data, are removed by default unless they are explicitly listed in the DICOM PS3.15 Appendix E.3.10 Safe Private Attributes list (see [§5.2](#52-private-tags-template)) and the Retain Safe Private Option profile ([§5.4.11](#5411-retain-safe-private-option)) is selected by the user. This approach ensures that potentially sensitive pixel data stored in vendor-specific private tags is not inadvertently retained.

#### 4.2.2 Detection Rules

- **Whitelist filters:** Known clean image types/modalities that are safe to process  
- **Graylist filters:** Specific pixel regions with common annotations where coordinates are defined for cleaning  
- **Blacklist filters:** High-risk patterns that flag images for manual review or rejection (e.g., SECONDARY/SAVE images, missing ImageType, BurnedInAnnotation=YES)  
- **Equipment-specific rules:** Manufacturer-specific patterns with pre-configured coordinate regions

#### 4.2.3 Common Burned-In Locations

- Dose reports in CT (coordinates 0,0,512,121)  
- Localizer images  
- Enhancement curves  
- Reconstruction metadata overlays

#### 4.2.4 Integration in Luwak Pipeline

**Pipeline Stage:** Pixel cleaning occurs at stage 2, after organization and BEFORE defacing.

**Conditional Processing:** Pixel cleaning is performed when `'clean_pixel_data'` is included in the `recipes` array in the config (see [§9.1](#91-configuration-file) for configuration details). The decision logic is implemented in `ProcessingPipeline._needs_pixel_cleaning()`.

**Processing Flow:**

1. **Service Initialization:** `PixelCleanerService` is instantiated with the burned-in pixel recipe path  
2. **Series Processing:** For each series requiring pixel cleaning:  
   - Files are read from the `organized` directory  
   - `DicomCleaner.detect()` is called for each file to identify burned-in annotations  
   - Files flagged with coordinates are cleaned using `DicomCleaner.clean()`  
   - Cleaned files are saved to the `pixel_cleaned` directory  
   - Files without flags or coordinates are copied as-is  
3. **Path Updates:** `DicomFile.set_pixel_cleaned_path()` updates each file's path for downstream processing  
4. **Status Tracking:** Series status is updated to `ProcessingStatus.PIXEL_CLEANED`  
5. **Result Reporting:** Results include counts of cleaned, flagged, and skipped files

**Behavior on Detection:**

- **Files with coordinates (graylist matches):** Pixel regions are masked with black pixels (value 0\)  
- **Files flagged without coordinates (blacklist matches):** Copied as-is with warning logged for manual review \- deidentification continues normally  
- **Files not flagged (whitelist matches):** Copied as-is without modification with this info logged  
- **Processing errors:** Files are copied as-is on error, with error logged

**Configuration:**

{

  "recipes": \["clean\_pixel\_data"\]

}

**Implementation Reference:**

- `PixelCleanerService.process_series()` \- Main entry point for series cleaning  
- `PixelCleanerService._detect_with_cleaner()` \- Detection wrapper  
- `ProcessingPipeline._pixel_clean_series()` \- Pipeline integration point  
- `ProcessingPipeline._needs_pixel_cleaning()` \- Business logic decision

#### 4.2.5 Current Limitations

**Detection Limitations:**

- **Rule-Based Only:** Detection relies solely on header metadata matching, NOT actual image analysis or OCR  
- **Known Patterns Only:** Only detects annotations in pre-configured locations for equipment specified in the recipe database  
- **No Custom Text Detection:** Will miss custom text, annotations in unexpected locations, or unknown equipment configurations  
- **Manual Review Required:** Files flagged without coordinates require manual inspection to ensure complete PHI removal

**Cleaning Scope:**

- **Top-Level Pixel Data Only:** Only modifies Pixel Data (7FE0,0010) at the dataset's top level  
- **No Private Tag Cleaning:** Private tag Pixel Data attributes are not cleaned at the pixel level; these tags are removed by default (unless retained via Safe Private Option)  
- **Rectangular Regions Only:** Masking is limited to pre-configured rectangular coordinate regions  
- **No Dynamic Detection:** Cannot adapt to new annotation patterns without recipe updates

**Recipe Maintenance:**

- The burned-in pixel recipe (`deid.dicom.burnedin-pixel-recipe`) must be manually updated to include new manufacturer/model/coordinate patterns  
- Community contributions to the deid library's recipe database improve coverage over time  
- Organizations should validate the recipe against their specific imaging equipment

**Recommendations:**

- Enable pixel cleaning as a baseline protection by adding "clean\_pixel\_data" to the recipe list in the config file  
- Perform manual review of flagged files, especially those without cleaning coordinates  
- Validate cleaned images visually for residual burned-in annotations  
- Report new annotation patterns to the deid library community for recipe inclusion

**Future Enhancements:**

- **Automatic Detection System (In Development):** Machine learning-based approach using OCR and image analysis to detect burned-in text in arbitrary locations without requiring pre-configured coordinates. This will address current limitations by enabling dynamic detection of:  
  - Custom text annotations in unexpected locations  
  - PHI from unknown equipment configurations  
- This enhancement will complement the existing rule-based method and significantly improve detection coverage

Once the pixel data have been cleaned of all possible identification risks, the pipeline proceeds to metadata deidentification.

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
Luwak supports nested DICOM sequences in the standard tags template only using double-underscore (`__`) notation:

**Example:**
```
Group: 0018__0__0008
Element: 9346__0__0104
```
This represents: `(0018,9346)[0](0008,0104)` - the Referenced SOP Instance UID within the first item of the Referenced Series Sequence.

**Note:** The nested sequence syntax is only available for standard tags (`standard_tags_template.csv`). Private tags (`private_tags_template.csv`) use the standard `xx` placeholder notation and do not support nested sequence paths currently.

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

#### 5.2.5 TCIA Data Processing

When processing TCIA Private Tag Knowledge Base data, the template generation script applies the following transformations:

**Duplicate Tag Handling:**
- If the same tag (matching Group, Element, and Private Creator) appears multiple times with different VRs in the TCIA source, only the first occurrence is retained
- Implementation: `drop_duplicates(subset=['Group', 'Element', 'Private Creator'])` in `retrieve_tags.py`.

**Nested Structure Extraction:**
- When TCIA lists a private tag with nested sequence structure (e.g., `(0008,1110)[<0>](2001,"Philips Imaging DD 001",c1)` or `(0008,1115)[<0>](0008,1140)[<1>](0009,"GEIIS_RA1000",01)`), the script extracts only the final child tag as `(2001,xxc1)` or `(0009,xx01)` respectively, and stores the private creator string separately in the Private Creator column
- The full nested path information from TCIA is preserved in the `TCIA element_sig_pattern` column for reference (e.g., `(0008,1110)[<0>](2001,xxc1)`)
- This simplifies the template structure while preserving the essential tag identification information
- Implementation: `transform_row()` function in `retrieve_tags.py`.

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

Luwak supports custom deidentification functions that extend pydicom/deid's recipe syntax. These functions are injected into the recipe as action arguments (e.g., `REPLACE (0020,000d) func:generate_hmacuid`) and executed by the pipeline for tags requiring advanced deidentification ([§6](#6-deidentification-recipe-creation-pipeline-stage-4---5)). The main custom functions are: `func:generate_hmacuid`, `func:generate_hmacdate_shift`, `func:clean_descriptors_with_llm`, `func:generate_patient_id`.

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

**Supported VR types:** The function applies UID replacement to tags with VR `UI` (Unique Identifier) and also to VR `LO` (Long String), since some private tags store UID-like values as `LO`. Tags with any other VR are subject to the VR-mismatch handling described below.

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
- **VR incompatibility handling:** When `func:generate_hmacuid` is applied to a tag whose VR is neither `UI` nor `LO`, Luwak first attempts to delete the tag entirely. If deletion succeeds the tag is silently removed from the output file. If deletion fails (e.g., the tag is nested inside a sequence and pydicom cannot remove it directly), a warning is logged once per tag per series, a `VR_MISMATCH_OPERATION` review flag is emitted, and the original value is written back unchanged. For binary VR types (`OB`, `OW`, `UN`) the review CSV records `<binary VR_TYPE data>` instead of serialising raw bytes.

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

**Purpose:** Remove PHI/PII from textual descriptors using open-source on-premise large language models. This module is intended to automatically detect PHI/PII in free-text, annotation fields, patient characteristics and device information, that are often manually edited by technicians and operators. **Example:** Study/Series Description with free-text "CT scan for John Doe".
**Note:** 
This module is intended to be used with open-source LLMs (e.g., gpt-oss, llama3, qwen) that are running locally on-premise so that sensitive DICOM tag data is not sent to external cloud infrastructure.

While Luwak theoretically allows to use an OpenAI API key for proprietary OpenAI LLMs (e.g. GPT-5), we do not recommend to do this. The OpenAI API key config option is intended to be used for benchmarking local LLMs against proprietary LLMs using synthetically infused DICOM data (e.g., from the MIDI-B challenge).

**Method:**
- Sends descriptor text to LLM for PHI/PII detection
- Uses a binary classifier that returns 0 (no PHI/PII) or 1 (PHI/PII detected)
- Caches results in shared SQLite database to avoid redundant LLM calls
- If PHI detected (result = 1): attempts to delete the tag entirely from the DICOM dataset; if deletion fails, replaces with "ANONYMIZED"
- If no PHI detected (result = 0): keeps the original text value unchanged and logs a warning requesting manual verification.
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
- Open-source, local models: Every LMStudio-compatible model: https://lmstudio.ai/models
- Proprietary, external models: Every model from OpenAI Platform: https://platform.openai.com/docs/models

**Cache Management:**
- Cache stored in: `{analysisCacheFolder}/llm_cache.db` (SQLite)
- Thread-safe for parallel processing with shared access across workers
- If `analysisCacheFolder` is specified: cache persists across anonymization runs
- If not specified: temporary cache created in private mapping folder and deleted after processing

**Validation and Benchmarks:**

A benchmark on 21,793 free-text/annotation DICOM tags (No PHI/PII: 20,383; PHI/PII: 1,410) from the MIDI-B Challenge Test Dataset revealed 97% sensitivity, 98% specificity, positive predictive value 80%, negative predictive value 100%, balanced accuracy 98%, and F2-score 93% in detecting PHI/PII. For more details, visit: https://github.com/ZentaLabs/luwak/tree/main/luwakx/scripts/detector 

**Current limitations:**
- This profile requires a local LLM compatible with LMStudio. This implies the usage of important local resources. When run for first time and for gpt-oss-20b, the used system allows deidentification at speed, e.g.: ~5s/tag when using a GPU with 8GB VRAM on Linux.
- The LLM provides a binary output for either keeping or removing the tag, no other action is currently supported.
- The LLM can have false negatives, so a final review of the content of the leftover tags is always advised.

**Bypass Mode (`bypassCleanDescriptorsLlm`):**

When the `bypassCleanDescriptorsLlm` configuration option is set to `true`, the LLM call is skipped entirely. The result is treated as `0` (no PHI detected) and the tag value is always kept unchanged. This is useful for pipeline testing, when no LLM infrastructure is available, or when the user prefers to perform manual review downstream via `review_flags.csv`.

**Configuration:**
```json
{
  "bypassCleanDescriptorsLlm": true
}
```

#### 5.3.8 Fixed DateTime `func:set_fixed_datetime`

**Purpose:** Set date/time tags to fixed epoch values. Used to remove temporal information while maintaining DICOM compliance.
**Example:** Dates in `basic_profile`.

**Method:**
- Replaces date/time values based on the tag's Value Representation (VR):
  - DA (Date): Returns "19000101" (January 1, year 1900)
  - DT (DateTime): Returns "19000101000000.000000+0000" (January 1, year 1900, 00:00:00 UTC)
  - TM (Time): Returns "000000.00" (00:00:00)
- Check: For unknown VR types, returns the original value with a warning

**Implementation:** `DicomProcessor.set_fixed_datetime()`

**Recipe Usage:**
```
REPLACE (0008,0012) func:set_fixed_datetime
```
**Current limitations**
- These dummy values will be assigned to all the patients, all series and all studies. This might create issues to 4D data loading and some DICOM viewer. This action is specified only for the Basic Profile, so if you don't want to have these issues, combine the profile with other options that keep/or shift the dates consistently (see [§6](#6-deidentification-recipe-creation-pipeline-stage-4---5) ).

#### 5.3.9 SQ Keep with Review Flag `func:sq_keep_original_with_review`

**Purpose:** Retain the value of a Sequence (VR=SQ) tag unchanged and emit a structured review flag so downstream reviewers can verify whether the sequence contains PHI. This action is generated by the recipe builder for SQ tags that require `replace` in the template but for which no automated replacement logic exists (i.e., the `Final CTP Script` column does not specify `@remove()` or `removed`).

**Method:**
- Returns the original sequence value to deid, leaving the tag intact in the anonymized file.
- Adds a `SQ_REPLACE_NEEDS_REVIEW` review flag to the `ReviewFlagCollector` buffer, capturing the tag path, original value, and series context.
- The review flag is written to `review_flags.csv` (see [§8.1](#81-output-files-generated-by-luwak)) for downstream audit.

**Implementation:** `DicomProcessor.sq_keep_original_with_review()`

**Recipe Usage:**
```
REPLACE (0040,A730) func:sq_keep_original_with_review
```

**Current limitations:**
- The original sequence value is kept unchanged; manual review via `review_flags.csv` is required to confirm that no PHI is present.

#### 5.3.10 Patient Age Handling `func:check_patient_age`

**Purpose:** Ensure patient age is handled according to profile requirements, with custom logic for age retention and deidentification.

**Method:**
- Replaces or keeps the value of patient age tag ((0010,1010) PatientAge) depending on its original value:
  - If the PatientAge value is missing (empty), the function returns an empty string.
  - If the format is non-standard, the function returns the original value and logs a warning for manual review (it does NOT remove or blank it automatically).
  - If the value is standard and valid, it is kept or capped to "090Y" if >"089Y" 

**Implementation:** `DicomProcessor.check_patient_age()`

**Recipe Usage:**
```
REPLACE (0010,1010) func:check_patient_age
```

**Current limitations:**
- Only standard age formats are supported; missing values are blanked, and non-standard values are kept with a warning for manual review.

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
  - Note: For `SQ` (sequences), the recipe builder checks TCIA's Final CTP Script and removes sequences if TCIA removes them (see [§6.3.1](#631-replace-action---dummy-value-generation))
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
- Maps to `func:clean_descriptors_with_llm` action (LLM-based PHI/PII cleaning)

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
- Maps to `func:check_patient_age` action for PatientAge tag

**DICOM Code 'C' (Clean):**
- Maps to `remove` or `func:clean_descriptors_with_llm` action (LLM-based PHI/PII cleaning) depending on specific tag and TCIA chosen action for that tag:
  - **(0010,0042) VR=UT (Sex Parameters for Clinical Use Category Comment)**: Not in TCIA list; maps to `func:clean_descriptors_with_llm` action.
  - **(0010,2110) VR=LO (Allergies)**: removed by TCIA; maps to `remove` action.
  - **(0038,0050) VR=LO (Special Needs)**: removed by TCIA; maps to `remove` action.
  - **(0038,0500) VR=LO (Patient State)**: removed by TCIA; maps to `remove` action.
  - **(0040,0012) VR=LO (Pre-Medication)**: kept by TCIA; maps to `func:clean_descriptors_with_llm` action.

  For these tags, we follow the TCIA line of action as specified in their CTP anonymizer script. For more details, see the [TCIA Submission and De-identification Overview, Table 1](https://wiki.cancerimagingarchive.net/display/Public/Submission+and+De-identification+Overview).

**Implementation:** `generate_retain_patient_characteristics_profile()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Retains patient age, sex, size, and weight that would otherwise be removed or replaced by Basic Profile. Applies LLM cleaning when required.

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
- If VR is any other type: Maps to `remove` (safe default for non-date/time fields, same action as TCIA)

**Implementation:** `generate_retain_long_modified_dates_profile()` in `luwakx/scripts/retrieve_tags.py`

**Effect:** Applies HMAC-based deterministic date shifting to all date fields while preserving time fields. All dates for the same patient are shifted by the same amount, maintaining relative temporal relationships for longitudinal analysis. Non-date/time fields with 'C' code are removed as they cannot be safely jittered.

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
- Private tags can have several VR for the same tag, hence it is possible that the action of `func:generate_hmacuid` and `func:generate_hmacdate_shift` could be prescribed to VRs which are not `UI`/`LO` or `DA`/`DT` respectively. This is handled by the deidentification process directly:
  - For `func:generate_hmacuid`: tags with VRs other than `UI` or `LO` are first attempted for deletion; if deletion succeeds the tag is silently removed; if deletion fails (tag is nested inside a sequence) a warning is logged once per tag/series and a `VR_MISMATCH_OPERATION` review flag is emitted (see [§6.4.1](#641-translation-logic-by-action) and [§8.1](#81-output-files-generated-by-luwak)).
  - For `func:generate_hmacdate_shift`: a single warning per series is issued for VRs that are not `DA` or `DT` (see [§6.4.1](#641-translation-logic-by-action)).
- TCIA provides a list of safe private tags specifying the VR, which can have multiple values, for each safe tag. No check is implemented in luwak to ascertain that the private tags listed as safe in the private_tags_template have a VR included in those listed by TCIA for that specific tag.

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

## 6. Deidentification Recipe Creation (pipeline stage 4 - 5)

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

**`func:check_patient_age` Action:**
```
REPLACE (tag) func:check_patient_age
```

**`func:sq_keep_original_with_review` Action:**
```
REPLACE (tag) func:sq_keep_original_with_review
```
Keeps the original Sequence value unchanged and emits a `SQ_REPLACE_NEEDS_REVIEW` review flag (see [§5.3.9](#539-sq-keep-with-review-flag-funcsq_keep_original_with_review))

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
- Note: Private tags may have different VRs for a single tag.
  - For `func:generate_hmacuid`: tags with VRs that are neither `UI` nor `LO` are first attempted for deletion. If deletion succeeds the tag is silently removed; if deletion fails (tag may be nested inside a sequence), a warning is logged once per tag per series and a `VR_MISMATCH_OPERATION` review flag is emitted. Check the logs and `review_flags.csv` for such occurrences.
  - For `func:generate_hmacdate_shift`: a single warning per series is issued if date shifting is attempted on VRs that are not `DA` or `DT`. Check the logs and verify the correct functioning for those tags.

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
This tag is removed from the data in case some pre-deidentification was already applied to the data. The tag is injected to the data again only after the entire deidentification process is complete ([§7](#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-6)).

### 6.5 Action Priority Rules

When multiple profiles are selected, actions are prioritized in the following order:

1. **`keep`** - Highest priority (retention options override removal)
2. **`func:generate_hmacdate_shift`** - Date shifting for longitudinal consistency
3. **`func:generate_hmacuid`** - UID anonymization
4. **`func:clean_descriptors_with_llm`** - LLM-based cleaning
5. **`replace`** - Generic replacement
6. **`func:set_fixed_datetime`** - Fixed datetime
7. **`func:check_patient_age`** - Keep/replace patient age
8. **`func:sq_keep_original_with_review`** - Keep sequence unchanged with review flag
9. **`blank`** - Blanking/emptying
10. **`remove`** - Removal (lowest priority)

#### 6.5.1

This action priority is based on the action priority in DEID, for which e.g., if a tag is specified with an action `KEEP`, i will always be kept even if somewhere in the recipe that same tag has the action of `REMOVE`.

Luwak has a testing suite that allows to verify that this logic is kept also when mixing different options and profiles together. 
Examples of these tests are:
- `test_keep_specific_private_tags_should_be_original_value`: Test that when specific private tags are marked to be retained, their original values are preserved in the anonymized output.
- `test_basic_retain_uid_should_have_original_uid` : Test that mixing basic profile and retain uid option keeps original UID for retained fields
- `test_basic_retain_date_should_have_original_date`: Test that mixing retain and date shift keeps original date for retain fields
- `test_basic_modified_date_should_have_modified_date`: Test that mixing basic profile and date shift modifies original date.
- `test_retain_patient_chars_recipe`: Test that mixing basic profile and retain patient characteristics profile keep/replace/clean patient characteristics.

### 6.6 Generated Recipe File Format

Output files: `deid.dicom.recipe`, `deid.dicom.recipe.csv`

The recipe builder generates two files:

- **`deid.dicom.recipe`** - A DEID-format recipe file containing all tag-level actions determined by the selected deidentification profiles and options. This is the input consumed by the deid library's `replace_identifiers()` function during metadata deidentification. A complete reference table showing all tags processed by the Basic Application Confidentiality Profile recipe is provided in [Appendix C](#appendix-c-basic-application-confidentiality-profile-recipe) (available [online](https://github.com/ZentaLabs/luwak/blob/main/docs/deidentification_conformance.md#appendix-c-basic-application-confidentiality-profile-recipe)).

- **`deid.dicom.recipe.csv`** - A human-readable summary CSV mirroring every directive in the recipe file. Each row records the tag address, tag name, action keyword (e.g., `KEEP`, `REMOVE`, `REPLACE`, `BLANK`, `JITTER`), replacement value (where applicable), and the rationale linking the decision back to the contributing deidentification profile and conformance documentation. The rationale is extracted from the `Documentation References` column of the tag template CSVs, using the label associated with the profile that drove the final action. When the source action is `func:clean_descriptors_with_llm` but the final action is derived as `remove` or `manual_review` (see [§6.4.1](#641-translation-logic-by-action)), the rationale is still attributed to the `clean_descriptors` profile. Additional directives (e.g., `ADD PatientIdentityRemoved`, `REMOVE ALL func:is_curve_or_overlay_tag`) are included as separate rows with a reference to [§6.4.2](#642-additional-recipe-directives).

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
  - Column Naming for UID fields:
    - **Standard tags**: Use the official DICOM keyword (e.g., `StudyInstanceUID`, `SeriesInstanceUID`)
    - **Private tags**: Constructed using the same convention as the `metadata.parquet` export - `{PrivateCreator}_{TagName}` if the tag name is known (e.g., `Siemens_CSA_Image_Header_Info`), or `{PrivateCreator}_{GGGG}xx{EE}` if the name is unknown (e.g., `PHILIPS_MR_IMAGING_0019xx10`). Spaces in the private creator string are replaced with underscores. Tags with no private creator block fall back to `str(tag)`.

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

**6b. Recipe Summary CSV (`deid.dicom.recipe.csv`)**
  - Location: Same folder as `deid.dicom.recipe` (recipes folder)
  - Content: Human-readable CSV summarising every directive in the recipe file. Columns: `Tag`, `Tag Name`, `Action`, `Replacement Value`, `Rationale`. The rationale links each directive back to the contributing deidentification profile and the relevant section of this conformance document.
  - Generation: Created alongside `deid.dicom.recipe` by `anonymization_recipe_builder.py`
  - Purpose: Audit trail and documentation of the applied deidentification decisions
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

**9. Review Flags CSV (`review_flags.csv`)**
  - Location: Private mapping folder (`outputPrivateMappingFolder`)
  - Content: CSV table listing all DICOM tags that could not be processed automatically and require manual review. Each row records the anonymized patient/study/series UIDs, tag coordinates (group, element), attribute name, VR, original value, whether the value was kept or removed, and a machine-readable reason code. Two user-fillable columns (`override_keep`, `override_value`) are reserved for annotators.
  - Generation: Appended incrementally after each series is processed by `MetadataExporter.append_series_review_flags()`
  - Reason codes:

    | Reason Code | Description |
    |-------------|-------------|
    | `VR_MISMATCH_OPERATION` | A recipe instruction (e.g. `func:generate_hmacuid`) was applied to a tag with an incompatible VR. For `func:generate_hmacuid`, Luwak first attempts to delete the tag; this flag is only emitted when deletion fails (e.g., the tag is nested inside a sequence) and the original value is preserved instead. |
    | `LLM_VERIFIED_CLEAN` | The LLM found no PHI; the original value was kept. Manual verification is still recommended. |
    | `VR_FORMAT_INVALID` | pydicom/deid detected that a stored value does not conform to its declared VR format; the value may or may not have been modified. |
    | `SQ_REPLACE_NEEDS_REVIEW` | A sequence tag (VR=SQ) was kept unchanged because no automated replacement logic is available; manual review is required. |
    | `PHI_REMOVAL_FAILED` | An attempt to delete or replace a PHI-containing tag failed (e.g., nested tag in a sequence); the tag may still contain PHI. |
    | `PATIENT_DB_UNAVAILABLE` | The patient UID database was unavailable during processing; UID/date anonymization may be incomplete. |
    | `SERIES_FAILED` | An unhandled exception occurred during series-level processing; the series output may be incomplete or unanonymized. |

  - Implementation: `luwakx/review_flag_collector.py`, `luwakx/metadata_exporter.py`

**10. Deface Mask Database (`deface_mask.db`)**
  - Location: As configured in the optional `analysisCacheFolder` (if not specified: private mapping folder, temporary); created whenever the `clean_recognizable_visual_features` recipe is active and PET/CT pairs are detected, or when `saveDefaceMasks` is `true`
  - Content: Two tables - `deface_mask_cache` (per-series CT face masks as NRRD file paths keyed by patient/study/FOR/modality/series UID) and `deface_series_pairing` (CT-PET pairing records with `mask_path` filled once the CT mask is computed)
  - Generation: Created and updated during the defacing stage by `DefaceMaskDatabase`
  - Persistence: Removed after processing unless `analysisCacheFolder` is specified
  - Thread-safe: Uses WAL mode with serialised writes for concurrent access
  - See [§4.1.8](#418-petct-defacing-via-ct-mask-projection) for details on PET/CT defacing logic

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

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `keepTempFiles` | boolean | false | If `true`, temporary directories created during processing (`temp_organized_input`, `temp_defaced_organized`) are retained after the workflow completes. Useful for step-by-step validation of the deidentification pipeline. |

**Deidentification Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `projectHashRoot` | string | "myproject2025" | Root hash for deterministic anonymization across project (required for HMAC-based anonymization) |
| `maxDateShiftDays` | integer | 1095 | Maximum days for date shifting (3 years default) |
| `patientIdPrefix` | string | "Patient" | Prefix for generated patient IDs (e.g., "Patient000001") |
| `physicalFacePixelationSizeMm` | number | 8.5 | Physical block size (in mm) for face pixelation during defacing. |
| `selectedModalities` | array | [] | List of DICOM modalities to include in processing. If empty or not set, all modalities are included. Example: `["MR", "CT"]` |

**Database and Cache Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `analysisCacheFolder` | string | none | Path to folder for persistent analysis databases (`patient_uid.db`, `llm_cache.db`, and `deface_mask.db`). If not specified, temporary databases are created in the private mapping folder and deleted after processing. If specified and folder exists with databases, they will be loaded and updated; if not, new databases will be created. Databases persist across anonymization runs to ensure consistent patient ID and UID mappings and to cache LLM results for performance. |

**LLM Descriptor Cleaning Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `cleanDescriptorsLlmBaseUrl` | string | "https://api.openai.com/v1" | Base URL for LLM API (OpenAI-compatible) |
| `cleanDescriptorsLlmModel` | string | "gpt-oss-20b" | Model name for LLM service |
| `cleanDescriptorsLlmApiKeyEnvVar` | string | "" | Environment variable name containing API key (empty by default) |
| `bypassCleanDescriptorsLlm` | boolean | false | If `true`, bypasses the LLM call in `func:clean_descriptors_with_llm`. Result is always treated as `0` (no PHI detected) and the tag value is kept unchanged. Useful for testing or when no LLM infrastructure is available. |

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

**PET/CT Defacing Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `saveDefaceMasks` | boolean | false | Controls face mask persistence across runs. When `true`, every series that runs ML inference has its mask saved to the private mapping folder and the database persists after the run, enabling full re-run cache hits. When `false` (default), only CT masks paired with a PET series are kept - just long enough to project onto the PET within the same run. PET/CT pairing itself is **automatic** whenever the `clean_recognizable_visual_features` recipe is active and requires no extra config. See [§4.1.8](#418-petct-defacing-via-ct-mask-projection). |
| `verifyDefacingIntegrity` | boolean | false | When `true`, after defacing each series the pipeline reads the written DICOM files back from disk and checks that no voxel outside the (dilated) face mask was modified. The face mask is dilated by the pixelation block size (`physicalFacePixelationSizeMm`) to tolerate block-boundary effects. A `WARNING` is logged for each series that fails the check; processing continues regardless. Intended for quality-assurance runs; leave `false` in production to avoid the extra I/O overhead. |

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
  - `_deface_series(series)` - Apply defacing
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
- **Purpose:** Defacing for CT/PET imaging volumes
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
- **Purpose:** Export UID mappings, metadata, NRRD volumes, and review flags
- **Key Responsibilities:**
  - Stream UID mappings to CSV
  - Stream metadata to Parquet
  - Move NRRD files to final destinations
  - Append review flag rows to `review_flags.csv`
- **Key Methods:**
  - `append_series_uid_mappings()` - Append UID mappings for one series
  - `append_series_metadata()` - Append metadata for one series
  - `extract_dicom_metadata()` - Extract metadata from anonymized file
  - `append_series_review_flags()` - Append review flag rows for one series

**`ReviewFlagCollector`**
- **Purpose:** In-memory accumulator of tags that could not be automatically anonymized and require manual review
- **Key Responsibilities:**
  - Buffer and deduplicate flagged-tag records during anonymization
  - Collapse per-instance rows to per-series rows when all instances share the same value
  - Expose structured rows for export via `flush_series()`
- **Reason Codes:** `VR_MISMATCH_OPERATION`, `LLM_VERIFIED_CLEAN`, `VR_FORMAT_INVALID`, `SQ_REPLACE_NEEDS_REVIEW`, `PHI_REMOVAL_FAILED`, `PATIENT_DB_UNAVAILABLE`, `SERIES_FAILED`
- **Key Methods:**
  - `set_series_context(patient_id, study_uid, series_uid)` - Set current series context
  - `add_flag(tag, reason, original_value, keep, value)` - Record a flagged tag
  - `flush_series()` - Return all buffered rows and clear the buffer

**`DefaceMaskDatabase`**
- **Purpose:** Thread-safe SQLite cache for storing and retrieving ML-generated face-segmentation masks per spatial reference frame
- **Key Responsibilities:**
  - Store primary defacing mask (NRRD path) per `(patient, study, FrameOfReferenceUID, modality)` group
  - Allow retrieval of cached masks for secondary series sharing the same frame
  - Support WAL-mode concurrent reads with serialised writes
- **Key Methods:**
  - `get_mask(key_hash)` - Retrieve cached mask entry
  - `store_mask(key_hash, nrrd_path, origin, spacing, direction)` - Store a new mask
  - `get_stats()` - Database statistics

**`DefacePriorityElector`**
- **Purpose:** Elect the *primary* CT series for each PET series in a (patient, study, FrameOfReferenceUID)` group and sort the full series list so the primary series are processed first
- **Key Responsibilities:**
  - Group series by spatial reference frame
  - Select the CT series whose `AcquisitionDateTime` (0008,002A) is closest to the PET acquisition date/time within the same `(patient, study, FrameOfReferenceUID)` group, prior to resampling and mask reapplication
  - Return the sorted list for efficient pipeline ordering
- **Key Methods:**
  - `elect_and_sort(all_series)` - Returns the sorted series list with primary series first per group

#### 9.2.3 Data Flow

```
1. Configuration Loading (LuwakAnonymizer)
   ↓
2. Directory Scanning (DicomSeriesFactory)
   -> Creates DicomSeries objects with DicomFile objects
   ↓
3. Recipe Generation (anonymization_recipe_builder)
   -> Generates deid.dicom.recipe
   ↓
4. Pipeline Coordination (PipelineCoordinator)
   -> Distributes series across workers
   -> Manages shared resources (UID DB, LLM cache, recipe)
   -> DefacePriorityElector: elect primary CT per (patient, study, FrameOfRef) group and pair with PET series (automatic when deface recipe is active); reorders series so each CT primary is processed before its paired PETs
   ↓
5. Pipeline Processing (ProcessingPipeline) - per worker
   -> For each DicomSeries:
      a. Organization Stage
         -> Copy files to organized temp structure
      b. Defacing Stage (optional)
         -> DefaceService: Volume reconstruction -> ML defacing (or mask reuse) -> DICOM export
         -> DefaceMaskDatabase: Cache/retrieve primary mask per spatial reference frame
      c. Anonymization Stage
         -> DicomProcessor: Apply recipe -> Custom functions -> Write files
         -> ReviewFlagCollector: Buffer tags requiring manual review
      d. Injection Stage
         -> Add DeidentificationMethodCodeSequence
      e. Export Stage
         -> MetadataExporter: Stream UID mappings, metadata, and review flags
   ↓
6. Result Aggregation (PipelineCoordinator)
   -> Finalize exports and verify files
   ↓
7. Cleanup
   -> Remove temp directories
   -> Close databases
   -> Delete temp UID/mask databases (if configured)
```

#### 9.2.4 Threading and Parallelization

- **Current Implementation:** Sequential series-by-series processing
- **Thread Safety:** 
  - `PatientUIDDatabase` uses write locks
  - `LLMResultCache` uses write locks
  - `DefaceMaskDatabase` uses write locks (WAL mode)
  - All three support concurrent read access
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
├── review_flags.csv
├── patient_uid.db (if persistent database configured)
├── deface_mask.db (if deface recipe active with PET/CT pairs, or saveDefaceMasks=true, and analysisCacheFolder not set)
└── {AnonymizedPatientID}/
    └── {HashedAnonymizedStudyUID}/
        └── {HashedAnonymizedSeriesUID}/
            └── image.nrrd (if defacing performed)

recipesFolder/
├── deid.dicom.recipe
└── deid.dicom.recipe.csv

analysisCacheFolder/ (if specified in config)
├── patient_uid.db
├── llm_cache.db (if descriptor cleaning used)
└── deface_mask.db (if deface recipe active with PET/CT pairs, or saveDefaceMasks=true)
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
2. **Large files:** Files >2GB may impact memory performance in parallel processing
3. **Directory naming collisions:** The directory structure uses 16-character truncated hashes of anonymized UIDs for study and series folders. While collision probability is negligible for typical institutional or national-scale datasets, theoretical collisions become possible at extremely large dataset scales (multi-country aggregations exceeding hundreds of millions of studies). The 96 bits of entropy provide strong collision resistance but are not sufficient to guarantee uniqueness at population scales approaching billions of studies. Users managing multi-institutional or international data aggregations should monitor for directory collisions and consider extending hash length if operating at scales exceeding 100 million unique studies.
4. **Manual verification requirement:** Considering the current limitations listed here and in each of the sections throughout this document, a complete deidentification workflow should include functionality to generate verification tables listing all files and tags for manual content review, and to display images for visual verification of correct defacing and/or burned-in pixel annotation removal. Such verification tools are not currently implemented in Luwak and must be performed using external DICOM viewers and analysis tools. 

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
- `test_defacer_profile.py` - Defacing with MOOSE integration
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
- `test_check_patient_age_method` - Patient age handling: capping values >89Y, empty inputs
- `test_retain_patient_chars_recipe` - Retain patient characteristics profile: age capping, LLM cleaning, and tag removal

**Profile Combination Tests:**
- `test_basic_retain_uid_should_have_original_uid` - Basic profile + retain UID option
- `test_basic_retain_date_should_have_original_date` - Basic profile + retain full dates
- `test_basic_modified_date_should_have_modified_date` - Basic profile + date shifting
- `test_keep_specific_private_tags_should_be_original_value` - Safe private tag retention

**Configuration Tests:**
- `test_keep_temp_files` - Verify that temporary directories are preserved when `keepTempFiles: true`
- `test_keep_temp_files_default` - Verify that temporary directories are removed by default
- `test_physical_face_pixelation_size_mm_custom` - Verify custom pixelation block size is applied

**Export and Integration Tests:**
- `test_uid_mapping_file_creation` - UID mappings CSV generation
- `test_parquet_metadata_export` - Parquet metadata export with dynamic schema
- `test_csv_and_parquet_consistency` - Consistency between export formats
- `test_defacer_service_makes_defacing` - Defacing pipeline integration

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

## Appendix C: Basic Application Confidentiality Profile Recipe

This appendix provides the complete list of DICOM tags processed by the Basic Application Confidentiality Profile, showing the specific actions applied to each tag. This table represents the actual recipe file generated by Luwak's anonymization recipe builder when the Basic Profile is selected (see [§5.4.1](#541-basic-application-confidentiality-profile---action-mapping-logic) and [§6](#6-deidentification-recipe-creation-pipeline-stage-4---5)).

### C.1 Recipe Format

The recipe follows the DEID library format with the following structure:
- **FORMAT**: `dicom` - Specifies DICOM format processing
- **%header**: Section containing all tag-level operations
- **Comment Lines**: Begin with `#` and provide the human-readable tag name
- **Action Lines**: Specify the operation to perform on each tag

### C.2 Complete Tag Actions Table

| Tag | Tag Name | Action | Replacement Value |
|-----|----------|--------|-------------------|
| (0000,1000) | Affected SOP Instance UID | REMOVE | - |
| (0000,1001) | Requested SOP Instance UID | REPLACE | func:generate_hmacuid |
| (0002,0003) | Media Storage SOP Instance UID | REPLACE | func:generate_hmacuid |
| (0004,1511) | Referenced SOP Instance UID in File | REPLACE | func:generate_hmacuid |
| (0008,0012) | Instance Creation Date | REPLACE | func:set_fixed_datetime |
| (0008,0013) | Instance Creation Time | REPLACE | func:set_fixed_datetime |
| (0008,0014) | Instance Creator UID | REPLACE | func:generate_hmacuid |
| (0008,0015) | Instance Coercion DateTime | REMOVE | - |
| (0008,0017) | Acquisition UID | REPLACE | func:generate_hmacuid |
| (0008,0018) | SOP Instance UID | REPLACE | func:generate_hmacuid |
| (0008,0019) | Pyramid UID | REPLACE | func:generate_hmacuid |
| (0008,0020) | Study Date | REPLACE | func:set_fixed_datetime |
| (0008,0021) | Series Date | REPLACE | func:set_fixed_datetime |
| (0008,0022) | Acquisition Date | REPLACE | func:set_fixed_datetime |
| (0008,0023) | Content Date | REPLACE | func:set_fixed_datetime |
| (0008,0024) | Overlay Date | REMOVE | - |
| (0008,0025) | Curve Date | REMOVE | - |
| (0008,002A) | Acquisition DateTime | REPLACE | func:set_fixed_datetime |
| (0008,0030) | Study Time | REPLACE | func:set_fixed_datetime |
| (0008,0031) | Series Time | REPLACE | func:set_fixed_datetime |
| (0008,0032) | Acquisition Time | REPLACE | func:set_fixed_datetime |
| (0008,0033) | Content Time | REPLACE | func:set_fixed_datetime |
| (0008,0034) | Overlay Time | REMOVE | - |
| (0008,0035) | Curve Time | REMOVE | - |
| (0008,0050) | Accession Number | BLANK | - |
| (0008,0054) | Retrieve AE Title | REMOVE | - |
| (0008,0055) | Station AE Title | REMOVE | - |
| (0008,0058) | Failed SOP Instance UID List | REPLACE | func:generate_hmacuid |
| (0008,0080) | Institution Name | REPLACE | ANONYMIZED |
| (0008,0081) | Institution Address | REMOVE | - |
| (0008,0082) | Institution Code Sequence | REMOVE | - |
| (0008,0090) | Referring Physician's Name | BLANK | - |
| (0008,0092) | Referring Physician's Address | REMOVE | - |
| (0008,0094) | Referring Physician's Telephone Numbers | REMOVE | - |
| (0008,0096) | Referring Physician Identification Sequence | REMOVE | - |
| (0008,009C) | Consulting Physician's Name | BLANK | - |
| (0008,009D) | Consulting Physician Identification Sequence | REMOVE | - |
| (0008,0106) | Context Group Version | REPLACE | func:set_fixed_datetime |
| (0008,0107) | Context Group Local Version | REPLACE | func:set_fixed_datetime |
| (0008,0201) | Timezone Offset From UTC | REMOVE | - |
| (0008,1000) | Network ID | REMOVE | - |
| (0008,1010) | Station Name | REPLACE | ANONYMIZED |
| (0008,1030) | Study Description | REMOVE | - |
| (0008,103E) | Series Description | REMOVE | - |
| (0008,1040) | Institutional Department Name | REMOVE | - |
| (0008,1041) | Institutional Department Type Code Sequence | REMOVE | - |
| (0008,1048) | Physician(s) of Record | REMOVE | - |
| (0008,1049) | Physician(s) of Record Identification Sequence | REMOVE | - |
| (0008,1050) | Performing Physician's Name | REMOVE | - |
| (0008,1052) | Performing Physician Identification Sequence | REMOVE | - |
| (0008,1060) | Name of Physician(s) Reading Study | REMOVE | - |
| (0008,1062) | Physician(s) Reading Study Identification Sequence | REMOVE | - |
| (0008,1070) | Operators' Name | REPLACE | Anonymized^Anonymized |
| (0008,1072) | Operator Identification Sequence | REMOVE | - |
| (0008,1080) | Admitting Diagnoses Description | REMOVE | - |
| (0008,1084) | Admitting Diagnoses Code Sequence | REMOVE | - |
| (0008,1088) | Pyramid Description | REMOVE | - |
| (0008,1110) | Referenced Study Sequence | BLANK | - |
| (0008,1120) | Referenced Patient Sequence | REMOVE | - |
| (0008,1140) | Referenced Image Sequence | BLANK | - |
| (0008,1155) | Referenced SOP Instance UID | REPLACE | func:generate_hmacuid |
| (0008,1195) | Transaction UID | REPLACE | func:generate_hmacuid |
| (0008,1301) | Principal Diagnosis Code Sequence | REMOVE | - |
| (0008,1302) | Primary Diagnosis Code Sequence | REMOVE | - |
| (0008,1303) | Secondary Diagnoses Code Sequence | REMOVE | - |
| (0008,1304) | Histological Diagnoses Code Sequence | REMOVE | - |
| (0008,2111) | Derivation Description | REMOVE | - |
| (0008,2112) | Source Image Sequence | BLANK | - |
| (0008,3010) | Irradiation Event UID | REPLACE | func:generate_hmacuid |
| (0008,4000) | Identifying Comments | REMOVE | - |
| (0010,0010) | Patient's Name | REPLACE | func:generate_patient_id |
| (0010,0011) | Person Names to Use Sequence | REMOVE | - |
| (0010,0012) | Name to Use | REMOVE | - |
| (0010,0013) | Name to Use Comment | REMOVE | - |
| (0010,0014) | Third Person Pronouns Sequence | REMOVE | - |
| (0010,0015) | Pronoun Code Sequence | REMOVE | - |
| (0010,0016) | Pronoun Comment | REMOVE | - |
| (0010,0020) | Patient ID | REPLACE | func:generate_patient_id |
| (0010,0021) | Issuer of Patient ID | REMOVE | - |
| (0010,0030) | Patient's Birth Date | REPLACE | func:set_fixed_datetime |
| (0010,0032) | Patient's Birth Time | REMOVE | - |
| (0010,0040) | Patient's Sex | BLANK | - |
| (0010,0041) | Gender Identity Sequence | REMOVE | - |
| (0010,0042) | Sex Parameters for Clinical Use Category Comment | REMOVE | - |
| (0010,0043) | Sex Parameters for Clinical Use Category Sequence | REMOVE | - |
| (0010,0044) | Gender Identity Code Sequence | REMOVE | - |
| (0010,0045) | Gender Identity Comment | REMOVE | - |
| (0010,0046) | Sex Parameters for Clinical Use Category Code Sequence | REMOVE | - |
| (0010,0047) | Sex Parameters for Clinical Use Category Reference | REMOVE | - |
| (0010,0050) | Patient's Insurance Plan Code Sequence | REMOVE | - |
| (0010,0101) | Patient's Primary Language Code Sequence | REMOVE | - |
| (0010,0102) | Patient's Primary Language Modifier Code Sequence | REMOVE | - |
| (0010,1000) | Other Patient IDs | REMOVE | - |
| (0010,1001) | Other Patient Names | REMOVE | - |
| (0010,1002) | Other Patient IDs Sequence | REMOVE | - |
| (0010,1005) | Patient's Birth Name | REMOVE | - |
| (0010,1010) | Patient's Age | REMOVE | - |
| (0010,1020) | Patient's Size | REMOVE | - |
| (0010,1030) | Patient's Weight | REMOVE | - |
| (0010,1040) | Patient's Address | REMOVE | - |
| (0010,1050) | Insurance Plan Identification | REMOVE | - |
| (0010,1060) | Patient's Mother's Birth Name | REMOVE | - |
| (0010,1080) | Military Rank | REMOVE | - |
| (0010,1081) | Branch of Service | REMOVE | - |
| (0010,1090) | Medical Record Locator | REMOVE | - |
| (0010,1100) | Referenced Patient Photo Sequence | REMOVE | - |
| (0010,2000) | Medical Alerts | REMOVE | - |
| (0010,2110) | Allergies | REMOVE | - |
| (0010,2150) | Country of Residence | REMOVE | - |
| (0010,2152) | Region of Residence | REMOVE | - |
| (0010,2154) | Patient's Telephone Numbers | REMOVE | - |
| (0010,2155) | Patient's Telecom Information | REMOVE | - |
| (0010,2160) | Ethnic Group | REMOVE | - |
| (0010,2161) | Ethnic Group Code Sequence | REMOVE | - |
| (0010,2162) | Ethnic Groups | REMOVE | - |
| (0010,2180) | Occupation | REMOVE | - |
| (0010,21A0) | Smoking Status | REMOVE | - |
| (0010,21B0) | Additional Patient History | REMOVE | - |
| (0010,21C0) | Pregnancy Status | REMOVE | - |
| (0010,21D0) | Last Menstrual Date | REMOVE | - |
| (0010,21F0) | Patient's Religious Preference | REMOVE | - |
| (0010,2203) | Patient's Sex Neutered | BLANK | - |
| (0010,2297) | Responsible Person | REMOVE | - |
| (0010,2299) | Responsible Organization | REMOVE | - |
| (0010,4000) | Patient Comments | REMOVE | - |
| (0012,0010) | Clinical Trial Sponsor Name | REPLACE | ANONYMIZED |
| (0012,0020) | Clinical Trial Protocol ID | REPLACE | ANONYMIZED |
| (0012,0021) | Clinical Trial Protocol Name | BLANK | - |
| (0012,0022) | Issuer of Clinical Trial Protocol ID | REMOVE | - |
| (0012,0023) | Other Clinical Trial Protocol IDs Sequence | REMOVE | - |
| (0012,0030) | Clinical Trial Site ID | BLANK | - |
| (0012,0031) | Clinical Trial Site Name | BLANK | - |
| (0012,0032) | Issuer of Clinical Trial Site ID | REMOVE | - |
| (0012,0040) | Clinical Trial Subject ID | REPLACE | ANONYMIZED |
| (0012,0041) | Issuer of Clinical Trial Subject ID | REMOVE | - |
| (0012,0042) | Clinical Trial Subject Reading ID | REPLACE | ANONYMIZED |
| (0012,0043) | Issuer of Clinical Trial Subject Reading ID | REMOVE | - |
| (0012,0050) | Clinical Trial Time Point ID | BLANK | - |
| (0012,0051) | Clinical Trial Time Point Description | REMOVE | - |
| (0012,0055) | Issuer of Clinical Trial Time Point ID | REMOVE | - |
| (0012,0060) | Clinical Trial Coordinating Center Name | BLANK | - |
| (0012,0071) | Clinical Trial Series ID | REMOVE | - |
| (0012,0072) | Clinical Trial Series Description | REMOVE | - |
| (0012,0073) | Issuer of Clinical Trial Series ID | REMOVE | - |
| (0012,0081) | Clinical Trial Protocol Ethics Committee Name | REPLACE | ANONYMIZED |
| (0012,0082) | Clinical Trial Protocol Ethics Committee Approval Number | REMOVE | - |
| (0012,0086) | Ethics Committee Approval Effectiveness Start Date | REMOVE | - |
| (0012,0087) | Ethics Committee Approval Effectiveness End Date | REMOVE | - |
| (0014,407C) | Calibration Time | REMOVE | - |
| (0014,407E) | Calibration Date | REMOVE | - |
| (0016,002B) | Maker Note | REMOVE | - |
| (0016,004B) | Device Setting Description | REMOVE | - |
| (0016,004D) | Camera Owner Name | REMOVE | - |
| (0016,004E) | Lens Specification | REMOVE | - |
| (0016,004F) | Lens Make | REMOVE | - |
| (0016,0050) | Lens Model | REMOVE | - |
| (0016,0051) | Lens Serial Number | REMOVE | - |
| (0016,0070) | GPS Version ID | REMOVE | - |
| (0016,0071) | GPS Latitude​ Ref | REMOVE | - |
| (0016,0072) | GPS Latitude​ | REMOVE | - |
| (0016,0073) | GPS Longitude Ref | REMOVE | - |
| (0016,0074) | GPS Longitude | REMOVE | - |
| (0016,0075) | GPS Altitude​ Ref | REMOVE | - |
| (0016,0076) | GPS Altitude​ | REMOVE | - |
| (0016,0077) | GPS Time​ Stamp | REMOVE | - |
| (0016,0078) | GPS Satellites | REMOVE | - |
| (0016,0079) | GPS Status | REMOVE | - |
| (0016,007A) | GPS Measure ​Mode | REMOVE | - |
| (0016,007B) | GPS DOP | REMOVE | - |
| (0016,007C) | GPS Speed​ Ref | REMOVE | - |
| (0016,007D) | GPS Speed​ | REMOVE | - |
| (0016,007E) | GPS Track ​Ref | REMOVE | - |
| (0016,007F) | GPS Track | REMOVE | - |
| (0016,0080) | GPS Img​ Direction Ref | REMOVE | - |
| (0016,0081) | GPS Img ​Direction | REMOVE | - |
| (0016,0082) | GPS Map​ Datum | REMOVE | - |
| (0016,0083) | GPS Dest​ Latitude Ref | REMOVE | - |
| (0016,0084) | GPS Dest​ Latitude | REMOVE | - |
| (0016,0085) | GPS Dest ​Longitude Ref | REMOVE | - |
| (0016,0086) | GPS Dest ​Longitude | REMOVE | - |
| (0016,0087) | GPS Dest​ Bearing Ref | REMOVE | - |
| (0016,0088) | GPS Dest ​Bearing | REMOVE | - |
| (0016,0089) | GPS Dest ​Distance Ref | REMOVE | - |
| (0016,008A) | GPS Dest ​Distance | REMOVE | - |
| (0016,008B) | GPS Processing​ Method | REMOVE | - |
| (0016,008C) | GPS Area ​Information | REMOVE | - |
| (0016,008D) | GPS Date​ Stamp | REMOVE | - |
| (0016,008E) | GPS Differential | REMOVE | - |
| (0018,0010) | Contrast/Bolus Agent | REPLACE | ANONYMIZED |
| (0018,0027) | Intervention Drug Stop Time | REMOVE | - |
| (0018,0035) | Intervention Drug Start Time | REMOVE | - |
| (0018,1000) | Device Serial Number | REPLACE | ANONYMIZED |
| (0018,1002) | Device UID | REPLACE | func:generate_hmacuid |
| (0018,1004) | Plate ID | REMOVE | - |
| (0018,1005) | Generator ID | REMOVE | - |
| (0018,1007) | Cassette ID | REMOVE | - |
| (0018,1008) | Gantry ID | REMOVE | - |
| (0018,1009) | Unique Device Identifier | REMOVE | - |
| (0018,100A) | UDI Sequence | REMOVE | - |
| (0018,100B) | Manufacturer's Device Class UID | REPLACE | func:generate_hmacuid |
| (0018,1010) | Secondary Capture Device ID | REMOVE | - |
| (0018,1011) | Hardcopy Creation Device ID | REMOVE | - |
| (0018,1012) | Date of Secondary Capture | REMOVE | - |
| (0018,1014) | Time of Secondary Capture | REMOVE | - |
| (0018,1030) | Protocol Name | REPLACE | ANONYMIZED |
| (0018,1042) | Contrast/Bolus Start Time | REMOVE | - |
| (0018,1043) | Contrast/Bolus Stop Time | REMOVE | - |
| (0018,1072) | Radiopharmaceutical Start Time | REMOVE | - |
| (0018,1073) | Radiopharmaceutical Stop Time | REMOVE | - |
| (0018,1078) | Radiopharmaceutical Start DateTime | REMOVE | - |
| (0018,1079) | Radiopharmaceutical Stop DateTime | REMOVE | - |
| (0018,11BB) | Acquisition Field Of View Label | REPLACE | ANONYMIZED |
| (0018,1200) | Date of Last Calibration | REMOVE | - |
| (0018,1201) | Time of Last Calibration | REMOVE | - |
| (0018,1202) | DateTime of Last Calibration | REMOVE | - |
| (0018,1203) | Calibration DateTime | REPLACE | func:set_fixed_datetime |
| (0018,1204) | Date of Manufacture | REMOVE | - |
| (0018,1205) | Date of Installation | REMOVE | - |
| (0018,1400) | Acquisition Device Processing Description | REPLACE | ANONYMIZED |
| (0018,2042) | Target UID | REPLACE | func:generate_hmacuid |
| (0018,4000) | Acquisition Comments | REMOVE | - |
| (0018,5011) | Transducer Identification Sequence | REMOVE | - |
| (0018,700A) | Detector ID | REPLACE | ANONYMIZED |
| (0018,700C) | Date of Last Detector Calibration | REPLACE | func:set_fixed_datetime |
| (0018,700E) | Time of Last Detector Calibration | REPLACE | func:set_fixed_datetime |
| (0018,9074) | Frame Acquisition DateTime | REPLACE | func:set_fixed_datetime |
| (0018,9151) | Frame Reference DateTime | REPLACE | func:set_fixed_datetime |
| (0018,9185) | Respiratory Motion Compensation Technique Description | REMOVE | - |
| (0018,9367) | X-Ray Source ID | REPLACE | ANONYMIZED |
| (0018,9369) | Source Start DateTime | REPLACE | func:set_fixed_datetime |
| (0018,936A) | Source End DateTime | REPLACE | func:set_fixed_datetime |
| (0018,9371) | X-Ray Detector ID | REPLACE | ANONYMIZED |
| (0018,9373) | X-Ray Detector Label | REMOVE | - |
| (0018,937B) | Multi-energy Acquisition Description | REMOVE | - |
| (0018,937F) | Decomposition Description | REMOVE | - |
| (0018,9424) | Acquisition Protocol Description | REMOVE | - |
| (0018,9516) | Start Acquisition DateTime | REPLACE | func:set_fixed_datetime |
| (0018,9517) | End Acquisition DateTime | REPLACE | func:set_fixed_datetime |
| (0018,9623) | Functional Sync Pulse | REPLACE | func:set_fixed_datetime |
| (0018,9701) | Decay Correction DateTime | REPLACE | func:set_fixed_datetime |
| (0018,9804) | Exclusion Start DateTime | REPLACE | func:set_fixed_datetime |
| (0018,9919) | Instruction Performed DateTime | REPLACE | func:set_fixed_datetime |
| (0018,9937) | Requested Series Description | REMOVE | - |
| (0018,A002) | Contribution DateTime | REMOVE | - |
| (0018,A003) | Contribution Description | REMOVE | - |
| (0020,000D) | Study Instance UID | REPLACE | func:generate_hmacuid |
| (0020,000E) | Series Instance UID | REPLACE | func:generate_hmacuid |
| (0020,0010) | Study ID | BLANK | - |
| (0020,0027) | Pyramid Label | REMOVE | - |
| (0020,0052) | Frame of Reference UID | REPLACE | func:generate_hmacuid |
| (0020,0200) | Synchronization Frame of Reference UID | REPLACE | func:generate_hmacuid |
| (0020,3401) | Modifying Device ID | REMOVE | - |
| (0020,3403) | Modified Image Date | REMOVE | - |
| (0020,3405) | Modified Image Time | REMOVE | - |
| (0020,3406) | Modified Image Description | REMOVE | - |
| (0020,4000) | Image Comments | REMOVE | - |
| (0020,9158) | Frame Comments | REMOVE | - |
| (0020,9161) | Concatenation UID | REPLACE | func:generate_hmacuid |
| (0020,9164) | Dimension Organization UID | REPLACE | func:generate_hmacuid |
| (0028,1199) | Palette Color Lookup Table UID | REPLACE | func:generate_hmacuid |
| (0028,1214) | Large Palette Color Lookup Table UID | REPLACE | func:generate_hmacuid |
| (0028,4000) | Image Presentation Comments | REMOVE | - |
| (0032,0012) | Study ID Issuer | REMOVE | - |
| (0032,0032) | Study Verified Date | REMOVE | - |
| (0032,0033) | Study Verified Time | REMOVE | - |
| (0032,0034) | Study Read Date | REMOVE | - |
| (0032,0035) | Study Read Time | REMOVE | - |
| (0032,1000) | Scheduled Study Start Date | REMOVE | - |
| (0032,1001) | Scheduled Study Start Time | REMOVE | - |
| (0032,1010) | Scheduled Study Stop Date | REMOVE | - |
| (0032,1011) | Scheduled Study Stop Time | REMOVE | - |
| (0032,1020) | Scheduled Study Location | REMOVE | - |
| (0032,1021) | Scheduled Study Location AE Title | REMOVE | - |
| (0032,1030) | Reason for Study | REMOVE | - |
| (0032,1032) | Requesting Physician | REMOVE | - |
| (0032,1033) | Requesting Service | REMOVE | - |
| (0032,1040) | Study Arrival Date | REMOVE | - |
| (0032,1041) | Study Arrival Time | REMOVE | - |
| (0032,1050) | Study Completion Date | REMOVE | - |
| (0032,1051) | Study Completion Time | REMOVE | - |
| (0032,1060) | Requested Procedure Description | BLANK | - |
| (0032,1066) | Reason for Visit | REMOVE | - |
| (0032,1067) | Reason for Visit Code Sequence | REMOVE | - |
| (0032,1070) | Requested Contrast Agent | REMOVE | - |
| (0032,4000) | Study Comments | REMOVE | - |
| (0034,0002) | Flow Identifier | REMOVE | - |
| (0034,0005) | Source Identifier | REMOVE | - |
| (0034,0007) | Frame Origin Timestamp | REMOVE | - |
| (0038,0004) | Referenced Patient Alias Sequence | REMOVE | - |
| (0038,0010) | Admission ID | REMOVE | - |
| (0038,0011) | Issuer of Admission ID | REMOVE | - |
| (0038,0014) | Issuer of Admission ID Sequence | REMOVE | - |
| (0038,001A) | Scheduled Admission Date | REMOVE | - |
| (0038,001B) | Scheduled Admission Time | REMOVE | - |
| (0038,001C) | Scheduled Discharge Date | REMOVE | - |
| (0038,001D) | Scheduled Discharge Time | REMOVE | - |
| (0038,001E) | Scheduled Patient Institution Residence | REMOVE | - |
| (0038,0020) | Admitting Date | REMOVE | - |
| (0038,0021) | Admitting Time | REMOVE | - |
| (0038,0030) | Discharge Date | REMOVE | - |
| (0038,0032) | Discharge Time | REMOVE | - |
| (0038,0040) | Discharge Diagnosis Description | REMOVE | - |
| (0038,0050) | Special Needs | REMOVE | - |
| (0038,0060) | Service Episode ID | REMOVE | - |
| (0038,0061) | Issuer of Service Episode ID | REMOVE | - |
| (0038,0062) | Service Episode Description | REMOVE | - |
| (0038,0064) | Issuer of Service Episode ID Sequence | REMOVE | - |
| (0038,0300) | Current Patient Location | REMOVE | - |
| (0038,0400) | Patient's Institution Residence | REMOVE | - |
| (0038,0500) | Patient State | REMOVE | - |
| (0038,4000) | Visit Comments | REMOVE | - |
| (003A,0020) | Multiplex Group Label | REMOVE | - |
| (003A,0203) | Channel Label | REMOVE | - |
| (003A,020C) | Channel Derivation Description | REMOVE | - |
| (003A,0310) | Multiplex Group UID | REPLACE | func:generate_hmacuid |
| (003A,0314) | Impedance Measurement DateTime | REPLACE | func:set_fixed_datetime |
| (003A,0329) | ​Waveform Filter Description | REMOVE | - |
| (003A,032B) | Filter Lookup Table Description | REMOVE | - |
| (0040,0001) | Scheduled Station AE Title | REMOVE | - |
| (0040,0002) | Scheduled Procedure Step Start Date | REMOVE | - |
| (0040,0003) | Scheduled Procedure Step Start Time | REMOVE | - |
| (0040,0004) | Scheduled Procedure Step End Date | REMOVE | - |
| (0040,0005) | Scheduled Procedure Step End Time | REMOVE | - |
| (0040,0006) | Scheduled Performing Physician's Name | REMOVE | - |
| (0040,0007) | Scheduled Procedure Step Description | REMOVE | - |
| (0040,0009) | Scheduled Procedure Step ID | REMOVE | - |
| (0040,000B) | Scheduled Performing Physician Identification Sequence | REMOVE | - |
| (0040,0010) | Scheduled Station Name | REMOVE | - |
| (0040,0011) | Scheduled Procedure Step Location | REMOVE | - |
| (0040,0012) | Pre-Medication | REMOVE | - |
| (0040,0241) | Performed Station AE Title | REMOVE | - |
| (0040,0242) | Performed Station Name | REMOVE | - |
| (0040,0243) | Performed Location | REMOVE | - |
| (0040,0244) | Performed Procedure Step Start Date | REMOVE | - |
| (0040,0245) | Performed Procedure Step Start Time | REMOVE | - |
| (0040,0250) | Performed Procedure Step End Date | REMOVE | - |
| (0040,0251) | Performed Procedure Step End Time | REMOVE | - |
| (0040,0253) | Performed Procedure Step ID | REMOVE | - |
| (0040,0254) | Performed Procedure Step Description | REMOVE | - |
| (0040,0275) | Request Attributes Sequence | REMOVE | - |
| (0040,0280) | Comments on the Performed Procedure Step | REMOVE | - |
| (0040,0310) | Comments on Radiation Dose | REMOVE | - |
| (0040,050A) | Specimen Accession Number | REMOVE | - |
| (0040,0512) | Container Identifier | REPLACE | ANONYMIZED |
| (0040,0513) | Issuer of the Container Identifier Sequence | BLANK | - |
| (0040,051A) | Container Description | REMOVE | - |
| (0040,0551) | Specimen Identifier | REPLACE | ANONYMIZED |
| (0040,0554) | Specimen UID | REPLACE | func:generate_hmacuid |
| (0040,0555) | Acquisition Context Sequence | BLANK | - |
| (0040,0556) | Acquisition Context Description | REMOVE | - |
| (0040,0562) | Issuer of the Specimen Identifier Sequence | BLANK | - |
| (0040,0600) | Specimen Short Description | REMOVE | - |
| (0040,0602) | Specimen Detailed Description | REMOVE | - |
| (0040,0610) | Specimen Preparation Sequence | BLANK | - |
| (0040,06FA) | Slide Identifier | REMOVE | - |
| (0040,1001) | Requested Procedure ID | REMOVE | - |
| (0040,1002) | Reason for the Requested Procedure | REMOVE | - |
| (0040,1004) | Patient Transport Arrangements | REMOVE | - |
| (0040,1005) | Requested Procedure Location | REMOVE | - |
| (0040,100A) | Reason for Requested Procedure Code Sequence | REMOVE | - |
| (0040,1010) | Names of Intended Recipients of Results | REMOVE | - |
| (0040,1011) | Intended Recipients of Results Identification Sequence | REMOVE | - |
| (0040,1101) | Person Identification Code Sequence | REMOVE | - |
| (0040,1102) | Person's Address | REMOVE | - |
| (0040,1103) | Person's Telephone Numbers | REMOVE | - |
| (0040,1104) | Person's Telecom Information | REMOVE | - |
| (0040,1400) | Requested Procedure Comments | REMOVE | - |
| (0040,2001) | Reason for the Imaging Service Request | REMOVE | - |
| (0040,2004) | Issue Date of Imaging Service Request | REMOVE | - |
| (0040,2005) | Issue Time of Imaging Service Request | REMOVE | - |
| (0040,2008) | Order Entered By | REMOVE | - |
| (0040,2009) | Order Enterer's Location | REMOVE | - |
| (0040,2010) | Order Callback Phone Number | REMOVE | - |
| (0040,2011) | Order Callback Telecom Information | REMOVE | - |
| (0040,2016) | Placer Order Number / Imaging Service Request | BLANK | - |
| (0040,2017) | Filler Order Number / Imaging Service Request | BLANK | - |
| (0040,2400) | Imaging Service Request Comments | REMOVE | - |
| (0040,3001) | Confidentiality Constraint on Patient Data Description | REMOVE | - |
| (0040,4005) | Scheduled Procedure Step Start DateTime | REMOVE | - |
| (0040,4008) | Scheduled Procedure Step Expiration DateTime | REMOVE | - |
| (0040,4010) | Scheduled Procedure Step Modification DateTime | REMOVE | - |
| (0040,4011) | Expected Completion DateTime | REMOVE | - |
| (0040,4023) | Referenced General Purpose Scheduled Procedure Step Transaction UID | REPLACE | func:generate_hmacuid |
| (0040,4025) | Scheduled Station Name Code Sequence | REMOVE | - |
| (0040,4027) | Scheduled Station Geographic Location Code Sequence | REMOVE | - |
| (0040,4028) | Performed Station Name Code Sequence | REMOVE | - |
| (0040,4030) | Performed Station Geographic Location Code Sequence | REMOVE | - |
| (0040,4034) | Scheduled Human Performers Sequence | REMOVE | - |
| (0040,4035) | Actual Human Performers Sequence | REMOVE | - |
| (0040,4036) | Human Performer's Organization | REMOVE | - |
| (0040,4037) | Human Performer's Name | REMOVE | - |
| (0040,4050) | Performed Procedure Step Start DateTime | REMOVE | - |
| (0040,4051) | Performed Procedure Step End DateTime | REMOVE | - |
| (0040,4052) | Procedure Step Cancellation DateTime | REMOVE | - |
| (0040,A023) | Findings Group Recording Date (Trial) | REMOVE | - |
| (0040,A024) | Findings Group Recording Time (Trial) | REMOVE | - |
| (0040,A027) | Verifying Organization | REPLACE | ANONYMIZED |
| (0040,A030) | Verification DateTime | REPLACE | func:set_fixed_datetime |
| (0040,A032) | Observation DateTime | REPLACE | func:set_fixed_datetime |
| (0040,A033) | Observation Start DateTime | REMOVE | - |
| (0040,A034) | Effective Start DateTime | REMOVE | - |
| (0040,A035) | Effective Stop DateTime | REMOVE | - |
| (0040,A075) | Verifying Observer Name | REPLACE | Anonymized^Anonymized |
| (0040,A078) | Author Observer Sequence | REMOVE | - |
| (0040,A07A) | Participant Sequence | REMOVE | - |
| (0040,A07C) | Custodial Organization Sequence | REMOVE | - |
| (0040,A082) | Participation DateTime | REPLACE | func:set_fixed_datetime |
| (0040,A088) | Verifying Observer Identification Code Sequence | BLANK | - |
| (0040,A110) | Date of Document or Verbal Transaction (Trial) | REMOVE | - |
| (0040,A112) | Time of Document Creation or Verbal Transaction (Trial) | REMOVE | - |
| (0040,A120) | DateTime | REPLACE | func:set_fixed_datetime |
| (0040,A121) | Date | REPLACE | func:set_fixed_datetime |
| (0040,A122) | Time | REPLACE | func:set_fixed_datetime |
| (0040,A123) | Person Name | REPLACE | Anonymized^Anonymized |
| (0040,A124) | UID | REPLACE | func:generate_hmacuid |
| (0040,A13A) | Referenced DateTime | REPLACE | func:set_fixed_datetime |
| (0040,A171) | Observation UID | REPLACE | func:generate_hmacuid |
| (0040,A172) | Referenced Observation UID (Trial) | REPLACE | func:generate_hmacuid |
| (0040,A192) | Observation Date (Trial) | REMOVE | - |
| (0040,A193) | Observation Time (Trial) | REMOVE | - |
| (0040,A307) | Current Observer (Trial) | REMOVE | - |
| (0040,A352) | Verbal Source (Trial) | REMOVE | - |
| (0040,A353) | Address (Trial) | REMOVE | - |
| (0040,A354) | Telephone Number (Trial) | REMOVE | - |
| (0040,A358) | Verbal Source Identifier Code Sequence (Trial) | REMOVE | - |
| (0040,A402) | Observation Subject UID (Trial) | REPLACE | func:generate_hmacuid |
| (0040,B034) | Annotation DateTime | REMOVE | - |
| (0040,B036) | Segment Definition DateTime | REMOVE | - |
| (0040,B03B) | Montage Name | REMOVE | - |
| (0040,B03F) | Montage Channel Label | REMOVE | - |
| (0040,DB06) | Template Version | REMOVE | - |
| (0040,DB07) | Template Local Version | REMOVE | - |
| (0040,DB0C) | Template Extension Organization UID | REPLACE | func:generate_hmacuid |
| (0040,DB0D) | Template Extension Creator UID | REPLACE | func:generate_hmacuid |
| (0040,E004) | HL7 Document Effective Time | REMOVE | - |
| (0042,0011) | Encapsulated Document | REMOVE | - |
| (0044,0004) | Approval Status DateTime | REMOVE | - |
| (0044,000B) | Product Expiration DateTime | REMOVE | - |
| (0044,0010) | Substance Administration DateTime | REMOVE | - |
| (0044,0104) | Assertion DateTime | REPLACE | func:set_fixed_datetime |
| (0044,0105) | Assertion Expiration DateTime | REMOVE | - |
| (0050,001B) | Container Component ID | REMOVE | - |
| (0050,0020) | Device Description | REMOVE | - |
| (0050,0021) | Long Device Description | REMOVE | - |
| (0062,0021) | Tracking UID | REPLACE | func:generate_hmacuid |
| (0064,0003) | Source Frame of Reference UID | REPLACE | func:generate_hmacuid |
| (0068,6226) | Effective DateTime | REPLACE | func:set_fixed_datetime |
| (0068,6270) | Information Issue DateTime | REPLACE | func:set_fixed_datetime |
| (006A,0003) | Annotation Group UID | REPLACE | func:generate_hmacuid |
| (006A,0005) | Annotation Group Label | REPLACE | ANONYMIZED |
| (006A,0006) | Annotation Group Description | REMOVE | - |
| (0070,0001) | Graphic Annotation Sequence | REMOVE | - |
| (0070,0006) | Unformatted Text Value | REPLACE | ANONYMIZED |
| (0070,0082) | Presentation Creation Date | REMOVE | - |
| (0070,0083) | Presentation Creation Time | REMOVE | - |
| (0070,0084) | Content Creator's Name | REPLACE | Anonymized^Anonymized |
| (0070,0086) | Content Creator's Identification Code Sequence | REMOVE | - |
| (0070,031A) | Fiducial UID | REPLACE | func:generate_hmacuid |
| (0070,1101) | Presentation Display Collection UID | REPLACE | func:generate_hmacuid |
| (0070,1102) | Presentation Sequence Collection UID | REPLACE | func:generate_hmacuid |
| (0072,000A) | Hanging Protocol Creation DateTime | REPLACE | func:set_fixed_datetime |
| (0072,005E) | Selector AE Value | REPLACE | ANONYMIZED |
| (0072,005F) | Selector AS Value | REPLACE | 000D |
| (0072,0061) | Selector DA Value | REPLACE | func:set_fixed_datetime |
| (0072,0063) | Selector DT Value | REPLACE | func:set_fixed_datetime |
| (0072,0065) | Selector OB Value | REMOVE | - |
| (0072,0066) | Selector LO Value | REPLACE | ANONYMIZED |
| (0072,0068) | Selector LT Value | REPLACE | ANONYMIZED |
| (0072,006A) | Selector PN Value | REPLACE | Anonymized^Anonymized |
| (0072,006B) | Selector TM Value | REPLACE | func:set_fixed_datetime |
| (0072,006C) | Selector SH Value | REPLACE | ANONYMIZED |
| (0072,006D) | Selector UN Value | REMOVE | - |
| (0072,006E) | Selector ST Value | REPLACE | ANONYMIZED |
| (0072,0070) | Selector UT Value | REPLACE | ANONYMIZED |
| (0072,0071) | Selector UR Value | REPLACE | ANONYMIZED |
| (0074,1234) | Receiving AE | REMOVE | - |
| (0074,1236) | Requesting AE | REMOVE | - |
| (0088,0140) | Storage Media File-set UID | REPLACE | func:generate_hmacuid |
| (0088,0200) | Icon Image Sequence(see Note 11) | REMOVE | - |
| (0088,0904) | Topic Title | REMOVE | - |
| (0088,0906) | Topic Subject | REMOVE | - |
| (0088,0910) | Topic Author | REMOVE | - |
| (0088,0912) | Topic Keywords | REMOVE | - |
| (0100,0420) | SOP Authorization DateTime | REMOVE | - |
| (0400,0100) | Digital Signature UID | REPLACE | func:generate_hmacuid |
| (0400,0105) | Digital Signature DateTime | REPLACE | func:set_fixed_datetime |
| (0400,0115) | Certificate of Signer | REMOVE | - |
| (0400,0310) | Certified Timestamp | REMOVE | - |
| (0400,0402) | Referenced Digital Signature Sequence | REMOVE | - |
| (0400,0403) | Referenced SOP Instance MAC Sequence | REMOVE | - |
| (0400,0404) | MAC | REMOVE | - |
| (0400,0550) | Modified Attributes Sequence | REMOVE | - |
| (0400,0551) | Nonconforming Modified Attributes Sequence | REMOVE | - |
| (0400,0552) | Nonconforming Data Element Value | REMOVE | - |
| (0400,0561) | Original Attributes Sequence | REMOVE | - |
| (0400,0562) | Attribute Modification DateTime | REPLACE | func:set_fixed_datetime |
| (0400,0563) | Modifying System | REPLACE | ANONYMIZED |
| (0400,0564) | Source of Previous Values | BLANK | - |
| (0400,0565) | Reason for the Attribute Modification | REPLACE | ANONYMIZED |
| (0400,0600) | Instance Origin Status | REMOVE | - |
| (2030,0020) | Text String | REMOVE | - |
| (2100,0040) | Creation Date | REMOVE | - |
| (2100,0050) | Creation Time | REMOVE | - |
| (2100,0070) | Originator | REMOVE | - |
| (2100,0140) | Destination AE | REPLACE | ANONYMIZED |
| (2200,0002) | Label Text | BLANK | - |
| (2200,0005) | Barcode Value | BLANK | - |
| (3002,0121) | Position Acquisition Template Name | REMOVE | - |
| (3002,0123) | Position Acquisition Template Description | REMOVE | - |
| (3006,0002) | Structure Set Label | REPLACE | ANONYMIZED |
| (3006,0004) | Structure Set Name | REMOVE | - |
| (3006,0006) | Structure Set Description | REMOVE | - |
| (3006,0008) | Structure Set Date | REPLACE | func:set_fixed_datetime |
| (3006,0009) | Structure Set Time | REPLACE | func:set_fixed_datetime |
| (3006,0024) | Referenced Frame of Reference UID | REPLACE | func:generate_hmacuid |
| (3006,0026) | ROI Name | BLANK | - |
| (3006,0028) | ROI Description | REMOVE | - |
| (3006,002D) | ROI DateTime | REMOVE | - |
| (3006,002E) | ROI Observation DateTime | REMOVE | - |
| (3006,0038) | ROI Generation Description | REMOVE | - |
| (3006,004D) | ROI Creator Sequence | REMOVE | - |
| (3006,004E) | ROI Interpreter Sequence | REMOVE | - |
| (3006,0085) | ROI Observation Label | REMOVE | - |
| (3006,0088) | ROI Observation Description | REMOVE | - |
| (3006,00A6) | ROI Interpreter | BLANK | - |
| (3006,00C2) | Related Frame of Reference UID | REPLACE | func:generate_hmacuid |
| (3008,0024) | Treatment Control Point Date | REPLACE | func:set_fixed_datetime |
| (3008,0025) | Treatment Control Point Time | REPLACE | func:set_fixed_datetime |
| (3008,0054) | First Treatment Date | REPLACE | func:set_fixed_datetime |
| (3008,0056) | Most Recent Treatment Date | REPLACE | func:set_fixed_datetime |
| (3008,0105) | Source Serial Number | BLANK | - |
| (3008,0162) | Safe Position Exit Date | REPLACE | func:set_fixed_datetime |
| (3008,0164) | Safe Position Exit Time | REPLACE | func:set_fixed_datetime |
| (3008,0166) | Safe Position Return Date | REPLACE | func:set_fixed_datetime |
| (3008,0168) | Safe Position Return Time | REPLACE | func:set_fixed_datetime |
| (3008,0250) | Treatment Date | REPLACE | func:set_fixed_datetime |
| (3008,0251) | Treatment Time | REPLACE | func:set_fixed_datetime |
| (300A,0002) | RT Plan Label | REPLACE | ANONYMIZED |
| (300A,0003) | RT Plan Name | REMOVE | - |
| (300A,0004) | RT Plan Description | REMOVE | - |
| (300A,0006) | RT Plan Date | REPLACE | func:set_fixed_datetime |
| (300A,0007) | RT Plan Time | REPLACE | func:set_fixed_datetime |
| (300A,000B) | Treatment Sites | REMOVE | - |
| (300A,000E) | Prescription Description | REMOVE | - |
| (300A,0013) | Dose Reference UID | REPLACE | func:generate_hmacuid |
| (300A,0016) | Dose Reference Description | REMOVE | - |
| (300A,0054) | Table Top Position Alignment UID | REPLACE | func:generate_hmacuid |
| (300A,0072) | Fraction Group Description | REMOVE | - |
| (300A,0083) | Referenced Dose Reference UID | REPLACE | func:generate_hmacuid |
| (300A,00B2) | Treatment Machine Name | BLANK | - |
| (300A,00C3) | Beam Description | REMOVE | - |
| (300A,00DD) | Bolus Description | REMOVE | - |
| (300A,0196) | Fixation Device Description | REMOVE | - |
| (300A,01A6) | Shielding Device Description | REMOVE | - |
| (300A,01B2) | Setup Technique Description | REMOVE | - |
| (300A,0216) | Source Manufacturer | REMOVE | - |
| (300A,022C) | Source Strength Reference Date | REPLACE | func:set_fixed_datetime |
| (300A,022E) | Source Strength Reference Time | REPLACE | func:set_fixed_datetime |
| (300A,02EB) | Compensator Description | REMOVE | - |
| (300A,0608) | Treatment Position Group Label | REPLACE | ANONYMIZED |
| (300A,0609) | Treatment Position Group UID | REPLACE | func:generate_hmacuid |
| (300A,0611) | RT Accessory Holder Slot ID | BLANK | - |
| (300A,0615) | RT Accessory Device Slot ID | BLANK | - |
| (300A,0619) | Radiation Dose Identification Label | REPLACE | ANONYMIZED |
| (300A,0623) | Radiation Dose In-Vivo Measurement Label | REPLACE | ANONYMIZED |
| (300A,062A) | RT Tolerance Set Label | REPLACE | ANONYMIZED |
| (300A,0650) | Patient Setup UID | REPLACE | func:generate_hmacuid |
| (300A,0676) | Equipment Frame of Reference Description | REMOVE | - |
| (300A,067C) | Radiation Generation Mode Label | REPLACE | ANONYMIZED |
| (300A,067D) | Radiation Generation Mode Description | BLANK | - |
| (300A,0700) | Treatment Session UID | REPLACE | func:generate_hmacuid |
| (300A,0734) | Treatment Tolerance Violation Description | REPLACE | ANONYMIZED |
| (300A,0736) | Treatment Tolerance Violation DateTime | REPLACE | func:set_fixed_datetime |
| (300A,073A) | Recorded RT Control Point DateTime | REPLACE | func:set_fixed_datetime |
| (300A,0741) | Interlock DateTime | REPLACE | func:set_fixed_datetime |
| (300A,0742) | Interlock Description | REPLACE | ANONYMIZED |
| (300A,0760) | Override DateTime | REPLACE | func:set_fixed_datetime |
| (300A,0783) | Interlock Origin Description | REPLACE | ANONYMIZED |
| (300A,0785) | Referenced Treatment Position Group UID | REPLACE | func:generate_hmacuid |
| (300A,078E) | Patient Treatment Preparation Procedure Parameter Description | REMOVE | - |
| (300A,0792) | Patient Treatment Preparation Method Description | REMOVE | - |
| (300A,0794) | Patient Setup Photo Description | REMOVE | - |
| (300A,079A) | Displacement Reference Label | REMOVE | - |
| (300C,0113) | Reason for Omission Description | REMOVE | - |
| (300C,0127) | Beam Hold Transition DateTime | REPLACE | func:set_fixed_datetime |
| (300E,0004) | Review Date | REPLACE | func:set_fixed_datetime |
| (300E,0005) | Review Time | REPLACE | func:set_fixed_datetime |
| (300E,0008) | Reviewer Name | BLANK | - |
| (3010,0006) | Conceptual Volume UID | REPLACE | func:generate_hmacuid |
| (3010,000B) | Referenced Conceptual Volume UID | REPLACE | func:generate_hmacuid |
| (3010,000F) | Conceptual Volume Combination Description | BLANK | - |
| (3010,0013) | Constituent Conceptual Volume UID | REPLACE | func:generate_hmacuid |
| (3010,0015) | Source Conceptual Volume UID | REPLACE | func:generate_hmacuid |
| (3010,0017) | Conceptual Volume Description | BLANK | - |
| (3010,001B) | Device Alternate Identifier | BLANK | - |
| (3010,002D) | Device Label | REPLACE | ANONYMIZED |
| (3010,0031) | Referenced Fiducials UID | REPLACE | func:generate_hmacuid |
| (3010,0033) | User Content Label | REPLACE | ANONYMIZED |
| (3010,0034) | User Content Long Label | REPLACE | ANONYMIZED |
| (3010,0035) | Entity Label | REPLACE | ANONYMIZED |
| (3010,0036) | Entity Name | REMOVE | - |
| (3010,0037) | Entity Description | REMOVE | - |
| (3010,0038) | Entity Long Label | REPLACE | ANONYMIZED |
| (3010,003B) | RT Treatment Phase UID | REPLACE | func:generate_hmacuid |
| (3010,0043) | Manufacturer's Device Identifier | BLANK | - |
| (3010,004C) | Intended Phase Start Date | REPLACE | func:set_fixed_datetime |
| (3010,004D) | Intended Phase End Date | REPLACE | func:set_fixed_datetime |
| (3010,0054) | RT Prescription Label | REPLACE | ANONYMIZED |
| (3010,0056) | RT Treatment Approach Label | REPLACE | ANONYMIZED |
| (3010,005A) | RT Physician Intent Narrative | BLANK | - |
| (3010,005C) | Reason for Superseding | BLANK | - |
| (3010,0061) | Prior Treatment Dose Description | REMOVE | - |
| (3010,006E) | Dosimetric Objective UID | REPLACE | func:generate_hmacuid |
| (3010,006F) | Referenced Dosimetric Objective UID | REPLACE | func:generate_hmacuid |
| (3010,0077) | Treatment Site | REPLACE | ANONYMIZED |
| (3010,007A) | Treatment Technique Notes | BLANK | - |
| (3010,007B) | Prescription Notes | BLANK | - |
| (3010,007F) | Fractionation Notes | BLANK | - |
| (3010,0081) | Prescription Notes Sequence | BLANK | - |
| (3010,0085) | Intended Fraction Start Time | REMOVE | - |
| (4000,0010) | Arbitrary | REMOVE | - |
| (4000,4000) | Text Comments | REMOVE | - |
| (4008,0040) | Results ID | REMOVE | - |
| (4008,0042) | Results ID Issuer | REMOVE | - |
| (4008,0100) | Interpretation Recorded Date | REMOVE | - |
| (4008,0101) | Interpretation Recorded Time | REMOVE | - |
| (4008,0102) | Interpretation Recorder | REMOVE | - |
| (4008,0108) | Interpretation Transcription Date | REMOVE | - |
| (4008,0109) | Interpretation Transcription Time | REMOVE | - |
| (4008,010A) | Interpretation Transcriber | REMOVE | - |
| (4008,010B) | Interpretation Text | REMOVE | - |
| (4008,010C) | Interpretation Author | REMOVE | - |
| (4008,0111) | Interpretation Approver Sequence | REMOVE | - |
| (4008,0112) | Interpretation Approval Date | REMOVE | - |
| (4008,0113) | Interpretation Approval Time | REMOVE | - |
| (4008,0114) | Physician Approving Interpretation | REMOVE | - |
| (4008,0115) | Interpretation Diagnosis Description | REMOVE | - |
| (4008,0118) | Results Distribution List Sequence | REMOVE | - |
| (4008,0119) | Distribution Name | REMOVE | - |
| (4008,011A) | Distribution Address | REMOVE | - |
| (4008,0200) | Interpretation ID | REMOVE | - |
| (4008,0202) | Interpretation ID Issuer | REMOVE | - |
| (4008,0300) | Impressions | REMOVE | - |
| (4008,4000) | Results Comments | REMOVE | - |
| (50XX,XXXX) | Curve Data | REMOVE with func:is_curve_or_overlay_tag| - |
| (60XX,3000) | Overlay Data | REMOVE with func:is_curve_or_overlay_tag` | - |
| (60XX,4000) | Overlay Comments | REMOVE with func:is_curve_or_overlay_tag` | - |
| (FFFA,FFFA) | Digital Signatures Sequence | REMOVE | - |
| (FFFC,FFFC) | Data Set Trailing Padding | REMOVE | - |

### C.3 Special Recipe Actions

In addition to the tag-specific actions listed above, the recipe includes these global operations:

| Action | Description |
|--------|-------------|
| `REMOVE ALL func:is_tag_private` | Removes all private DICOM tags (odd group numbers) |
| `REMOVE ALL func:is_curve_or_overlay_tag` | Removes all curve data (group 50xx) and overlay data/comments tags (group 60xx and element 3000 and 4000 respectively) |
| `ADD PatientIdentityRemoved YES` | Adds tag (0012,0062) with value "YES" |
| `ADD DeidentificationMethod LUWAK_ANONYMIZER` | Adds tag (0012,0063) with value "LUWAK_ANONYMIZER" |
| `ADD LongitudinalTemporalInformationModified REMOVED` | Adds tag (0028,0303) with value "REMOVED" |

### C.4 Replacement Value Definitions

| Replacement Value | Description |
|-------------------|-------------|
| `func:generate_hmacuid` | HMAC-based deterministic UID generation (see [§5.3.1](#531-uid-generation-generate_hmacuid)) |
| `func:set_fixed_datetime` | Fixed epoch datetime values (see [§5.3.8](#538-fixed-datetime-funcset_fixed_datetime)) |
| `func:generate_patient_id` | Deterministic patient ID generation (see [§5.3.2](#532-patient-id-generation-generate_patient_id)) |
| `ANONYMIZED` | Static replacement string "ANONYMIZED" |
| `Anonymized^Anonymized` | DICOM Person Name format with "Anonymized" for family and given names |
| `000D` | Fixed Age String value (13 days) |
| `YES` | Boolean affirmative value |
| `-` (blank) | No replacement value (tag is removed or blanked) |

### C.5 Notes

- This table represents the Basic Application Confidentiality Profile without additional retention options
- When combined with retention options (UIDs, dates, device identity, etc.), some actions may be overridden (see [§5.4](#54-profileoptions-description))
- Actions marked with `#REPLACE (tag) SEQUENCE NEEDS REVIEW` are commented out in the recipe and require manual review
- The actual recipe file is generated by `anonymization_recipe_builder.py` (see [§6](#6-deidentification-recipe-creation-pipeline-stage-4---5))

