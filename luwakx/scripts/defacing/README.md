# Medical Image Defacing Tool

This tool defaces medical images (NIfTI or DICOM) by pixelating the face region.
It can either generate a new face mask automatically or use an existing mask that you provide.

---

## Requirements

- Python 3.10+
- `SimpleITK`
- `moosez`

---

## Usage

    python image_defacer.py -i INPUT [-o OUTPUT] [-m MODALITY] [-fm FACE_MASK_PATH] [-sfm]

---

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `-i`, `--input` | ✅ | Path to input image. Can be a **NIfTI file** (`.nii` or `.nii.gz`) or a **DICOM directory**. |
| `-o`, `--output` | ❌ | Path to the output directory. Defaults to the input’s directory. |
| `-m`, `--modality` | ⚠️ Required for NIfTI | Image modality (e.g., `MR`, `CT`). Required when input is NIfTI. Not required for DICOM (extracted from metadata if available). |
| `-fm`, `--face_mask_path` | ❌ | Path to an existing face mask (NIfTI) to use. If provided, no new mask will be generated. |
| `-sfm`, `--save_face_mask` | ❌ | If set (and no `--face_mask_path` was provided), the generated face mask will be saved to the output directory. |

---

## Output Files

- **Defaced image** → `<input_name>_defaced.nii.gz`
- **Face mask (optional)** → `<input_name>_face_mask.nii.gz` (if `--save_face_mask` is set and no mask was provided)

---

## Examples

### Deface a NIfTI file
    python image_defacer.py -i CT.nii.gz -m CT -o ./output

### Deface a DICOM directory
    python image_defacer.py -i ./dicom_series -o ./output

### Use an existing face mask
    python image_defacer.py -i CT.nii.gz -m CT -fm CT_face_mask.nii.gz -o ./output

### Generate and save the face mask
    python image_defacer.py -i CT.nii.gz -m CT -o ./output -sfm

---

## Notes

- For NIfTI inputs, `--modality` **must** be provided (e.g., `MR`, `CT`).
  - The only valid modality so far is `CT`
- For DICOM inputs, the modality will be read from the metadata if present.
