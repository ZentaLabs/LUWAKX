# Volume renderer for validating defaced images

Volume-render DICOM (CT/PET), NIfTI, and NRRD medical images with VTK.
Supports PDF and JPEG outputs, single-page composites, and batch rendering.

## Scripts

### `renderer.py`

Render a single volume from 10 predefined camera angles (front, back, left, right, obliques, above, below).

**Input formats:**
- DICOM directory (CT or PET) - uncompressed transfer syntax only
- NIfTI file (`.nii` / `.nii.gz`)
- NRRD file (`.nrrd`)

#### Usage

```bash
# DICOM — modality auto-detected
python renderer.py /path/to/dicom_dir

# NIfTI/NRRD — modality auto-detected (can still be overridden)
python renderer.py scan.nii.gz
python renderer.py image_defaced.nrrd

# Override auto-detected DICOM modality
python renderer.py /path/to/dicom_dir --modality PT

# Render JPEG instead of PDF
python renderer.py /path/to/dicom_dir -o out.jpg --output-format jpg

# Single-page composite (PDF or JPG)
python renderer.py /path/to/dicom_dir -o out.pdf --single-page
python renderer.py image_defaced.nrrd -o out.jpg --output-format jpg --single-page

# Custom output path and header label
python renderer.py /path/to/dicom_dir -o out.pdf --label "Patient 001"
```

#### Arguments

- `input`: DICOM directory or volume file (`.nii`, `.nii.gz`, `.nrrd`)
- `-o, --output`: output path (default: `renders.pdf`)
- `--output-format {pdf,jpg}`: output format (default: `pdf`)
- `--modality {CT,PT}`: override auto-detected modality
- `--label`: header text shown in output (default: last 3 path components)
- `--single-page`: render all 10 views into one page/image

### `batch_render.py`

Walk a directory tree, render every DICOM series and volume file found into JPG files or a PDF file.
NIfTI/NRRD modality is auto-detected (CT/PT) from voxel intensity distribution heuristics.

#### Usage

```bash
python batch_render.py /path/to/inputs /path/to/output_dir
python batch_render.py /path/to/inputs /path/to/output_dir --workers 4

# Filter inputs by filename glob (repeatable)
python batch_render.py /path/to/inputs /path/to/output_dir --filename-filter "*.nrrd"
python batch_render.py /path/to/inputs /path/to/output_dir --filename-filter "*.nrrd" --filename-filter "*.dcm"

# Single-page JPG output
python batch_render.py /path/to/inputs /path/to/output_dir --output-format jpg --single-page

# Single-page PDF output
python batch_render.py /path/to/inputs /path/to/output_dir --output-format pdf --single-page
```

#### Arguments

- `input_dir`: root directory to scan
- `output_dir`: destination directory for outputs and optional `skipped.log`
- `--workers N`: concurrent renderer subprocesses (default: 3)
- `--filename-filter GLOB`: filename glob filter; can be provided multiple times
- `--single-page`: render each input on one page/image
- `--output-format {pdf,jpg}`: output format (default: `pdf`)

## Requirements

- Python 3.10+
- `vtk`, `pydicom`, `numpy`, `matplotlib`, `pypdf`, `Pillow`
