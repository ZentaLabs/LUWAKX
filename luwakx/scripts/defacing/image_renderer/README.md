# Volume renderer for validating defaced images

Volume-render DICOM (CT/PET) and NIfTI medical images with VTK. Produces multi-angle PDF overviews.

## Scripts

### `renderer.py`

Render a single volume from 10 predefined camera angles (front, back, left, right, obliques, above, below) and combine them into a PDF.

**Input formats:**
- DICOM directory (CT or PET) — uncompressed transfer syntax only
- NIfTI file (`.nii` / `.nii.gz`)

**Usage:**
```bash
# DICOM — modality auto-detected
python renderer.py /path/to/dicom_dir

# NIfTI — modality required (CT or PT)
python renderer.py scan.nii.gz --modality CT
python renderer.py pet_scan.nii.gz --modality PT

# Override auto-detected DICOM modality
python renderer.py /path/to/dicom_dir --modality PT

# Custom output path and PDF header label
python renderer.py /path/to/dicom_dir -o out.pdf --label "Patient 001"
```

**Arguments:**
- `input` — DICOM directory or NIfTI file
- `-o, --output` — output PDF path (default: `renders.pdf`)
- `--modality {CT,PT}` — auto-detected from DICOM, required for NIfTI
- `--label` — header text shown in the PDF (default: last 3 path components)

### `batch_render.py`

Walk a directory tree, render every DICOM series and NIfTI file found, and merge all results into one PDF.

**Usage:**
```bash
python batch_render.py /path/to/inputs /path/to/output_dir
python batch_render.py /path/to/inputs /path/to/output_dir --workers 4
```

**Arguments:**
- `input_dir` — root directory to scan
- `output_dir` — directory for `batch_renders.pdf` and `skipped.log`
- `--workers N` — concurrent renderer subprocesses (default: 3)

**Notes:**
- NIfTI files are always rendered as CT. For PET NIfTI volumes, use `renderer.py` directly with `--modality PT`.
- DICOM series with unsupported (compressed) transfer syntaxes are skipped and listed in `skipped.log`.
- Per-series PDFs are rendered in parallel, then concatenated in directory order.

## Requirements

- Python 3.10+
- `vtk`, `pydicom`, `matplotlib`, `pypdf`
