#!/usr/bin/env python3
"""Volume render DICOM CT or PET images, or NIfTI volumes, with VTK from multiple angles.

Usage:
    python renderer.py <dicom_dir> [--output output.pdf]
    python renderer.py <dicom_dir> --modality PET
    python renderer.py <nifti_file.nii.gz> [--output output.pdf]

"""

import argparse
import glob
import math
import os
import shutil
import sys
import tempfile

import pydicom
import vtk


# ── Transfer syntax support ────────────────────────────────────────────────

# Transfer syntaxes that vtkDICOMImageReader can handle (uncompressed)
VTK_SUPPORTED_TRANSFER_SYNTAXES = {
    "1.2.840.10008.1.2",        # Implicit VR Little Endian
    "1.2.840.10008.1.2.1",      # Explicit VR Little Endian
    "1.2.840.10008.1.2.2",      # Explicit VR Big Endian
}


def check_transfer_syntax(dicom_dir: str) -> tuple[bool, str]:
    """Check if the DICOM files use a transfer syntax supported by VTK.

    Returns (is_supported, transfer_syntax_description).
    """
    dcm_files = glob.glob(os.path.join(dicom_dir, "**", "*"), recursive=True)
    for f in dcm_files:
        if os.path.isdir(f):
            continue
        try:
            ds = pydicom.dcmread(f, stop_before_pixels=True)
            ts_uid = str(ds.file_meta.TransferSyntaxUID)
            ts_name = getattr(ds.file_meta.TransferSyntaxUID, "name", ts_uid)
            if ts_uid in VTK_SUPPORTED_TRANSFER_SYNTAXES:
                return True, ts_name
            return False, ts_name
        except Exception:
            continue
    return False, "unknown (could not read DICOM metadata)"


# ── NIfTI support ───────────────────────────────────────────────────────────

NIFTI_EXTENSIONS = (".nii", ".nii.gz")


def is_nifti(path: str) -> bool:
    """Return True if the path looks like a NIfTI file."""
    lower = path.lower()
    return any(lower.endswith(ext) for ext in NIFTI_EXTENSIONS)


def load_nifti(path: str) -> vtk.vtkImageData:
    """Load a NIfTI file via VTK's own reader (handles orientation)."""
    reader = vtk.vtkNIFTIImageReader()
    reader.SetFileName(path)
    reader.Update()
    return reader.GetOutput()


# ── Default transfer function ────────────────────────────────────────────────

DEFAULT_CT_TF = {
    "opacity": [
        [-3708.0, 0.0],
        [-616.99, 0.0],
        [-432.9, 0.4464],
        [-244.71, 0.625],
        [2387.0, 0.616],
    ],
    "gradient_opacity": [
        [0.0, 1.0],
        [255.0, 1.0],
    ],
    "color": [
        [-3708.0, [0.0, 0.0, 0.0]],
        [-616.99, [0.549, 0.251, 0.149]],
        [-432.9, [0.882, 0.604, 0.29]],
        [-244.71, [1.0, 0.937, 0.955]],
        [2387.0, [0.827, 0.659, 1.0]],
    ],
}

DEFAULT_PET_TF = {
    "opacity": [
        [-2984.0, 0.0],
        [183.556, 0.0],
        [206.222, 0.686],
        [254.389, 0.696],
        [459.736, 0.833],
        [3111.0, 0.804],
    ],
    "gradient_opacity": [
        [0.0, 1.0],
        [255.0, 1.0],
    ],
    "color": [
        [-2984.0, [0.0, 0.0, 0.0]],
        [183.556, [0.616, 0.357, 0.184]],
        [206.222, [0.882, 0.604, 0.290]],
        [254.389, [1.0, 1.0, 1.0]],
        [459.736, [1.0, 0.937, 0.955]],
        [3111.0, [0.827, 0.659, 1.0]],
    ],
}


def detect_modality(dicom_dir: str) -> str:
    """Read the Modality DICOM tag from the first file in the directory."""
    dcm_files = glob.glob(os.path.join(dicom_dir, "**", "*"), recursive=True)
    for f in dcm_files:
        if os.path.isdir(f):
            continue
        try:
            ds = pydicom.dcmread(f, stop_before_pixels=True)
            modality = ds.Modality.upper()
            print(f"Detected DICOM modality: {modality}")
            return modality
        except Exception:
            continue
    raise RuntimeError(f"Could not detect modality from DICOM files in {dicom_dir}")


def get_pet_rescale(dicom_dir: str) -> tuple[float, float, str]:
    """Read rescale slope, intercept and units from a PET DICOM file."""
    dcm_files = glob.glob(os.path.join(dicom_dir, "**", "*"), recursive=True)
    for f in dcm_files:
        if os.path.isdir(f):
            continue
        try:
            ds = pydicom.dcmread(f, stop_before_pixels=True)
            slope = float(getattr(ds, "RescaleSlope", 1.0))
            intercept = float(getattr(ds, "RescaleIntercept", 0.0))
            units = str(getattr(ds, "Units", "BQML"))
            return slope, intercept, units
        except Exception:
            continue
    return 1.0, 0.0, "BQML"


def create_volume_property(tf: dict) -> vtk.vtkVolumeProperty:
    """Build a vtkVolumeProperty from a transfer-function dict."""
    vp = vtk.vtkVolumeProperty()
    vp.SetInterpolationTypeToLinear()

    vp.ShadeOn()
    vp.SetAmbient(0.15)
    vp.SetDiffuse(0.7)
    vp.SetSpecular(0.3)
    vp.SetSpecularPower(15)

    opacity = vtk.vtkPiecewiseFunction()
    for val, alpha in tf["opacity"]:
        opacity.AddPoint(val, alpha)

    gradient_opacity = vtk.vtkPiecewiseFunction()
    for grad, alpha in tf["gradient_opacity"]:
        gradient_opacity.AddPoint(grad, alpha)

    color = vtk.vtkColorTransferFunction()
    for val, rgb in tf["color"]:
        color.AddRGBPoint(val, *rgb)

    vp.SetScalarOpacity(opacity)
    vp.SetGradientOpacity(gradient_opacity)
    vp.SetColor(color)
    return vp


# ── Views ────────────────────────────────────────────────────────────────────

VIEWS = [
    (0, 0, "front"),
    (180, 0, "back"),
    (90, 0, "left"),
    (-90, 0, "right"),
    (30, 0, "oblique_left_30"),
    (-30, 0, "oblique_right_30"),
    (45, 15, "oblique_left_above"),
    (-45, 15, "oblique_right_above"),
    (0, 30, "front_above"),
    (0, -20, "front_below"),
]


# ── Rendering ────────────────────────────────────────────────────────────────


def render_views(input_path: str, modality: str) -> list[str]:
    """Render all views and return list of temp PNG paths."""

    if is_nifti(input_path):
        image_data = load_nifti(input_path)
    else:
        reader = vtk.vtkDICOMImageReader()
        reader.SetDirectoryName(input_path)
        reader.Update()
        image_data = reader.GetOutput()

    scalar_range = image_data.GetScalarRange()
    print(
        f"Volume: {image_data.GetDimensions()}, "
        f"spacing: {image_data.GetSpacing()} mm, "
        f"range: {scalar_range}"
    )

    if modality == "PT":
        resolved_tf = DEFAULT_PET_TF
        slope, intercept, units = get_pet_rescale(input_path) if not is_nifti(input_path) else (1.0, 0.0, "BQML")
        bqml_lo = scalar_range[0] * slope + intercept
        bqml_hi = scalar_range[1] * slope + intercept
        print(
            f"PET rescale: slope={slope}, intercept={intercept}, units={units}\n"
            f"Bq/ml range: {bqml_lo:.1f} – {bqml_hi:.1f}"
        )
    else:
        resolved_tf = DEFAULT_CT_TF

    mapper = vtk.vtkGPUVolumeRayCastMapper()
    mapper.SetInputData(image_data)
    mapper.SetSampleDistance(0.5)
    mapper.SetAutoAdjustSampleDistances(True)

    volume = vtk.vtkVolume()
    volume.SetMapper(mapper)
    volume.SetProperty(create_volume_property(resolved_tf))

    renderer = vtk.vtkRenderer()
    renderer.AddVolume(volume)
    renderer.SetBackground(0.1, 0.1, 0.15)

    win = vtk.vtkRenderWindow()
    win.SetOffScreenRendering(1)
    win.SetSize(1024, 1024)
    win.AddRenderer(renderer)

    bounds = volume.GetBounds()
    cx = (bounds[0] + bounds[1]) / 2
    cy = (bounds[2] + bounds[3]) / 2
    cz = (bounds[4] + bounds[5]) / 2
    max_ext = max(bounds[1] - bounds[0], bounds[3] - bounds[2], bounds[5] - bounds[4])
    cam_dist = max_ext * 1.8

    tmpdir = tempfile.mkdtemp(prefix="vol_render_")
    png_paths = []

    for i, (az_deg, el_deg, label) in enumerate(VIEWS):
        az = math.radians(az_deg)
        el = math.radians(el_deg)

        cam = renderer.GetActiveCamera()
        cam.SetFocalPoint(cx, cy, cz)
        cam.SetPosition(
            cx + cam_dist * math.sin(az) * math.cos(el),
            cy + cam_dist * math.cos(az) * math.cos(el),
            cz + cam_dist * math.sin(el),
        )
        cam.SetViewUp(0, 0, -1)
        cam.SetViewAngle(30)
        renderer.ResetCameraClippingRange()
        win.Render()

        w2i = vtk.vtkWindowToImageFilter()
        w2i.SetInput(win)
        w2i.Update()

        path = os.path.join(tmpdir, f"{i + 1:02d}_{label}.png")
        writer = vtk.vtkPNGWriter()
        writer.SetFileName(path)
        writer.SetInputConnection(w2i.GetOutputPort())
        writer.Write()
        png_paths.append(path)

        print(f"  [{i + 1}/{len(VIEWS)}] {label} (az={az_deg}°, el={el_deg}°)")

    return png_paths


def save_pdf(png_paths: list[str], output_path: str, name: str, modality: str):
    """Combine PNG renders into a single PDF — 2 per page in a grid."""
    from matplotlib.backends.backend_pdf import PdfPages
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg

    cols, rows = 2, 1
    per_page = cols * rows
    header = f"{name}  —  {modality}"

    with PdfPages(output_path) as pdf:
        for page_start in range(0, len(png_paths), per_page):
            batch = png_paths[page_start : page_start + per_page]
            fig, axes = plt.subplots(rows, cols, figsize=(16, 9))
            if per_page == 1:
                axes = [axes]
            elif rows == 1 or cols == 1:
                axes = list(axes)
            else:
                axes = [ax for row in axes for ax in row]

            for j, ax in enumerate(axes):
                if j < len(batch):
                    img = mpimg.imread(batch[j])
                    ax.imshow(img)
                    label = os.path.splitext(os.path.basename(batch[j]))[0]
                    # Strip leading number: "01_front" -> "front"
                    label = "_".join(label.split("_")[1:])
                    ax.set_title(
                        label.replace("_", " "), fontsize=14, color="white", pad=8
                    )
                ax.axis("off")

            fig.suptitle(header, fontsize=18, color="white", fontweight="bold", y=0.98)
            fig.patch.set_facecolor("#1a1a26")
            fig.tight_layout(pad=1.0, rect=[0, 0, 1, 0.94])
            pdf.savefig(fig, facecolor=fig.get_facecolor())
            plt.close(fig)

    print(f"\nPDF saved: {output_path}")


# ── CLI ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Volume-render DICOM CT/PET or NIfTI volumes."
    )
    parser.add_argument("input", help="Path to DICOM directory or NIfTI file (.nii/.nii.gz)")
    parser.add_argument(
        "-o",
        "--output",
        default="renders.pdf",
        help="Output PDF path (default: renders.pdf)",
    )
    parser.add_argument(
        "--modality",
        default=None,
        choices=["CT", "PT"],
        help="Override modality (auto-detected from DICOM if omitted, required for NIfTI). Use PT for PET.",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Label shown in the PDF header (default: last 3 path components)",
    )
    args = parser.parse_args()

    input_path = args.input
    nifti_mode = is_nifti(input_path)

    if nifti_mode:
        if not os.path.isfile(input_path):
            print(f"NIfTI file not found: {input_path}", file=sys.stderr)
            sys.exit(1)
        if not args.modality:
            print("NIfTI input requires --modality (CT or PT).", file=sys.stderr)
            sys.exit(1)
        modality = args.modality
        print(f"NIfTI input: {input_path}  (modality: {modality})")
    else:
        # Check transfer syntax before attempting VTK rendering
        supported, ts_name = check_transfer_syntax(input_path)
        if not supported:
            print(
                f"SKIPPED: {input_path}\n"
                f"  Unsupported transfer syntax: {ts_name}\n"
                f"  vtkDICOMImageReader only supports uncompressed DICOM.",
                file=sys.stderr,
            )
            sys.exit(1)
        modality = args.modality or detect_modality(input_path)

    # Render
    png_paths = render_views(input_path, modality)

    # Save PDF and clean up temp PNGs
    if args.label:
        name = args.label
    else:
        parts = os.path.normpath(input_path).split(os.sep)
        name = os.sep.join(parts[-3:]) if len(parts) >= 3 else os.sep.join(parts)
        name = "./" + name
    save_pdf(png_paths, args.output, name, modality)

    shutil.rmtree(os.path.dirname(png_paths[0]), ignore_errors=True)


if __name__ == "__main__":
    main()
