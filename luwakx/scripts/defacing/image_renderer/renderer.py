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

import numpy as np
import pydicom
import SimpleITK as sitk
import vtk
from vtk.util.numpy_support import numpy_to_vtk, vtk_to_numpy


# -- NIfTI/NRRD support ------------------------------------------------------

NIFTI_EXTENSIONS = (".nii", ".nii.gz")
NRRD_EXTENSIONS = (".nrrd",)
VOLUME_EXTENSIONS = NIFTI_EXTENSIONS + NRRD_EXTENSIONS


def is_nifti(path: str) -> bool:
    """Return True if the path looks like a NIfTI file."""
    lower = path.lower()
    return any(lower.endswith(ext) for ext in NIFTI_EXTENSIONS)


def is_nrrd(path: str) -> bool:
    """Return True if the path looks like an NRRD file."""
    lower = path.lower()
    return any(lower.endswith(ext) for ext in NRRD_EXTENSIONS)


def is_volume_file(path: str) -> bool:
    """Return True if the path looks like a supported single-volume file."""
    lower = path.lower()
    return any(lower.endswith(ext) for ext in VOLUME_EXTENSIONS)


def load_nifti(path: str) -> vtk.vtkImageData:
    """Load a NIfTI file via VTK's own reader (handles orientation)."""
    reader = vtk.vtkNIFTIImageReader()
    reader.SetFileName(path)
    reader.Update()
    return reader.GetOutput()


def load_nrrd(path: str) -> vtk.vtkImageData:
    """Load an NRRD file via VTK's own reader."""
    if not hasattr(vtk, "vtkNrrdReader"):
        raise RuntimeError("This VTK build does not include vtkNrrdReader")

    reader = vtk.vtkNrrdReader()
    reader.SetFileName(path)
    reader.Update()
    return reader.GetOutput()


def _load_volume_file(path: str) -> vtk.vtkImageData:
    """Load a standalone volume file (NIfTI or NRRD) via SimpleITK."""
    if not is_volume_file(path):
        raise RuntimeError(f"Unsupported volume file extension: {path}")

    image = sitk.ReadImage(path)
    return _sitk_image_to_vtk(image)


def _load_volume_file_with_matrix(path: str) -> tuple[vtk.vtkImageData, vtk.vtkMatrix4x4]:
    """Load a standalone volume file and return image data plus LPS transform."""
    if not is_volume_file(path):
        raise RuntimeError(f"Unsupported volume file extension: {path}")

    image = sitk.ReadImage(path)
    return _sitk_image_to_vtk(image), _build_lps_patient_matrix(image)


def _load_volume_file_vtk(path: str) -> vtk.vtkImageData:
    """Legacy VTK-based loader kept for reference/troubleshooting."""
    if is_nifti(path):
        return load_nifti(path)
    if is_nrrd(path):
        return load_nrrd(path)
    raise RuntimeError(f"Unsupported volume file extension: {path}")


def _sitk_image_to_vtk(image: sitk.Image) -> vtk.vtkImageData:
    """Convert a SimpleITK scalar image (z, y, x) to vtkImageData (x, y, z)."""
    arr = sitk.GetArrayFromImage(image)
    if arr.ndim != 3:
        raise RuntimeError(f"Expected a 3D volume from DICOM, got shape {arr.shape}")

    z, y, x = arr.shape
    vtk_arr = numpy_to_vtk(num_array=np.ascontiguousarray(arr).ravel(order="C"), deep=True)

    image_data = vtk.vtkImageData()
    image_data.SetDimensions(x, y, z)
    image_data.SetExtent(0, x - 1, 0, y - 1, 0, z - 1)
    image_data.SetSpacing(image.GetSpacing())
    image_data.SetOrigin(0.0, 0.0, 0.0)
    image_data.GetPointData().SetScalars(vtk_arr)
    return image_data


def _build_lps_patient_matrix(image: sitk.Image) -> vtk.vtkMatrix4x4:
    """Build a voxel-to-LPS transform matrix from a SimpleITK image."""
    direction = image.GetDirection()  # row-major 3x3
    origin = image.GetOrigin()

    matrix = vtk.vtkMatrix4x4()
    matrix.Identity()
    for r in range(3):
        for c in range(3):
            matrix.SetElement(r, c, float(direction[r * 3 + c]))
        matrix.SetElement(r, 3, float(origin[r]))
    return matrix


def load_dicom_with_simpleitk(dicom_dir: str) -> tuple[vtk.vtkImageData, vtk.vtkMatrix4x4]:
    """Load a DICOM series with SimpleITK and return VTK image data and LPS matrix."""
    series_ids = sitk.ImageSeriesReader.GetGDCMSeriesIDs(dicom_dir)
    if not series_ids:
        raise RuntimeError(f"No DICOM series found in directory: {dicom_dir}")

    file_names = sitk.ImageSeriesReader.GetGDCMSeriesFileNames(dicom_dir, series_ids[0])
    if not file_names:
        raise RuntimeError(f"No DICOM files found for series in directory: {dicom_dir}")

    reader = sitk.ImageSeriesReader()
    reader.SetFileNames(file_names)
    image = reader.Execute()

    image_data = _sitk_image_to_vtk(image)
    patient_matrix = _build_lps_patient_matrix(image)
    return image_data, patient_matrix


def infer_modality_from_image(image_data: vtk.vtkImageData) -> tuple[str, str]:
    """Infer CT/PT from robust intensity-distribution heuristics.

    Uses percentiles so a few hot voxels do not dominate the decision.
    """
    lo, hi = image_data.GetScalarRange()
    if lo < -200:
        return "CT", f"min {lo:.2f} < -200 (HU-like negative tail)"

    scalars = image_data.GetPointData().GetScalars()
    if scalars is None:
        return "CT", "no scalar buffer available; defaulting to CT"

    vox = vtk_to_numpy(scalars)
    if vox.size == 0:
        return "CT", "empty scalar buffer; defaulting to CT"

    # Keep percentile computation fast for large volumes.
    if vox.size > 1_000_000:
        stride = int(np.ceil(vox.size / 1_000_000))
        vox = vox[::stride]

    p50 = float(np.percentile(vox, 50))
    p75 = float(np.percentile(vox, 75))
    p90 = float(np.percentile(vox, 90))
    p95 = float(np.percentile(vox, 95))
    p99 = float(np.percentile(vox, 99))
    frac_gt200 = float(np.mean(vox > 200))

    # Near-zero non-negative volumes are typically not PET activity maps.
    if lo >= 0 and hi < 1.0 and p99 < 0.01:
        return "CT", f"near-zero non-negative distribution (max={hi:.4f}, p99={p99:.4f})"

    # PET-like pattern: non-negative background-dominant distribution with hot uptake tail.
    if lo >= 0 and p75 < 150 and p95 > 300:
        return "PT", f"non-negative with low p75={p75:.2f} and hot-tail p95={p95:.2f}"

    # Lower-uptake PET can have a softer tail; require a low-intermediate distribution.
    if lo >= 0 and p75 < 80 and p90 < 150 and p95 > 200:
        return "PT", (
            f"non-negative with low/intermediate percentiles "
            f"(p75={p75:.2f}, p90={p90:.2f}) and elevated p95={p95:.2f}"
        )

    # Borderline PET: still background-dominant but with milder uptake tail.
    if lo >= 0 and p75 < 40 and p90 < 180 and p95 > 170:
        return "PT", (
            f"non-negative with very low p75={p75:.2f}, moderate p90={p90:.2f}, "
            f"and PET-like tail p95={p95:.2f}"
        )

    # PET-like when a meaningful fraction of voxels are >200 even if percentile tails are softer.
    if lo >= 0 and p90 > 50 and frac_gt200 > 0.015:
        return "PT", (
            f"non-negative with p90={p90:.2f} and >200 voxel fraction={frac_gt200:.3f}"
        )

    # PET can also present sparse but very hot uptake.
    if lo >= 0 and p50 < 10 and p90 > 500:
        return "PT", f"sparse non-negative distribution with low p50={p50:.2f} and high p90={p90:.2f}"

    # Typical PET SUV-like scale without extreme tail, excluding near-zero volumes.
    if lo >= 0 and hi <= 80 and p95 > 1.0:
        return "PT", f"range [{lo:.2f}, {hi:.2f}] is non-negative and p95={p95:.2f} > 1"

    # Unsigned/shifted CT often has high central percentiles.
    if lo >= 0 and hi > 1200 and p50 > 300:
        return "CT", f"high dynamic range with elevated p50={p50:.2f}"

    return "CT", (
        f"distribution ambiguous (p50={p50:.2f}, p75={p75:.2f}, p90={p90:.2f}, "
        f"p95={p95:.2f}, p99={p99:.2f}, frac>200={frac_gt200:.3f}); defaulting to CT"
    )


def detect_volume_modality(input_path: str) -> str:
    """Auto-detect modality for standalone NIfTI/NRRD using intensity distribution."""
    image_data = _load_volume_file(input_path)
    scalar_range = image_data.GetScalarRange()
    modality, reason = infer_modality_from_image(image_data)
    print(
        f"Detected volume modality from intensity distribution: {modality} "
        f"(range={scalar_range}, reason={reason})"
    )
    return modality


# -- Default transfer function ------------------------------------------------

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


# -- Views --------------------------------------------------------------------

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

SINGLE_PAGE_VIEW_ORDER = [
    "left",
    "oblique_left_30",
    "front",
    "oblique_right_30",
    "right",
    "oblique_left_above",
    "front_below",
    "front_above",
    "oblique_right_above",
    "back",
]


def _label_from_png_path(path: str) -> str:
    """Extract view label from renderer output filename (e.g. 01_front -> front)."""
    base = os.path.splitext(os.path.basename(path))[0]
    return "_".join(base.split("_")[1:])


def _reorder_single_page_paths(png_paths: list[str]) -> list[str]:
    """Return PNG paths ordered for single-page grid presentation."""
    by_label = {_label_from_png_path(p): p for p in png_paths}
    ordered = [by_label[label] for label in SINGLE_PAGE_VIEW_ORDER if label in by_label]

    # Preserve any unexpected paths by appending them after known slots.
    seen = set(ordered)
    ordered.extend([p for p in png_paths if p not in seen])
    return ordered


# -- Rendering ----------------------------------------------------------------


def render_views(input_path: str, modality: str) -> list[str]:
    """Render all views and return list of temp PNG paths."""

    patient_matrix = None
    if is_volume_file(input_path):
        image_data, patient_matrix = _load_volume_file_with_matrix(input_path)
    else:
        image_data, patient_matrix = load_dicom_with_simpleitk(input_path)

    scalar_range = image_data.GetScalarRange()
    print(
        f"Volume: {image_data.GetDimensions()}, "
        f"spacing: {image_data.GetSpacing()} mm, "
        f"range: {scalar_range}"
    )

    if modality == "PT":
        resolved_tf = DEFAULT_PET_TF
        slope, intercept, units = get_pet_rescale(input_path) if not is_volume_file(input_path) else (1.0, 0.0, "BQML")
        bqml_lo = scalar_range[0] * slope + intercept
        bqml_hi = scalar_range[1] * slope + intercept
        print(
            f"PET rescale: slope={slope}, intercept={intercept}, units={units}\n"
            f"Bq/ml range: {bqml_lo:.1f} - {bqml_hi:.1f}"
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
    if patient_matrix is not None:
        volume.SetUserMatrix(patient_matrix)

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
            cy - cam_dist * math.cos(az) * math.cos(el),
            cz + cam_dist * math.sin(el),
        )
        # In patient LPS space, keep superior (+Z) as up.
        cam.SetViewUp(0, 0, 1)
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

        print(f"  [{i + 1}/{len(VIEWS)}] {label} (az={az_deg} deg, el={el_deg} deg)")

    return png_paths


def save_pdf(
    png_paths: list[str],
    output_path: str,
    name: str,
    modality: str,
    single_page: bool = False,
):
    """Combine PNG renders into a PDF.

    Default layout is 2-up pages. With single_page=True, all views for one volume
    are arranged on a single page.
    """
    from matplotlib.backends.backend_pdf import PdfPages
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg

    ordered_paths = _reorder_single_page_paths(png_paths) if single_page else png_paths

    if single_page:
        cols = 5
        rows = math.ceil(len(ordered_paths) / cols)
        per_page = len(ordered_paths)
        fig_size = (cols * 3.2, rows * 3.2 + 1.0)
    else:
        cols, rows = 2, 1
        per_page = cols * rows
        fig_size = (16, 9)
    header = f"{name}  -  {modality}"

    with PdfPages(output_path) as pdf:
        for page_start in range(0, len(ordered_paths), per_page):
            batch = ordered_paths[page_start : page_start + per_page]
            fig, axes = plt.subplots(rows, cols, figsize=fig_size)
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
                    pretty_label = label.replace("_", " ")
                    if single_page and (j // cols) == (rows - 1):
                        ax.text(
                            0.5,
                            -0.05,
                            pretty_label,
                            transform=ax.transAxes,
                            ha="center",
                            va="top",
                            fontsize=13,
                            color="white",
                            clip_on=False,
                        )
                    else:
                        ax.set_title(
                            pretty_label, fontsize=14, color="white", pad=8
                        )
                ax.axis("off")

            fig.suptitle(header, fontsize=18, color="white", fontweight="bold", y=0.98)
            fig.patch.set_facecolor("#1a1a26")
            fig.tight_layout(pad=1.0, rect=[0, 0.02, 1, 0.94])
            if single_page:
                fig.subplots_adjust(hspace=0.16)
            pdf.savefig(fig, facecolor=fig.get_facecolor())
            plt.close(fig)

    print(f"\nPDF saved: {output_path}")


def save_jpg(
    png_paths: list[str],
    output_path: str,
    name: str,
    modality: str,
    single_page: bool = False,
):
    """Save renders as JPEG with medium quality for better compression ratio.

    - single_page=False: save one JPEG per viewpoint.
    - single_page=True: save all viewpoints in one composed JPEG.
    """
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg
    from PIL import Image

    ordered_paths = _reorder_single_page_paths(png_paths) if single_page else png_paths

    if single_page:
        cols = 5
        rows = math.ceil(len(ordered_paths) / cols)
        fig_size = (cols * 3.2, rows * 3.2 + 1.0)
        header = f"{name}  -  {modality}"

        root, ext = os.path.splitext(output_path)
        single_jpg_path = output_path if ext.lower() in (".jpg", ".jpeg") else f"{output_path}.jpg"

        fig, axes = plt.subplots(rows, cols, figsize=fig_size)
        if rows == 1 and cols == 1:
            axes = [axes]
        elif rows == 1 or cols == 1:
            axes = list(axes)
        else:
            axes = [ax for row in axes for ax in row]

        for j, ax in enumerate(axes):
            if j < len(ordered_paths):
                img = mpimg.imread(ordered_paths[j])
                ax.imshow(img)
                label = os.path.splitext(os.path.basename(ordered_paths[j]))[0]
                label = "_".join(label.split("_")[1:])
                pretty_label = label.replace("_", " ")
                if (j // cols) == (rows - 1):
                    ax.text(
                        0.5,
                        -0.05,
                        pretty_label,
                        transform=ax.transAxes,
                        ha="center",
                        va="top",
                        fontsize=12,
                        color="white",
                        clip_on=False,
                    )
                else:
                    ax.set_title(pretty_label, fontsize=12, color="white", pad=6)
            ax.axis("off")

        fig.suptitle(header, fontsize=18, color="white", fontweight="bold", y=0.98)
        fig.patch.set_facecolor("#1a1a26")
        fig.tight_layout(pad=1.0, rect=[0, 0.02, 1, 0.94])
        fig.subplots_adjust(hspace=0.16)
        fig.savefig(
            single_jpg_path,
            format="jpg",
            facecolor=fig.get_facecolor(),
            dpi=150,
            pil_kwargs={"quality": 75, "optimize": True},
        )
        plt.close(fig)
        print(f"\nJPEG saved: {single_jpg_path}")
        return

    base, _ = os.path.splitext(output_path)
    out_dir = os.path.dirname(output_path) or "."
    os.makedirs(out_dir, exist_ok=True)

    for path in png_paths:
        label = os.path.splitext(os.path.basename(path))[0]
        label = "_".join(label.split("_")[1:])
        jpg_path = f"{base}_{label}.jpg"
        with Image.open(path) as im:
            rgb = im.convert("RGB")
            rgb.save(jpg_path, format="JPEG", quality=75, optimize=True)
        print(f"JPEG saved: {jpg_path}")


# -- CLI ----------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Volume-render DICOM CT/PET or NIfTI/NRRD volumes."
    )
    parser.add_argument("input", help="Path to DICOM directory or volume file (.nii/.nii.gz/.nrrd)")
    parser.add_argument(
        "-o",
        "--output",
        default="renders.pdf",
        help="Output path (default: renders.pdf)",
    )
    parser.add_argument(
        "--output-format",
        default="pdf",
        choices=["pdf", "jpg"],
        help="Output format: pdf (default) or jpg.",
    )
    parser.add_argument(
        "--modality",
        default=None,
        choices=["CT", "PT"],
        help="Override modality (auto-detected if omitted). Use PT for PET.",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Label shown in the PDF header (default: last 3 path components)",
    )
    parser.add_argument(
        "--single-page",
        action="store_true",
        help="Render all views for this volume on a single page/image.",
    )
    args = parser.parse_args()

    input_path = args.input
    volume_mode = is_volume_file(input_path)

    if volume_mode:
        if not os.path.isfile(input_path):
            print(f"Volume file not found: {input_path}", file=sys.stderr)
            sys.exit(1)
        modality = args.modality or detect_volume_modality(input_path)
        print(f"Volume input: {input_path}  (modality: {modality})")
    else:
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
    if args.output_format == "pdf":
        save_pdf(png_paths, args.output, name, modality, single_page=args.single_page)
    else:
        save_jpg(png_paths, args.output, name, modality, single_page=args.single_page)

    shutil.rmtree(os.path.dirname(png_paths[0]), ignore_errors=True)


if __name__ == "__main__":
    main()
