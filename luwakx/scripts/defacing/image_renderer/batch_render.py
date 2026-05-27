#!/usr/bin/env python3
"""Walk a directory tree of DICOM studies or volume files and produce outputs.

Finds every series-level folder (directory containing .dcm files) or volume file
(.nii / .nii.gz / .nrrd), renders it with renderer.py, and writes results.

Usage:
    python batch_render.py /path/to/dicoms /path/to/output_dir
    python batch_render.py /path/to/niftis /path/to/output_dir
    python batch_render.py /path/to/input /path/to/output_dir --workers 4
    python batch_render.py /path/to/input /path/to/output_dir --filename-filter "*.dcm"
    python batch_render.py /path/to/input /path/to/output_dir --single-page
    python batch_render.py /path/to/input /path/to/output_dir --output-format jpg
"""

import argparse
import fnmatch
import glob
import os
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

# Make renderer importable from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from renderer import detect_modality, detect_volume_modality, check_transfer_syntax, NIFTI_EXTENSIONS, NRRD_EXTENSIONS

RENDERER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "renderer.py")


def _matches_filename_filters(filename: str, filename_filters: list[str] | None) -> bool:
    """Return True if *filename* matches any provided glob filter.

    Matching is case-insensitive and based on the file basename.
    """
    if not filename_filters:
        return True

    lower_filename = filename.lower()
    return any(fnmatch.fnmatchcase(lower_filename, pattern.lower()) for pattern in filename_filters)


def find_inputs(root: str, filename_filters: list[str] | None = None) -> list[dict]:
    """Return a list of render-able inputs found under *root*.

    Each entry is a dict with keys:
        path  - DICOM series directory or volume file path
        type  - "dicom" or "volume"
    """
    inputs = []

    for dirpath, dirnames, filenames in os.walk(root):
        # Skip hidden directories in-place so os.walk doesn't descend into them
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        # Skip hidden files
        filenames = [f for f in filenames if not f.startswith(".")]

        matching_filenames = [f for f in filenames if _matches_filename_filters(f, filename_filters)]

        # Check for standalone volume files (NIfTI/NRRD)
        for f in matching_filenames:
            if any(f.lower().endswith(ext) for ext in NIFTI_EXTENSIONS + NRRD_EXTENSIONS):
                inputs.append({
                    "path": os.path.join(dirpath, f),
                    "type": "volume",
                })

        # Check for DICOM series (directory containing .dcm files)
        if any(f.lower().endswith(".dcm") for f in matching_filenames):
            inputs.append({
                "path": dirpath,
                "type": "dicom",
            })

    inputs.sort(key=lambda x: x["path"])
    return inputs


def build_label(input_path: str, root: str) -> str:
    """Derive a human-readable label from the path relative to root.

    e.g. root=/data, input=/data/LMU0078/scan.nii.gz
         -> "LMU0078 / scan.nii.gz"
    """
    rel = os.path.relpath(input_path, root)
    return " / ".join(rel.split(os.sep))


def _flatten_relative_path(input_path: str, root: str) -> str:
    """Return an output-safe flattened relative path using '_' separators."""
    rel = os.path.relpath(input_path, root)
    rel_lower = rel.lower()
    for ext in sorted(NIFTI_EXTENSIONS + NRRD_EXTENSIONS, key=len, reverse=True):
        if rel_lower.endswith(ext):
            rel = rel[: -len(ext)]
            break
    return rel.replace(os.sep, "_").replace(" ", "_")


def _process_input(
    index: int,
    total: int,
    inp: dict,
    root: str,
    output_dir: str,
    tmpdir: str | None,
    single_page: bool,
    output_format: str,
) -> dict:
    """Process a single input: pre-check, then render via subprocess.

    Returns a result dict with keys:
        index, label, modality, output_paths, input_path, skipped, skip_reason
    """
    input_path = inp["path"]
    input_type = inp["type"]
    label = build_label(input_path, root)
    result = {
        "index": index,
        "label": label,
        "modality": None,
        "output_paths": [],
        "input_path": input_path,
        "skipped": False,
        "skip_reason": None,
    }

    if input_type == "volume":
        try:
            modality = detect_volume_modality(input_path)
        except RuntimeError:
            result["skipped"] = True
            result["skip_reason"] = "could not detect modality from volume scalar range"
            return result
        result["modality"] = modality
    else:
        # DICOM - check transfer syntax
        supported, ts_name = check_transfer_syntax(input_path)
        if not supported:
            result["skipped"] = True
            result["skip_reason"] = f"unsupported transfer syntax: {ts_name}"
            return result

        # Detect modality
        try:
            modality = detect_modality(input_path)
        except RuntimeError:
            result["skipped"] = True
            result["skip_reason"] = "could not detect modality"
            return result

        result["modality"] = modality

    # Run renderer subprocess
    if output_format == "pdf":
        if tmpdir is None:
            result["skipped"] = True
            result["skip_reason"] = "internal error: missing temp directory for pdf output"
            return result
        series_output = os.path.join(tmpdir, f"series_{index:04d}.pdf")
    else:
        base_name = _flatten_relative_path(input_path, root)
        series_output = os.path.join(output_dir, f"{base_name}.jpg")

    cmd = [sys.executable, RENDERER_SCRIPT, input_path, "-o", series_output]
    cmd += ["--modality", modality]
    cmd += ["--label", label]
    cmd += ["--output-format", output_format]
    if single_page:
        cmd += ["--single-page"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        result["skipped"] = True
        result["skip_reason"] = proc.stderr.strip()
        return result

    if output_format == "pdf":
        if os.path.exists(series_output):
            result["output_paths"] = [series_output]
    else:
        if single_page:
            if os.path.exists(series_output):
                result["output_paths"] = [series_output]
        else:
            stem, _ = os.path.splitext(series_output)
            result["output_paths"] = sorted(glob.glob(f"{stem}_*.jpg"))

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Batch volume-render DICOM series or NIfTI/NRRD files."
    )
    parser.add_argument("input_dir", help="Root directory containing DICOM studies, NIfTI files or NRRD files. "
                        "Standalone volume files are auto-detected as CT/PT from scalar range.")
    parser.add_argument("output_dir", help="Directory to write outputs into")
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Max concurrent renderer subprocesses (default: 3)",
    )
    parser.add_argument(
        "--filename-filter",
        action="append",
        default=None,
        metavar="GLOB",
        help=(
            "Optional filename glob filter (can be passed multiple times), "
            "e.g. --filename-filter '*.dcm', '*.nii.gz', or '*.nrrd'"
        ),
    )
    parser.add_argument(
        "--single-page",
        action="store_true",
        help="Render each volume's views into a single page/image.",
    )
    parser.add_argument(
        "--output-format",
        default="pdf",
        choices=["pdf", "jpg"],
        help="Output format: pdf (default) or jpg.",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        sys.exit(f"Input directory not found: {args.input_dir}")

    os.makedirs(args.output_dir, exist_ok=True)

    # Discover all inputs
    inputs = find_inputs(args.input_dir, args.filename_filter)
    if not inputs:
        if args.filename_filter:
            joined_filters = ", ".join(args.filename_filter)
            sys.exit(
                f"No DICOM series or NIfTI/NRRD files found under {args.input_dir} "
                f"matching filename filter(s): {joined_filters}"
            )
        sys.exit(f"No DICOM series or NIfTI/NRRD files found under {args.input_dir}")

    total = len(inputs)
    n_vol = sum(1 for i in inputs if i["type"] == "volume")
    n_dicom = sum(1 for i in inputs if i["type"] == "dicom")
    print(f"Found {total} inputs ({n_dicom} DICOM, {n_vol} NIfTI/NRRD, {args.workers} workers)")
    if args.filename_filter:
        print(f"Filename filter(s): {', '.join(args.filename_filter)}")
    if args.single_page:
        if args.output_format == "pdf":
            print("Single-page mode: each volume will be rendered on one PDF page.")
        else:
            print("Single-page mode: each volume will be rendered into one JPEG image.")
    if n_vol > 0:
        print(f"Standalone volume modality auto-detection enabled for {n_vol} inputs.")
    print()

    # Render inputs in parallel using thread pool
    tmpdir = tempfile.mkdtemp(prefix="batch_render_") if args.output_format == "pdf" else None
    results: list[dict] = []
    done_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                _process_input, i, total, inp,
                args.input_dir, args.output_dir, tmpdir, args.single_page, args.output_format,
            ): i
            for i, inp in enumerate(inputs, 1)
        }

        for future in as_completed(futures):
            r = future.result()
            results.append(r)
            done_count += 1

            if r["skipped"]:
                print(f"[done {done_count:>{len(str(total))}}/{total}] "
                      f"{r['label']} - skipped: {r['skip_reason']}")
            else:
                print(f"[done {done_count:>{len(str(total))}}/{total}] "
                      f"{r['label']} - rendered ({r['modality']})")

    # Sort by original index to preserve directory order in merged PDF
    results.sort(key=lambda r: r["index"])

    rendered_outputs = [p for r in results for p in r["output_paths"]]
    skipped = [
        (r["input_path"], r["label"], r["skip_reason"])
        for r in results if r["skipped"]
    ]

    # Write skipped inputs log
    if skipped:
        log_path = os.path.join(args.output_dir, "skipped.log")
        with open(log_path, "w") as f:
            f.write(f"Skipped {len(skipped)} inputs out of {total} total\n")
            f.write("=" * 72 + "\n\n")
            for input_path, label, reason in skipped:
                f.write(f"{label}\n  path: {input_path}\n  reason: {reason}\n\n")
        print(f"\n[!] {len(skipped)} inputs skipped - see {log_path}")

    if not rendered_outputs:
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)
        sys.exit("No inputs were rendered successfully.")

    if args.output_format == "pdf":
        # Merge all per-series PDFs into one
        output_pdf = os.path.join(args.output_dir, "batch_renders.pdf")
        _merge_pdfs(rendered_outputs, output_pdf)
        print(f"Done - {len(rendered_outputs)} rendered -> {output_pdf}")
    else:
        print(f"Done - wrote {len(rendered_outputs)} JPEG files -> {args.output_dir}")

    if tmpdir:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _merge_pdfs(pdf_paths: list[str], output_path: str):
    """Concatenate multiple PDFs into one."""
    from pypdf import PdfWriter

    writer = PdfWriter()
    for path in pdf_paths:
        writer.append(path)
    writer.write(output_path)
    writer.close()
    print(f"\nMerged PDF saved: {output_path}")


if __name__ == "__main__":
    main()
