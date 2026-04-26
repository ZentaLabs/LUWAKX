#!/usr/bin/env python3
"""Walk a directory tree of DICOM studies or NIfTI files and produce a single PDF overview.

Finds every series-level folder (directory containing .dcm files) or NIfTI file
(.nii / .nii.gz), renders it with renderer.py, and combines all results into one
scrollable PDF.

Usage:
    python batch_render.py /path/to/dicoms /path/to/output_dir
    python batch_render.py /path/to/niftis /path/to/output_dir
    python batch_render.py /path/to/input /path/to/output_dir --workers 4
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

# Make renderer importable from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from renderer import detect_modality, check_transfer_syntax, is_nifti, NIFTI_EXTENSIONS

RENDERER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "renderer.py")


def find_inputs(root: str) -> list[dict]:
    """Return a list of render-able inputs found under *root*.

    Each entry is a dict with keys:
        path  - DICOM series directory or NIfTI file path
        type  - "dicom" or "nifti"
    """
    inputs = []

    for dirpath, dirnames, filenames in os.walk(root):
        # Skip hidden directories in-place so os.walk doesn't descend into them
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        # Skip hidden files
        filenames = [f for f in filenames if not f.startswith(".")]

        # Check for NIfTI files
        for f in filenames:
            if any(f.lower().endswith(ext) for ext in NIFTI_EXTENSIONS):
                inputs.append({
                    "path": os.path.join(dirpath, f),
                    "type": "nifti",
                })

        # Check for DICOM series (directory containing .dcm files)
        if any(f.endswith(".dcm") for f in filenames):
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


def _process_input(
    index: int,
    total: int,
    inp: dict,
    root: str,
    tmpdir: str,
) -> dict:
    """Process a single input: pre-check, then render via subprocess.

    Returns a result dict with keys:
        index, label, modality, pdf_path, input_path, skipped, skip_reason
    """
    input_path = inp["path"]
    input_type = inp["type"]
    label = build_label(input_path, root)
    result = {
        "index": index,
        "label": label,
        "modality": None,
        "pdf_path": None,
        "input_path": input_path,
        "skipped": False,
        "skip_reason": None,
    }

    if input_type == "nifti":
        modality = "CT"
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
    series_pdf = os.path.join(tmpdir, f"series_{index:04d}.pdf")
    cmd = [sys.executable, RENDERER_SCRIPT, input_path, "-o", series_pdf]
    cmd += ["--modality", modality]
    cmd += ["--label", label]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        result["skipped"] = True
        result["skip_reason"] = proc.stderr.strip()
        return result

    if os.path.exists(series_pdf):
        result["pdf_path"] = series_pdf

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Batch volume-render DICOM series or NIfTI files into a single PDF."
    )
    parser.add_argument("input_dir", help="Root directory containing DICOM studies or NIfTI files. "
                        "NIfTI files are always rendered as CT (use renderer.py directly for PET).")
    parser.add_argument("output_dir", help="Directory to write the output PDF into")
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Max concurrent renderer subprocesses (default: 3)",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        sys.exit(f"Input directory not found: {args.input_dir}")

    os.makedirs(args.output_dir, exist_ok=True)

    # Discover all inputs
    inputs = find_inputs(args.input_dir)
    if not inputs:
        sys.exit(f"No DICOM series or NIfTI files found under {args.input_dir}")

    total = len(inputs)
    n_nifti = sum(1 for i in inputs if i["type"] == "nifti")
    n_dicom = sum(1 for i in inputs if i["type"] == "dicom")
    print(f"Found {total} inputs ({n_dicom} DICOM, {n_nifti} NIfTI, {args.workers} workers)")
    if n_nifti > 0:
        print(f"WARNING: All {n_nifti} NIfTI files will be rendered as CT. "
              "Use renderer.py directly with --modality PT for PET volumes.")
    print()

    # Render inputs in parallel using thread pool
    tmpdir = tempfile.mkdtemp(prefix="batch_render_")
    results: list[dict] = []
    done_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                _process_input, i, total, inp,
                args.input_dir, tmpdir,
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

    per_series_pdfs = [r["pdf_path"] for r in results if r["pdf_path"]]
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

    if not per_series_pdfs:
        shutil.rmtree(tmpdir, ignore_errors=True)
        sys.exit("No inputs were rendered successfully.")

    # Merge all per-series PDFs into one
    output_pdf = os.path.join(args.output_dir, "batch_renders.pdf")
    _merge_pdfs(per_series_pdfs, output_pdf)

    shutil.rmtree(tmpdir, ignore_errors=True)
    print(f"Done - {len(per_series_pdfs)} rendered -> {output_pdf}")


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
