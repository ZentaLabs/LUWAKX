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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import logging
import time

# Make renderer importable from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import renderer as renderer_module
from renderer import (
    detect_modality,
    detect_volume_modality,
    NIFTI_EXTENSIONS,
    NRRD_EXTENSIONS,
    VIEW_LABELS,
)

RENDERER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "renderer.py")


# -- Logging setup -----------------------------------------------------------

def setup_logging(level: str = "INFO"):
    """Configure logging for batch rendering.
    
    Levels: SILENT, ERROR, WARNING, INFO (default), DEBUG
    SILENT suppresses all logging output.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    if level.upper() == "SILENT":
        logging.disable(logging.CRITICAL)
    else:
        logging.basicConfig(
            level=numeric_level,
            format="%(levelname)s: %(message)s",
            stream=sys.stderr,
        )
    return logging.getLogger(__name__)


logger: logging.Logger | None = None


def emit_progress(message: str):
    """Print progress messages when INFO logging is enabled."""
    if logger is not None and logger.isEnabledFor(logging.INFO):
        print(message, flush=True)


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
    dirs_scanned = 0
    files_seen = 0
    last_update = time.monotonic()

    for dirpath, dirnames, filenames in os.walk(root):
        dirs_scanned += 1
        # Skip hidden directories in-place so os.walk doesn't descend into them
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        # Skip hidden files
        filenames = [f for f in filenames if not f.startswith(".")]
        files_seen += len(filenames)

        now = time.monotonic()
        if now - last_update >= 5:
            emit_progress(
                f"Scanning inputs... {dirs_scanned} directories, {files_seen} files seen"
            )
            last_update = now

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
    renders_dir: str,
    single_page: bool,
    output_format: str,
    modality_method: str,
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

    # Determine final output path first so resume checks can short-circuit
    # before any expensive modality detection or metadata reads.
    base_name = _flatten_relative_path(input_path, root)
    if output_format == "pdf":
        series_output = os.path.join(renders_dir, f"{base_name}.pdf")
    else:
        series_output = os.path.join(output_dir, f"{base_name}.jpg")

    # --- Resume: skip if already fully rendered ---
    if output_format == "pdf":
        if os.path.exists(series_output):
            result["output_paths"] = [series_output]
            result["skip_reason"] = "already rendered (resume)"
            # We still want to include this in the merge, so don't set skipped=True.
            return result
    else:
        if single_page:
            if os.path.exists(series_output):
                result["output_paths"] = [series_output]
                result["skip_reason"] = "already rendered (resume)"
                return result
        else:
            stem = os.path.splitext(series_output)[0]
            existing = [f"{stem}_{label}.jpg" for label in VIEW_LABELS
                        if os.path.exists(f"{stem}_{label}.jpg")]
            if len(existing) == len(VIEW_LABELS):
                result["output_paths"] = sorted(existing)
                result["skip_reason"] = "already rendered (resume)"
                return result

    if input_type == "volume":
        try:
            modality = detect_volume_modality(input_path, method=modality_method)
        except RuntimeError:
            result["skipped"] = True
            result["skip_reason"] = "could not detect modality from volume scalar range"
            return result
        result["modality"] = modality
    else:
        # Detect modality
        try:
            modality = detect_modality(input_path)
        except RuntimeError:
            result["skipped"] = True
            result["skip_reason"] = "could not detect modality"
            return result

        result["modality"] = modality
    cmd = [sys.executable, RENDERER_SCRIPT, input_path, "-o", series_output]
    cmd += ["--modality", modality]
    cmd += ["--label", label]
    cmd += ["--output-format", output_format]
    cmd += ["--modality-method", modality_method]
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
    parser.add_argument(
        "--modality-method",
        default="intensity",
        choices=["intensity", "dicom"],
        help=(
            "How to auto-detect modality for NIfTI/NRRD files. "
            "'intensity' (default): use voxel-intensity heuristics. "
            "'dicom': read Modality tag from a sibling DICOM file (falls back to intensity)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["SILENT", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity (default: INFO). Modality detection only logged at DEBUG level.",
    )
    args = parser.parse_args()
    global logger
    logger = setup_logging(args.log_level)
    renderer_module.logger = logger

    if not os.path.isdir(args.input_dir):
        sys.exit(f"Input directory not found: {args.input_dir}")

    os.makedirs(args.output_dir, exist_ok=True)

    # For PDF output, per-series PDFs are stored in a stable subfolder so
    # interrupted runs can be resumed without re-rendering already-done series.
    renders_dir = os.path.join(args.output_dir, "renders") if args.output_format == "pdf" else None
    if renders_dir:
        os.makedirs(renders_dir, exist_ok=True)

    # Discover all inputs
    emit_progress(f"Scanning input tree under {args.input_dir} for DICOM/NIfTI/NRRD inputs...")
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
    emit_progress(f"Found {total} inputs ({n_dicom} DICOM, {n_vol} NIfTI/NRRD, {args.workers} workers)")
    if args.filename_filter:
        emit_progress(f"Filename filter(s): {', '.join(args.filename_filter)}")
    if args.single_page:
        if args.output_format == "pdf":
            emit_progress("Single-page mode: each volume will be rendered on one PDF page.")
        else:
            emit_progress("Single-page mode: each volume will be rendered into one JPEG image.")
    if n_vol > 0:
        logger.info(f"Standalone volume modality auto-detection enabled for {n_vol} inputs (method: {args.modality_method}).")
    emit_progress("Starting parallel rendering jobs. First completion can take a while depending on volume size.")

    # Render inputs in parallel using thread pool
    results: list[dict] = []
    done_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        pending: set = set()
        input_iter = iter(enumerate(inputs, 1))
        submitted_count = 0

        # Seed initial workers so we can start waiting/reporting immediately.
        for _ in range(args.workers):
            try:
                i, inp = next(input_iter)
            except StopIteration:
                break
            pending.add(
                executor.submit(
                    _process_input, i, total, inp,
                    args.input_dir, args.output_dir, renders_dir,
                    args.single_page, args.output_format, args.modality_method,
                )
            )
            submitted_count += 1

        emit_progress(
            f"Queued {submitted_count}/{total} render jobs "
            f"({len(pending)} active workers)"
        )

        last_wait_update = time.monotonic()
        while pending:
            done, pending = wait(pending, timeout=10, return_when=FIRST_COMPLETED)
            if not done:
                now = time.monotonic()
                if now - last_wait_update >= 30:
                    emit_progress(
                        f"Still working... completed {done_count}/{total}, "
                        f"submitted {submitted_count}/{total}, active {len(pending)}"
                    )
                    last_wait_update = now
                continue

            for future in done:
                r = future.result()
                results.append(r)
                done_count += 1

                if r["skipped"]:
                    emit_progress(f"[done {done_count:>{len(str(total))}}/{total}] "
                                  f"{r['label']} - skipped: {r['skip_reason']}")
                elif r.get("skip_reason") == "already rendered (resume)":
                    emit_progress(f"[done {done_count:>{len(str(total))}}/{total}] "
                                  f"{r['label']} - skipped: already rendered (resume)")
                else:
                    emit_progress(f"[done {done_count:>{len(str(total))}}/{total}] "
                                  f"{r['label']} - rendered ({r['modality']})")

                try:
                    i, inp = next(input_iter)
                except StopIteration:
                    continue

                pending.add(
                    executor.submit(
                        _process_input, i, total, inp,
                        args.input_dir, args.output_dir, renders_dir,
                        args.single_page, args.output_format, args.modality_method,
                    )
                )
                submitted_count += 1

                if submitted_count % max(args.workers * 10, 50) == 0:
                    emit_progress(
                        f"Queued {submitted_count}/{total} render jobs "
                        f"({len(pending)} active workers)"
                    )

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
        sys.exit("No inputs were rendered successfully.")

    if args.output_format == "pdf":
        # Merge all per-series PDFs into one
        output_pdf = os.path.join(args.output_dir, "batch_renders.pdf")
        _merge_pdfs(rendered_outputs, output_pdf)
        n_resumed = sum(1 for r in results
                        if not r["skipped"] and r.get("skip_reason") == "already rendered (resume)")
        n_new = len(rendered_outputs) - n_resumed
        print(f"Done - {n_new} newly rendered, {n_resumed} resumed from cache -> {output_pdf}")
    else:
        n_resumed = sum(1 for r in results
                        if not r["skipped"] and r.get("skip_reason") == "already rendered (resume)")
        n_new = len(rendered_outputs) - n_resumed
        print(f"Done - wrote {len(rendered_outputs)} JPEG files "
              f"({n_new} new, {n_resumed} resumed) -> {args.output_dir}")


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
