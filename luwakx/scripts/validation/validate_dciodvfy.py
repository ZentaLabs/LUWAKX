#!/usr/bin/env python3
"""
DICOM IOD validation script using dciodvfy.

Compares dciodvfy output between original and anonymized DICOM series,
identifying errors and warnings in the anonymized data and flagging those
that are new (not present in the original data).

Output files are saved in the same directory as the uid_mapping CSV:
  - dciodvfy_validation.csv : per-series error/warning table
  - dciodvfy_summary.log    : deduplicated list of unique errors/warnings
                              (updated after every series is processed)

Usage:
    python validate_dciodvfy.py \\
        --uid_mapping /path/to/uid_mappings.db \\
        --original_folder /path/to/original_data \\
        --anonymized_folder /path/to/anonymized_data

Arguments:
    --uid_mapping       Path to uid_mappings.db produced by luwak.
    --original_folder   Base directory for original (pre-anonymization) DICOM
                        files. FilePath_original values in the database are
                        resolved relative to this folder.
    --anonymized_folder Base directory for anonymized DICOM files.
                        FilePath_anonymized values in the database are resolved
                        relative to this folder.

Notes:
    - Log files (.log) and NRRD files (.nrrd) are automatically excluded.
    - Only errors/warnings that are new in the anonymized series (not already
      present in the original series) are written to the output CSV.
    - dciodvfy must be installed and available on PATH.
    - pydicom is used to extract Modality and SeriesNumber; if it is not
      installed those fields will be left empty in the output CSV.
"""

import argparse
import csv
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

EXCLUDED_EXTENSIONS = {".log", ".nrrd"}

OUTPUT_CSV_NAME = "dciodvfy_validation.csv"
SUMMARY_LOG_NAME = "dciodvfy_summary.log"

CSV_COLUMNS = [
    "patient_id_original",
    "patient_id_anonymized",
    "study_uid_original",
    "study_uid_anonymized",
    "series_uid_original",
    "series_uid_anonymized",
    "affected_tag",
    "severity",
    "message",
    "modality",
    "series_number",
]


# --------------------------------------------------------------------------- #
# Argument parsing
# --------------------------------------------------------------------------- #

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run dciodvfy on original and anonymized DICOM series and "
            "report errors/warnings, flagging any that are new after anonymization."
        )
    )
    parser.add_argument(
        "--uid_mapping",
        required=True,
        metavar="PATH",
        help="Path to the uid_mappings.db file produced by luwak.",
    )
    parser.add_argument(
        "--original_folder",
        required=True,
        metavar="PATH",
        help=(
            "Base directory for original (pre-anonymization) DICOM files. "
            "FilePath_original values in uid_mappings.db are resolved "
            "relative to this folder."
        ),
    )
    parser.add_argument(
        "--anonymized_folder",
        required=True,
        metavar="PATH",
        help=(
            "Base directory for anonymized DICOM files. "
            "FilePath_anonymized values in uid_mappings.db are resolved "
            "relative to this folder."
        ),
    )
    return parser.parse_args()


# --------------------------------------------------------------------------- #
# Loading and grouping the UID mapping CSV
# --------------------------------------------------------------------------- #

def _resolve_path(rel_or_abs: str, base_folder: str) -> str:
    """Return an absolute path, joining with base_folder when the path is relative."""
    if not rel_or_abs:
        return ""
    p = Path(rel_or_abs)
    if p.is_absolute():
        return str(p)
    if base_folder:
        return str(Path(base_folder) / p)
    return rel_or_abs


def iter_series_mapping(uid_mapping_path: str, original_folder: str, anonymized_folder: str):
    """
    Stream uid_mappings.db one series at a time.

    Rows are fetched ordered by SeriesInstanceUID_original so that all files
    for a series arrive consecutively.  Only one series worth of data is held
    in memory at any point.

    Yields dicts with keys::

        patient_id_original, patient_id_anonymized,
        study_uid_original,  study_uid_anonymized,
        series_uid_original, series_uid_anonymized,
        original_files,      anonymized_files
    """
    conn = sqlite3.connect(f"file:{uid_mapping_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            """
            SELECT
                FilePath_original,
                FilePath_anonymized,
                PatientID_original,
                PatientID_anonymized,
                StudyInstanceUID_original,
                StudyInstanceUID_anonymized,
                SeriesInstanceUID_original,
                SeriesInstanceUID_anonymized
            FROM Instance
            ORDER BY SeriesInstanceUID_original
            """
        )

        current_uid: str = ""
        current: dict = {}

        for row in cursor:
            series_uid = (row["SeriesInstanceUID_original"] or "").strip()
            if not series_uid:
                continue

            if series_uid != current_uid:
                if current:
                    yield current
                current_uid = series_uid
                current = {
                    "patient_id_original":   (row["PatientID_original"]          or "").strip(),
                    "patient_id_anonymized": (row["PatientID_anonymized"]         or "").strip(),
                    "study_uid_original":    (row["StudyInstanceUID_original"]    or "").strip(),
                    "study_uid_anonymized":  (row["StudyInstanceUID_anonymized"]  or "").strip(),
                    "series_uid_original":   series_uid,
                    "series_uid_anonymized": (row["SeriesInstanceUID_anonymized"] or "").strip(),
                    "original_files":        [],
                    "anonymized_files":      [],
                }

            orig_path = _resolve_path((row["FilePath_original"]   or "").strip(), original_folder)
            anon_path = _resolve_path((row["FilePath_anonymized"] or "").strip(), anonymized_folder)
            if orig_path:
                current["original_files"].append(orig_path)
            if anon_path:
                current["anonymized_files"].append(anon_path)

        if current:
            yield current
    finally:
        conn.close()


def count_series(uid_mapping_path: str) -> int:
    """Count distinct SeriesInstanceUID_original values in the database (for progress display)."""
    conn = sqlite3.connect(f"file:{uid_mapping_path}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT SeriesInstanceUID_original) FROM Instance"
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Running dciodvfy
# --------------------------------------------------------------------------- #

import re as _re


def _normalize_tag_key(affected_tag: str) -> str:
    """
    Return a stable comparison key from *affected_tag* by stripping embedded
    DICOM values that differ between original and anonymized data.

    dciodvfy often embeds the actual field value inside its output, e.g.:
      ``(0x0010,0x0010) PN Patient's Name  PN [1] = <LMU000001>``
      ``Value invalid for this VR [DA] = <00010101>``
      ``Bad Sequence number of Items = <0> (1-n ...)``

    After stripping ``= <...>`` patterns the key becomes attribute-specific
    rather than value-specific, so the same structural issue on original and
    anonymized series maps to the same key.
    """
    # Remove " = <value>" patterns (with any surrounding whitespace)
    key = _re.sub(r'\s*=\s*<[^>]*>', '', affected_tag)
    # Collapse any run of whitespace left behind
    return ' '.join(key.split()).strip()


def _is_excluded(path: str) -> bool:
    """Return True for files that should not be passed to dciodvfy."""
    return Path(path).suffix.lower() in EXCLUDED_EXTENSIONS


def _parse_dciodvfy_line(line: str):
    """
    Parse a single line of dciodvfy output.

    Three output formats are handled:

      Format 1 - tag path first:
        ``Error - </ReferencedImageSequence(0008,1140)> - Bad Sequence ... - Module=<...>``
        ``Error - <ReferencedImageSequence(0008,1140)> - Bad Sequence ...``
        (tag element is enclosed in ``<...>`` or ``</...>`` immediately after the severity)

      Format 2 - description first, attribute name second (common default):
        ``Warning - Missing attribute or value ... - Study ID``

      Format B - description only with embedded ``Element=<Name>`` reference:
        ``Error - Bad Sequence number of Items 0 (...) Element=<ReferencedStudySequence> Module=<...>``

      Format 3 - DICOM tag first, then severity (seen on the installed version):
        ``(0x0010,0x0010) PN Patient's Name  - Warning - Value dubious ...``
        ``(0x0019,0x10b1) LO ?  - Warning - Explicit VR doesn't match dict ...``

    Returns ``(severity, affected_tag, message)`` where:
      - severity    : ``"Error"`` or ``"Warning"``
      - affected_tag: the DICOM attribute/tag affected - used as the stable
                      comparison key between original and anonymized series.
                      - Format 1 -> tag name stripped of angle brackets/slash,
                        e.g. ``ReferencedImageSequence(0008,1140)``
                      - Format 2 -> attribute-name segment, e.g. ``Study ID``
                      - Format B -> element name extracted from ``Element=<Name>``,
                        e.g. ``ReferencedStudySequence``
                      - Format 3 -> tag-identity prefix,
                        e.g. ``(0x0010,0x0010) PN Patient's Name``
      - message     : the diagnostic description without the leading severity
                      prefix and without the tag segment (for Format 1).

    Returns ``None`` for informational lines (e.g. IOD name ``CTImage``).
    """
    line = line.strip()
    if not line:
        return None

    low = line.lower()

    # ---- Format 3: "(0xgggg,0xeeee) ..." with " - Warning/Error - " inside ----
    if line.startswith("(0x") or line.startswith("("):
        for keyword, severity in ((" - error - ", "Error"), (" - warning - ", "Warning")):
            idx = low.find(keyword)
            if idx != -1:
                # Everything before " - Warning/Error - " is the tag identity
                affected_tag = line[:idx].strip().rstrip(" -").strip()
                message = line[idx + len(keyword):].strip()
                return severity, affected_tag, message

    # ---- Format 1 / Format 2 / Format B: line starts with "Error" or "Warning" ----
    for keyword, severity in (("error", "Error"), ("warning", "Warning")):
        if low.startswith(keyword):
            rest = line[len(keyword):].lstrip(" -")
            parts = [p.strip() for p in rest.split(" - ")]

            # Format 1: first segment is a tag path enclosed in angle brackets,
            # e.g. "</ReferencedImageSequence(0008,1140)>" or "<PixelData(7fe0,0010)>".
            # Must start with "<" followed by a letter (to exclude bare values like "<0>").
            if parts and _re.match(r'^</?[A-Za-z]', parts[0]):
                # Strip angle brackets and optional leading slash
                affected_tag = parts[0].strip("<>/")
                message = " - ".join(parts[1:]) if len(parts) > 1 else rest

            # Format 2: first segment is the error description, second is the
            # attribute name.
            elif len(parts) >= 2:
                affected_tag = parts[1]
                message = rest

            # Format B: single-segment description with "Element=<Name>" embedded,
            # e.g. "Bad Sequence ... Element=<ReferencedStudySequence> Module=<...>".
            else:
                element_match = _re.search(r'Element\s*=\s*<([^>]+)>', rest)
                if element_match:
                    affected_tag = element_match.group(1)
                else:
                    affected_tag = parts[0] if parts else rest
                message = rest

            return severity, affected_tag, message

    # ---- "E:" / "W:" short format (some dciodvfy versions) ----
    if low.startswith("e:") or low.startswith("e "):
        rest = line[2:].strip()
        return "Error", rest, rest
    if low.startswith("w:") or low.startswith("w "):
        rest = line[2:].strip()
        return "Warning", rest, rest

    return None


def run_dciodvfy_on_files(file_paths: list) -> list:
    """
    Run dciodvfy on each file in *file_paths* (skipping excluded extensions).

    Returns a list of ``(severity, error_type, full_message)`` tuples.

    Raises ``SystemExit`` if dciodvfy is not found on PATH.
    """
    issues = []
    for fpath in file_paths:
        if not os.path.isfile(fpath):
            print(f"  [SKIP] File not found: {fpath}", file=sys.stderr)
            continue
        if _is_excluded(fpath):
            continue

        try:
            result = subprocess.run(
                ["dciodvfy", fpath],
                capture_output=True,
                text=True,
                timeout=120,
            )
        except FileNotFoundError:
            print(
                "ERROR: 'dciodvfy' not found. "
                "Please install dciodvfy and make sure it is on PATH.",
                file=sys.stderr,
            )
            sys.exit(1)
        except subprocess.TimeoutExpired:
            print(f"  [WARN] dciodvfy timed out on: {fpath}", file=sys.stderr)
            continue

        # dciodvfy writes diagnostics to stderr; informational output to stdout
        combined = result.stdout + result.stderr
        for line in combined.splitlines():
            parsed = _parse_dciodvfy_line(line)
            if parsed is not None:
                issues.append(parsed)

    return issues


# --------------------------------------------------------------------------- #
# DICOM metadata (modality, series number)
# --------------------------------------------------------------------------- #

def get_series_metadata(file_paths: list) -> tuple:
    """
    Return ``(modality, series_number)`` extracted from the first readable file.

    Falls back to empty strings if pydicom is not installed or no file is readable.
    """
    try:
        import pydicom  # noqa: PLC0415
    except ImportError:
        return "", ""

    for fpath in file_paths:
        if not os.path.isfile(fpath) or _is_excluded(fpath):
            continue
        try:
            ds = pydicom.dcmread(fpath, stop_before_pixels=True)
            modality = str(getattr(ds, "Modality", "") or "")
            series_number = str(getattr(ds, "SeriesNumber", "") or "")
            return modality, series_number
        except Exception:
            continue
    return "", ""


# --------------------------------------------------------------------------- #
# Summary log helpers
# --------------------------------------------------------------------------- #

def write_summary_log(log_path: str, unique_issues: dict) -> None:
    """
    Overwrite the summary log with the current set of unique new issues.

    The log contains only error/warning information - no patient or series
    identifiers - organised into ERRORS and WARNINGS sections.
    Each entry shows:
      - The affected DICOM attribute / tag
      - One example message from the anonymized series
      - A note about what was found in the original series for that tag,
        to help diagnose false positives caused by path-resolution problems.
    Entries are deduplicated by (severity, affected_tag).
    """
    errors   = sorted((tag, info) for (sev, tag), info in unique_issues.items() if sev == "Error")
    warnings = sorted((tag, info) for (sev, tag), info in unique_issues.items() if sev == "Warning")

    def _write_section(fh, label, items):
        if items:
            fh.write(f"{label} ({len(items)}):\n")
            fh.write("-" * 40 + "\n")
            for tag, info in items:
                fh.write(f"  Affected tag : {tag}\n")
                fh.write(f"  Example (anonymized) : {info['message']}\n")
                if info['orig_empty']:
                    fh.write(
                        "  Original series      : NO ORIGINAL FILES RESOLVED - "
                        "all findings reported as new; check that --original_folder "
                        "(or --anonymized_folder as fallback) points to the correct root\n"
                    )
                else:
                    fh.write(
                        f"  Original series      : not found "
                        f"(original had {info['orig_total']} issue(s) total)\n"
                    )
                fh.write("\n")
        else:
            fh.write(f"{label}: none\n\n")

    with open(log_path, "w", encoding="utf-8") as fh:
        fh.write("dciodvfy Validation - Unique New Errors and Warnings\n")
        fh.write("=" * 60 + "\n\n")
        _write_section(fh, "ERRORS",   errors)
        _write_section(fh, "WARNINGS", warnings)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> None:
    args = parse_args()

    uid_mapping_path = os.path.abspath(args.uid_mapping)
    if not os.path.isfile(uid_mapping_path):
        print(f"ERROR: uid_mapping database not found: {uid_mapping_path}", file=sys.stderr)
        sys.exit(1)

    anonymized_folder = args.anonymized_folder
    original_folder   = args.original_folder

    output_dir = os.path.dirname(uid_mapping_path)
    output_csv_path = os.path.join(output_dir, OUTPUT_CSV_NAME)
    summary_log_path = os.path.join(output_dir, SUMMARY_LOG_NAME)

    print(f"uid_mapping       : {uid_mapping_path}")
    print(f"anonymized_folder : {anonymized_folder}")
    print(f"original_folder   : {original_folder}")
    print(f"output CSV        : {output_csv_path}")
    print(f"summary log       : {summary_log_path}")
    print()

    # Count series first (single fast pass) so we can show [idx/total] progress.
    print("Counting series in database...")
    total_series = count_series(uid_mapping_path)
    if total_series == 0:
        print("No series found in uid_mapping database. Nothing to validate.")
        sys.exit(0)
    print(f"Series to validate: {total_series}\n")

    # Accumulates unique new issues keyed by (severity, affected_tag).
    # Only distinct structural issue types are kept - never grows beyond a
    # few dozen entries regardless of dataset size.
    unique_issues: dict = {}

    with open(output_csv_path, "w", newline="", encoding="utf-8") as csv_fh:
        writer = csv.DictWriter(csv_fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for idx, info in enumerate(
            iter_series_mapping(uid_mapping_path, original_folder, anonymized_folder),
            start=1,
        ):
            series_uid_orig = info["series_uid_original"]
            print(
                f"[{idx}/{total_series}] Series: {series_uid_orig}"
                f"  ->  {info['series_uid_anonymized']}"
            )

            orig_files = info["original_files"]
            anon_files = info["anonymized_files"]

            # Obtain modality / series number from original files first,
            # then fall back to anonymized files.
            modality, series_number = get_series_metadata(orig_files)
            if not modality:
                modality, series_number = get_series_metadata(anon_files)

            print(
                f"  original files: {len(orig_files)} | "
                f"anonymized files: {len(anon_files)} | "
                f"modality: {modality or '?'} | "
                f"series number: {series_number or '?'}"
            )
            if not orig_files:
                print(
                    "  [WARN] No original files resolved for this series - "
                    "all anonymized findings will be reported as new. "
                    "Check --original_folder and the FilePath_original column in the database.",
                    file=sys.stderr,
                )

            # Run dciodvfy on both sets
            orig_issues  = run_dciodvfy_on_files(orig_files)
            anon_issues  = run_dciodvfy_on_files(anon_files)

            # Build a set of (severity, normalized_tag) keys present in the
            # original series.  The tag is normalized to strip embedded DICOM
            # values (e.g. " = <SmithJohn>") that differ after anonymization,
            # so the same structural issue maps to the same key in both runs.
            # A genuinely new issue for a tag that was clean in the original
            # is still caught because its key won't be in orig_keys.
            orig_keys = {(sev, _normalize_tag_key(tag)) for sev, tag, _ in orig_issues}

            new_in_anon = 0
            for severity, affected_tag, message in anon_issues:
                if (severity, _normalize_tag_key(affected_tag)) in orig_keys:
                    # Same structural issue already existed in the original
                    # series - not introduced by anonymization, skip it.
                    continue

                new_in_anon += 1
                row = {
                    "patient_id_original":   info["patient_id_original"],
                    "patient_id_anonymized": info["patient_id_anonymized"],
                    "study_uid_original":    info["study_uid_original"],
                    "study_uid_anonymized":  info["study_uid_anonymized"],
                    "series_uid_original":   info["series_uid_original"],
                    "series_uid_anonymized": info["series_uid_anonymized"],
                    "affected_tag":          affected_tag,
                    "severity":              severity,
                    "message":               message,
                    "modality":              modality,
                    "series_number":         series_number,
                }
                writer.writerow(row)
                csv_fh.flush()

                # Track unique issues for the summary log
                issue_key = (severity, affected_tag)
                if issue_key not in unique_issues:
                    unique_issues[issue_key] = {
                        "message":    message,
                        "orig_total": len(orig_issues),
                        "orig_empty": len(orig_files) == 0,
                    }
                    # Update the summary log immediately upon each new occurrence
                    write_summary_log(summary_log_path, unique_issues)

            if new_in_anon == 0:
                # Series is clean: write a single informational row so that every
                # series has at least one entry in the output CSV.
                clean_row = {
                    "patient_id_original":   info["patient_id_original"],
                    "patient_id_anonymized": info["patient_id_anonymized"],
                    "study_uid_original":    info["study_uid_original"],
                    "study_uid_anonymized":  info["study_uid_anonymized"],
                    "series_uid_original":   info["series_uid_original"],
                    "series_uid_anonymized": info["series_uid_anonymized"],
                    "affected_tag":          "",
                    "severity":              "No new issues found",
                    "message":               "No new errors or warnings after anonymization",
                    "modality":              modality,
                    "series_number":         series_number,
                }
                writer.writerow(clean_row)
                csv_fh.flush()

            print(
                f"  dciodvfy findings - original: {len(orig_issues)} | "
                f"anonymized: {len(anon_issues)} | "
                f"new after anonymization: {new_in_anon}"
            )

    # ---- Final summary ----
    write_summary_log(summary_log_path, unique_issues)

    total_errors   = sum(1 for (s, _) in unique_issues if s == "Error")
    total_warnings = sum(1 for (s, _) in unique_issues if s == "Warning")

    print()
    print("=" * 60)
    print("Validation complete.")
    print(f"  Unique errors   : {total_errors}")
    print(f"  Unique warnings : {total_warnings}")
    print(f"  Results CSV     : {output_csv_path}")
    print(f"  Summary log     : {summary_log_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
