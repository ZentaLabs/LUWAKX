#!/usr/bin/env python3
"""General-purpose lookup of original IDs from deidentified identifiers.

Supports lookups from:
- rendered defacing filenames
- deidentified series folders (reads one companion DICOM)
- anonymized SeriesInstanceUID
- anonymized StudyInstanceUID
- anonymized PatientID

Examples:
        python reidentify.py \
      --db /path/to/privateMapping/uid_mappings.db \
      --deidentified-rendering-filename Patient0001_..._image_defaced.jpg

        python reidentify.py \
      --db /path/to/privateMapping/uid_mappings.db \
      --deidentified-folder /path/to/deidentified/Patient0001/<study>/<series> \
      --series-instance-uid 1.2.826.... \
      --output-format csv
"""

import argparse
import csv
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Optional, Tuple

import pydicom


@dataclass
class QueryInput:
    """One requested lookup item."""

    source: str
    raw_value: str
    anon_patient_id: str = ""
    anon_study_uid: str = ""
    anon_series_uid: str = ""
    error: str = ""


@dataclass
class LookupResult:
    """One original mapping result row."""

    original_patient_id: str
    original_study_uid: str
    original_series_uid: str
    original_file_folder: str


def _flatten_arg_values(values: Optional[list[list[str]]]) -> list[str]:
    """Flatten argparse values collected via action='append', nargs='+'."""
    if not values:
        return []
    flat: list[str] = []
    for chunk in values:
        flat.extend(chunk)
    return flat


def _infer_deidentified_root(db_path: str) -> str:
    """Infer deidentified root from a typical .../privateMapping/uid_mappings.db path."""
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(db_path)))
    return os.path.join(project_dir, "deidentified")


def _extract_patient_and_tail(stem: str) -> Tuple[str, str]:
    """Split '<patient>_<tail>' from a rendered stem name."""
    if "_" not in stem:
        raise ValueError("rendered filename does not contain expected separators")
    patient_anon, tail = stem.split("_", 1)
    if not patient_anon:
        raise ValueError("could not parse anonymized patient token")
    return patient_anon, tail


def _candidate_study_series_pairs(tail: str) -> list[Tuple[str, str]]:
    """Generate possible (study_token, series_token) splits from tail."""
    pairs: list[Tuple[str, str]] = []
    for idx, ch in enumerate(tail):
        if ch != "_":
            continue
        study = tail[:idx]
        series = tail[idx + 1 :]
        if study and series:
            pairs.append((study, series))
    return pairs


def _find_first_dicom(series_dir: str) -> Optional[str]:
    """Return path to first DICOM in folder (non-recursive, then recursive fallback)."""
    if not os.path.isdir(series_dir):
        return None

    for name in sorted(os.listdir(series_dir)):
        path = os.path.join(series_dir, name)
        if os.path.isfile(path) and name.lower().endswith(".dcm"):
            return path

    for dirpath, _, filenames in os.walk(series_dir):
        for name in sorted(filenames):
            if name.lower().endswith(".dcm"):
                return os.path.join(dirpath, name)
    return None


def _read_anonymized_uids_from_dicom(dcm_path: str) -> Tuple[str, str, str]:
    """Read anonymized Patient/Study/Series IDs from one DICOM file."""
    ds = pydicom.dcmread(dcm_path, stop_before_pixels=True)
    anon_patient = str(getattr(ds, "PatientID", "") or "")
    anon_study_uid = str(getattr(ds, "StudyInstanceUID", "") or "")
    anon_series_uid = str(getattr(ds, "SeriesInstanceUID", "") or "")
    return anon_patient, anon_study_uid, anon_series_uid


def _parse_rendering_filename_to_folder_tokens(
    rendered_filename: str,
    deidentified_root: str,
) -> Tuple[str, str, str]:
    """Parse anonymized patient/study/series folder tokens from rendered filename."""
    basename = os.path.basename(rendered_filename)
    stem, _ = os.path.splitext(basename)

    suffix = "_image_defaced"
    if not stem.endswith(suffix):
        raise ValueError(f"expected filename stem to end with '{suffix}', got '{stem}'")

    stem = stem[: -len(suffix)]
    patient_anon, tail = _extract_patient_and_tail(stem)

    candidates = _candidate_study_series_pairs(tail)
    if not candidates:
        raise ValueError("could not parse anonymized study/series tokens")

    matches: list[Tuple[str, str]] = []
    for study_anon, series_anon in candidates:
        series_dir = os.path.join(deidentified_root, patient_anon, study_anon, series_anon)
        if _find_first_dicom(series_dir) is not None:
            matches.append((study_anon, series_anon))

    if len(matches) == 1:
        study_anon, series_anon = matches[0]
        return patient_anon, study_anon, series_anon

    if len(matches) > 1:
        raise ValueError("multiple study/series token candidates matched on disk")

    study_anon, series_anon = tail.rsplit("_", 1)
    series_dir = os.path.join(deidentified_root, patient_anon, study_anon, series_anon)
    if _find_first_dicom(series_dir) is not None:
        return patient_anon, study_anon, series_anon

    raise ValueError("no companion DICOM found for parsed patient/study/series folder")


def _resolve_deidentified_folder(folder: str, deidentified_root: str) -> str:
    """Resolve a deidentified folder path from absolute or common relative forms."""
    folder = folder.rstrip("/\\")
    candidates: list[str] = []

    if os.path.isabs(folder):
        candidates.append(folder)
    else:
        candidates.append(os.path.abspath(folder))
        candidates.append(os.path.join(deidentified_root, folder))

        deidentified_name = "deidentified" + os.sep
        normalized = folder.replace("\\", "/")
        if normalized.startswith("deidentified/"):
            project_dir = os.path.dirname(deidentified_root)
            suffix = normalized[len("deidentified/") :]
            candidates.append(os.path.join(project_dir, "deidentified", suffix))

    for cand in candidates:
        if os.path.isdir(cand):
            return cand

    return candidates[0]


def _results_from_rows(rows: list[sqlite3.Row]) -> list[LookupResult]:
    """Convert and de-duplicate sqlite rows to output records."""
    seen: set[tuple[str, str, str, str]] = set()
    out: list[LookupResult] = []

    for row in rows:
        original_file = row["FilePath_original"] or ""
        folder = os.path.dirname(original_file) if original_file else ""
        key = (
            str(row["PatientID_original"] or ""),
            str(row["StudyInstanceUID_original"] or ""),
            str(row["SeriesInstanceUID_original"] or ""),
            folder,
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(
            LookupResult(
                original_patient_id=key[0],
                original_study_uid=key[1],
                original_series_uid=key[2],
                original_file_folder=key[3],
            )
        )

    return out


def _lookup_for_query(conn: sqlite3.Connection, query: QueryInput) -> list[LookupResult]:
    """Run lookup for one query input, prioritizing series-based matching."""
    rows: list[sqlite3.Row] = []

    if query.anon_series_uid:
        if query.anon_patient_id:
            rows = conn.execute(
                """
                SELECT
                    PatientID_original,
                    StudyInstanceUID_original,
                    SeriesInstanceUID_original,
                    FilePath_original
                FROM Instance
                WHERE SeriesInstanceUID_anonymized = ?
                  AND PatientID_anonymized = ?
                """,
                (query.anon_series_uid, query.anon_patient_id),
            ).fetchall()
            if rows:
                return _results_from_rows(rows)

        rows = conn.execute(
            """
            SELECT
                PatientID_original,
                StudyInstanceUID_original,
                SeriesInstanceUID_original,
                FilePath_original
            FROM Instance
            WHERE SeriesInstanceUID_anonymized = ?
            """,
            (query.anon_series_uid,),
        ).fetchall()
        return _results_from_rows(rows)

    if query.anon_study_uid:
        if query.anon_patient_id:
            rows = conn.execute(
                """
                SELECT
                    PatientID_original,
                    StudyInstanceUID_original,
                    SeriesInstanceUID_original,
                    FilePath_original
                FROM Instance
                WHERE StudyInstanceUID_anonymized = ?
                  AND PatientID_anonymized = ?
                """,
                (query.anon_study_uid, query.anon_patient_id),
            ).fetchall()
            if rows:
                return _results_from_rows(rows)

        rows = conn.execute(
            """
            SELECT
                PatientID_original,
                StudyInstanceUID_original,
                SeriesInstanceUID_original,
                FilePath_original
            FROM Instance
            WHERE StudyInstanceUID_anonymized = ?
            """,
            (query.anon_study_uid,),
        ).fetchall()
        return _results_from_rows(rows)

    if query.anon_patient_id:
        rows = conn.execute(
            """
            SELECT
                PatientID_original,
                StudyInstanceUID_original,
                SeriesInstanceUID_original,
                FilePath_original
            FROM Instance
            WHERE PatientID_anonymized = ?
            """,
            (query.anon_patient_id,),
        ).fetchall()
        return _results_from_rows(rows)

    return []


def _build_queries(args: argparse.Namespace) -> list[QueryInput]:
    """Build QueryInput objects from CLI arguments."""
    queries: list[QueryInput] = []
    deidentified_root = args.deidentified_root or _infer_deidentified_root(args.db)

    for value in _flatten_arg_values(args.deidentified_rendering_filename):
        q = QueryInput(source="deidentified_rendering_filename", raw_value=value)
        try:
            patient, study, series = _parse_rendering_filename_to_folder_tokens(value, deidentified_root)
            series_dir = os.path.join(deidentified_root, patient, study, series)
            dcm_path = _find_first_dicom(series_dir)
            if dcm_path is None:
                raise ValueError(f"no companion DICOM file found in {series_dir}")
            q.anon_patient_id, q.anon_study_uid, q.anon_series_uid = _read_anonymized_uids_from_dicom(dcm_path)
        except Exception as exc:
            q.error = f"{type(exc).__name__}: {exc}"
        queries.append(q)

    for value in _flatten_arg_values(args.deidentified_folder):
        q = QueryInput(source="deidentified_folder", raw_value=value)
        try:
            folder = _resolve_deidentified_folder(value, deidentified_root)
            dcm_path = _find_first_dicom(folder)
            if dcm_path is None:
                raise ValueError(f"no DICOM file found in folder: {folder}")
            q.anon_patient_id, q.anon_study_uid, q.anon_series_uid = _read_anonymized_uids_from_dicom(dcm_path)
        except Exception as exc:
            q.error = f"{type(exc).__name__}: {exc}"
        queries.append(q)

    for value in _flatten_arg_values(args.series_instance_uid):
        queries.append(
            QueryInput(
                source="series_instance_uid",
                raw_value=value,
                anon_series_uid=value,
            )
        )

    for value in _flatten_arg_values(args.study_instance_uid):
        queries.append(
            QueryInput(
                source="study_instance_uid",
                raw_value=value,
                anon_study_uid=value,
            )
        )

    for value in _flatten_arg_values(args.patient_id):
        queries.append(
            QueryInput(
                source="patient_id",
                raw_value=value,
                anon_patient_id=value,
            )
        )

    return queries


def _print_txt(results: list[tuple[QueryInput, list[LookupResult]]]) -> None:
    """Print readable text output."""
    for query, matches in results:
        print(f"Input ({query.source}): {query.raw_value}")
        if query.error:
            print(f"  Error: {query.error}")
            print()
            continue

        print(f"  Anonymized PatientID: {query.anon_patient_id or '-'}")
        print(f"  Anonymized StudyInstanceUID: {query.anon_study_uid or '-'}")
        print(f"  Anonymized SeriesInstanceUID: {query.anon_series_uid or '-'}")

        if not matches:
            print("  No match found.")
            print()
            continue

        for idx, match in enumerate(matches, 1):
            print(f"  Match {idx}:")
            print(f"    Original PatientID: {match.original_patient_id or '-'}")
            print(f"    Original StudyInstanceUID: {match.original_study_uid or '-'}")
            print(f"    Original SeriesInstanceUID: {match.original_series_uid or '-'}")
            print(f"    Original file folder: {match.original_file_folder or '-'}")
        print()


def _print_csv(results: list[tuple[QueryInput, list[LookupResult]]]) -> None:
    """Print CSV output."""
    writer = csv.writer(sys.stdout)
    writer.writerow(
        [
            "input_source",
            "input_value",
            "anonymized_patient_id",
            "anonymized_study_instance_uid",
            "anonymized_series_instance_uid",
            "original_patient_id",
            "original_study_instance_uid",
            "original_series_instance_uid",
            "original_file_folder",
            "status",
            "error",
        ]
    )

    for query, matches in results:
        if query.error:
            writer.writerow(
                [
                    query.source,
                    query.raw_value,
                    query.anon_patient_id,
                    query.anon_study_uid,
                    query.anon_series_uid,
                    "",
                    "",
                    "",
                    "",
                    "error",
                    query.error,
                ]
            )
            continue

        if not matches:
            writer.writerow(
                [
                    query.source,
                    query.raw_value,
                    query.anon_patient_id,
                    query.anon_study_uid,
                    query.anon_series_uid,
                    "",
                    "",
                    "",
                    "",
                    "no_match",
                    "",
                ]
            )
            continue

        for match in matches:
            writer.writerow(
                [
                    query.source,
                    query.raw_value,
                    query.anon_patient_id,
                    query.anon_study_uid,
                    query.anon_series_uid,
                    match.original_patient_id,
                    match.original_study_uid,
                    match.original_series_uid,
                    match.original_file_folder,
                    "ok",
                    "",
                ]
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Lookup original patient/study/series IDs from deidentified inputs "
            "using uid_mappings.db"
        )
    )
    parser.add_argument("--db", required=True, help="Path to uid_mappings.db")
    parser.add_argument(
        "--deidentified-root",
        default=None,
        help=(
            "Root deidentified folder containing PatientXXXX/<study>/<series>. "
            "Defaults to sibling 'deidentified' next to privateMapping/uid_mappings.db"
        ),
    )

    parser.add_argument(
        "--deidentified-rendering-filename",
        action="append",
        nargs="+",
        default=None,
        help=(
            "One or more rendered defacing filenames/paths, e.g. "
            "Patient0001_..._image_defaced or .jpg variant"
        ),
    )
    parser.add_argument(
        "--deidentified-folder",
        action="append",
        nargs="+",
        default=None,
        help=(
            "One or more deidentified series folders containing DICOM files "
            "(absolute or relative path)"
        ),
    )
    parser.add_argument(
        "--series-instance-uid",
        action="append",
        nargs="+",
        default=None,
        help="One or more anonymized SeriesInstanceUID values",
    )
    parser.add_argument(
        "--study-instance-uid",
        action="append",
        nargs="+",
        default=None,
        help="One or more anonymized StudyInstanceUID values",
    )
    parser.add_argument(
        "--patient-id",
        action="append",
        nargs="+",
        default=None,
        help="One or more anonymized PatientID values",
    )

    parser.add_argument(
        "--output-format",
        choices=["txt", "csv"],
        default="txt",
        help="Output format: txt (default) or csv",
    )

    args = parser.parse_args()

    if not os.path.isfile(args.db):
        sys.exit(f"Database not found: {args.db}")

    queries = _build_queries(args)
    if not queries:
        parser.error(
            "at least one input is required: --deidentified-rendering-filename, "
            "--deidentified-folder, --series-instance-uid, --study-instance-uid, or --patient-id"
        )

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    try:
        results: list[tuple[QueryInput, list[LookupResult]]] = []
        for query in queries:
            if query.error:
                results.append((query, []))
                continue
            matches = _lookup_for_query(conn, query)
            results.append((query, matches))

        if args.output_format == "csv":
            _print_csv(results)
        else:
            _print_txt(results)

    except sqlite3.Error as exc:
        sys.exit(f"SQLite error: {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
