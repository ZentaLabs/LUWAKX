#!/usr/bin/env python3
"""
Script to automatically delete DICOM series marked for removal in metadata.json files.

This script:
1. Recursively searches for all metadata.json files in the output folder
2. Reads each file and finds entries with "keep_series": false
3. Deletes all DICOM files listed in file_paths for rejected series
4. Creates a log of all deleted files

Usage:
    python delete_rejected_series.py <output_folder> <base_dicom_folder>

Example:
    python delete_rejected_series.py ./plot_output ./clean_dicom_data
"""

import json
import sys
from pathlib import Path
from datetime import datetime


def find_metadata_files(output_folder):
    """Recursively find all metadata.json files."""
    output_path = Path(output_folder)
    return list(output_path.rglob('metadata.json'))


def process_metadata_file(metadata_path, base_dicom_folder, dry_run=False):
    """
    Process a single metadata.json file and delete rejected series.

    Checks folder-level flag first:
    - If keep_folder_series is False, deletes all files regardless of individual flags
    - If keep_folder_series is True, checks individual keep_series flags

    Returns:
        tuple: (deleted_files, rejected_series_info)
    """
    deleted_files = []
    rejected_series = []

    with open(metadata_path, 'r') as f:
        metadata = json.load(f)

    base_path = Path(base_dicom_folder)

    # Check folder-level flag (default to True if not present for backward compatibility)
    keep_folder = metadata.get('keep_folder_series', True)

    # Process each plot entry (skip the keep_folder_series key itself)
    for plot_name, plot_info in metadata.items():
        # Skip the folder-level flag key
        if plot_name == 'keep_folder_series':
            continue

        # Ensure plot_info is a dictionary (metadata entry)
        if not isinstance(plot_info, dict):
            continue

        # Determine if this series should be deleted
        should_delete = False
        deletion_reason = ""

        if not keep_folder:
            # Folder marked for deletion - delete all series in this folder
            should_delete = True
            deletion_reason = "folder marked for deletion (keep_folder_series=false)"
        elif not plot_info.get('keep_series', True):
            # Individual series marked for deletion
            should_delete = True
            deletion_reason = "series marked for deletion (keep_series=false)"

        if should_delete:
            # This series should be deleted
            plot_filename = plot_info.get('plot_filename', f'{plot_name}.png')
            patient_id = plot_info.get('patient_id', 'Unknown')
            series_uid = plot_info.get('series_uid', 'Unknown')
            series_desc = plot_info.get('series_description', 'No Description')
            modality = plot_info.get('modality', 'Unknown')
            file_paths = plot_info.get('file_paths', [])

            rejected_series.append({
                'plot_name': plot_name,
                'plot_filename': plot_filename,
                'patient_id': patient_id,
                'series_uid': series_uid,
                'series_description': series_desc,
                'modality': modality,
                'file_count': len(file_paths),
                'metadata_location': str(metadata_path.parent),
                'deletion_reason': deletion_reason
            })

            # Delete each file
            for rel_path in file_paths:
                file_path = base_path / rel_path

                if file_path.exists():
                    if not dry_run:
                        try:
                            file_path.unlink()
                            deleted_files.append(str(file_path))
                        except Exception as e:
                            print(f"  Error deleting {file_path}: {e}")
                    else:
                        deleted_files.append(str(file_path))
                else:
                    print(f"  Warning: File not found: {file_path}")

    return deleted_files, rejected_series


def main():
    if len(sys.argv) < 3:
        print("Usage: python delete_rejected_series.py <output_folder> <base_dicom_folder> [--dry-run]")
        print("\nArguments:")
        print("  output_folder      : Folder containing plot outputs with metadata.json files")
        print("  base_dicom_folder  : Base folder where DICOM files are stored")
        print("  --dry-run         : (Optional) Show what would be deleted without actually deleting")
        sys.exit(1)

    output_folder = sys.argv[1]
    base_dicom_folder = sys.argv[2]
    dry_run = '--dry-run' in sys.argv

    if dry_run:
        print("=" * 80)
        print("DRY RUN MODE - No files will be deleted")
        print("=" * 80)

    # Find all metadata files
    print(f"\nSearching for metadata.json files in: {output_folder}")
    metadata_files = find_metadata_files(output_folder)
    print(f"Found {len(metadata_files)} metadata.json file(s)")

    if not metadata_files:
        print("No metadata files found. Exiting.")
        return

    # Process each metadata file
    all_deleted_files = []
    all_rejected_series = []

    for metadata_path in metadata_files:
        print(f"\nProcessing: {metadata_path.parent.relative_to(output_folder)}")
        deleted_files, rejected_series = process_metadata_file(
            metadata_path, base_dicom_folder, dry_run
        )

        if rejected_series:
            print(f"  Found {len(rejected_series)} rejected series:")
            for series_info in rejected_series:
                print(f"    - {series_info['plot_filename']}: {series_info['series_description']}")
                print(f"      Patient: {series_info['patient_id']} | Modality: {series_info['modality']}")
                print(f"      Series UID: {series_info['series_uid']}")
                print(f"      Reason: {series_info['deletion_reason']}")
                print(f"      Files to delete: {series_info['file_count']}")

        all_deleted_files.extend(deleted_files)
        all_rejected_series.extend(rejected_series)

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total rejected series: {len(all_rejected_series)}")
    print(f"Total files {'that would be' if dry_run else ''} deleted: {len(all_deleted_files)}")

    # Create deletion log
    if all_deleted_files and not dry_run:
        log_filename = f"deleted_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        log_path = Path(output_folder) / log_filename

        with open(log_path, 'w') as f:
            f.write(f"Deletion Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Base DICOM folder: {base_dicom_folder}\n")
            f.write(f"Output folder: {output_folder}\n")
            f.write(f"\nTotal series rejected: {len(all_rejected_series)}\n")
            f.write(f"Total files deleted: {len(all_deleted_files)}\n")
            f.write("\n" + "=" * 80 + "\n")
            f.write("REJECTED SERIES:\n")
            f.write("=" * 80 + "\n\n")

            for series_info in all_rejected_series:
                f.write(f"Plot filename: {series_info['plot_filename']}\n")
                f.write(f"Plot key: {series_info['plot_name']}\n")
                f.write(f"Patient ID: {series_info['patient_id']}\n")
                f.write(f"Series UID: {series_info['series_uid']}\n")
                f.write(f"Description: {series_info['series_description']}\n")
                f.write(f"Modality: {series_info['modality']}\n")
                f.write(f"Deletion reason: {series_info['deletion_reason']}\n")
                f.write(f"Files deleted: {series_info['file_count']}\n")
                f.write(f"Location: {series_info['metadata_location']}\n")
                f.write("-" * 80 + "\n\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("DELETED FILES:\n")
            f.write("=" * 80 + "\n\n")

            for file_path in all_deleted_files:
                f.write(f"{file_path}\n")

        print(f"\nDeletion log saved to: {log_path}")

    if dry_run:
        print("\nDRY RUN complete. Run without --dry-run to actually delete files.")
    else:
        print(f"\n(SUCCESS) Deletion complete!")


if __name__ == '__main__':
    main()
