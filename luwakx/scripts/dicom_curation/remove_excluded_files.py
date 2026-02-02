#!/usr/bin/env python3
"""
Remove files listed in excluded-files-log.csv from a specified directory.
"""

import os
import argparse
import csv
from pathlib import Path


def read_excluded_files(log_file):
    """
    Read file paths from the exclusion log CSV.
    
    Args:
        log_file: Path to excluded-files-log.csv
    
    Returns:
        list: List of relative file paths to remove
    """
    excluded_files = []
    
    with open(log_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            file_path = row.get('File Path', '').strip()
            if file_path:
                excluded_files.append(file_path)
    
    return excluded_files


def remove_empty_directories(directory, base_dir, dry_run=False):
    """
    Remove empty directories recursively up to the base directory.
    
    Args:
        directory: Directory to check and potentially remove
        base_dir: Base directory (don't remove this)
        dry_run: If True, only simulate
    
    Returns:
        int: Number of directories removed
    """
    removed_count = 0
    directory = Path(directory)
    base_dir = Path(base_dir)
    
    # Don't remove the base directory itself
    if directory == base_dir or not directory.is_relative_to(base_dir):
        return 0
    
    try:
        # Check if directory is empty
        if directory.exists() and directory.is_dir():
            if not any(directory.iterdir()):  # Directory is empty
                if not dry_run:
                    directory.rmdir()
                    print(f"[REMOVED EMPTY DIR] {directory.relative_to(base_dir)}")
                else:
                    print(f"[WOULD REMOVE EMPTY DIR] {directory.relative_to(base_dir)}")
                removed_count = 1
                
                # Recursively check parent directory
                removed_count += remove_empty_directories(directory.parent, base_dir, dry_run)
    except Exception as e:
        print(f"[ERROR REMOVING DIR] {directory.relative_to(base_dir)}: {str(e)}")
    
    return removed_count


def remove_files(base_dir, excluded_files, dry_run=False):
    """
    Remove files from the base directory and clean up empty directories.
    
    Args:
        base_dir: Base directory path
        excluded_files: List of relative file paths to remove
        dry_run: If True, only simulate (don't actually delete)
    
    Returns:
        dict: Statistics about the operation
    """
    base_path = Path(base_dir)
    
    stats = {
        'total_files': len(excluded_files),
        'removed': 0,
        'not_found': 0,
        'errors': 0,
        'dirs_removed': 0
    }
    
    print(f"Base directory: {base_dir}")
    print(f"Files to remove: {len(excluded_files)}")
    print(f"Dry run: {dry_run}\n")
    
    # Track directories that might become empty
    directories_to_check = set()
    
    for file_path in excluded_files:
        full_path = base_path / file_path
        
        if not full_path.exists():
            stats['not_found'] += 1
            print(f"[NOT FOUND] {file_path}")
            continue
        
        if not dry_run:
            try:
                # Track parent directory before removing file
                parent_dir = full_path.parent
                full_path.unlink()
                stats['removed'] += 1
                print(f"[REMOVED] {file_path}")
                directories_to_check.add(parent_dir)
            except Exception as e:
                stats['errors'] += 1
                print(f"[ERROR] {file_path}: {str(e)}")
        else:
            stats['removed'] += 1
            parent_dir = full_path.parent
            directories_to_check.add(parent_dir)
            print(f"[WOULD REMOVE] {file_path}")
    
    # Clean up empty directories
    if directories_to_check:
        print(f"\nChecking {len(directories_to_check)} directories for cleanup...")
        for directory in sorted(directories_to_check):
            stats['dirs_removed'] += remove_empty_directories(directory, base_path, dry_run)
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total files in list: {stats['total_files']}")
    print(f"Files removed: {stats['removed']}")
    print(f"Files not found: {stats['not_found']}")
    print(f"Errors: {stats['errors']}")
    print(f"Empty directories removed: {stats['dirs_removed']}")
    print("="*60)
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Remove files listed in exclusion log CSV from a directory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to preview what would be deleted
  %(prog)s -d /path-to-data -f excluded-files-log.csv --dry-run
  
  # Actually delete the files
  %(prog)s -d /path-to-data -f excluded-files-log.csv
        """
    )
    
    parser.add_argument('-d', '--directory', required=True,
                        help='Base directory containing the files to remove')
    parser.add_argument('-f', '--log-file', required=True,
                        help='Path to excluded-files-log.csv')
    parser.add_argument('--dry-run', action='store_true',
                        help='Simulate without actually deleting files')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.isdir(args.directory):
        print(f"Error: Directory does not exist: {args.directory}")
        return 1
    
    if not os.path.isfile(args.log_file):
        print(f"Error: Log file does not exist: {args.log_file}")
        return 1
    
    # Read excluded files
    print(f"Reading exclusion log: {args.log_file}")
    excluded_files = read_excluded_files(args.log_file)
    print(f"Found {len(excluded_files)} files to remove\n")
    
    if not excluded_files:
        print("No files to remove!")
        return 0
    
    # Confirm before deletion (unless dry run)
    if not args.dry_run:
        print("\n" + "!"*60)
        print("WARNING: You are about to DELETE files!")
        print("!"*60)
        response = input("\nType 'DELETE' to confirm: ")
        if response != 'DELETE':
            print("Operation cancelled.")
            return 0
    
    # Remove files
    remove_files(args.directory, excluded_files, dry_run=args.dry_run)
    
    return 0


if __name__ == '__main__':
    exit(main())