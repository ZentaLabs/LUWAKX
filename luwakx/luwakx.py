#!/usr/bin/env python

import os
import argparse
import sys
from pathlib import Path
from anonymize import LuwakAnonymizer

def main():
    parser = argparse.ArgumentParser(
        description="Process DICOM files using luwak configuration file."
    )
    parser.add_argument(
        "--config_path",
        default="data/luwak-config.json",
        help="Path to the luwak configuration JSON file (default: data/luwak-config.json)"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Show what would be processed without actually processing"
    )
    
    args = parser.parse_args()
    
    # Validate config file exists
    if not os.path.exists(args.config_path):
        print(f"ERROR: Configuration file not found: {args.config_path}")
        sys.exit(1)
    
    if args.dry_run:
        print("DRY RUN MODE - Configuration will be loaded but no files will be processed")
        # Create anonymizer to load and validate config
        anonymizer = LuwakAnonymizer(args.config_path)
        print("\nDry run completed. Configuration is valid.")
    else:
        # Create and run anonymizer
        anonymizer = LuwakAnonymizer(args.config_path)
        anonymizer.anonymize()

if __name__ == "__main__":
    main()
