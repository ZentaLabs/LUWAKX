#!/usr/bin/env python

import os
import argparse
import sys
import json
from .luwak_logger import setup_logger, get_logger
from .anonymize import LuwakAnonymizer

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
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Disable console logging (only log to file)"
    )
    
    args = parser.parse_args()
    
    # Validate config file exists first
    if not os.path.exists(args.config_path):
        print(f"Error: Configuration file not found: {args.config_path}")
        sys.exit(1)
    
    # Load config to determine log file path
    try:
        with open(args.config_path, 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error: Failed to load configuration file: {e}")
        sys.exit(1)
    
    # Resolve log file path from config
    config_dir = os.path.dirname(os.path.abspath(args.config_path))
    output_folder = config.get('outputDeidentifiedFolder', 'output')
    
    # Resolve paths relative to config file
    if not os.path.isabs(output_folder):
        if output_folder.startswith('~'):
            output_folder = os.path.expanduser(output_folder)
        else:
            output_folder = os.path.join(config_dir, output_folder)
        
    # Create log file path
    os.makedirs(output_folder, exist_ok=True)
    log_file_path = os.path.join(output_folder, 'luwak.log')

    # Get log level from config file (with fallback to INFO)
    log_level = config.get('logLevel', 'INFO')
    
    # Configure logging with config-based file path and level
    setup_logger(
        log_level=log_level,
        log_file=log_file_path,
        console_output=not args.no_console
    )

    # Get logger for this module
    logger = get_logger(__name__)
    
    # Log the arguments being used
    logger.info("=" * 50)
    logger.info("LUWAK DICOM Anonymizer Started")
    logger.info("=" * 50)
    logger.info("Command-line arguments:")
    logger.info(f"  Config file: {args.config_path}")
    logger.info(f"  Log level: {log_level} (from config)")
    logger.info(f"  Console output: {not args.no_console}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info(f"  Log file: {log_file_path}")
    logger.info("-" * 50)
    
    logger.info(f"Using configuration file: {args.config_path}")
    # Set environment variable for deid logging based on log level
    if log_level == "PRIVATE":
        os.environ["MESSAGELEVEL"] = "DEBUG"
    else:
        os.environ["MESSAGELEVEL"] = "INFO"

    if args.dry_run:
        logger.info("DRY RUN MODE - Configuration will be loaded but no files will be processed")
        # Import here to avoid circular imports
        anonymizer = LuwakAnonymizer(args.config_path)
        #logger.info("Dry run completed. Configuration is valid.")
    else:
        anonymizer = LuwakAnonymizer(args.config_path)
        anonymizer.anonymize()

if __name__ == "__main__":
    main()
