#!/usr/bin/env python

import subprocess
import sys
import os
import re
import argparse
import json
import jsonschema
import hashlib
import importlib.util
import csv
import pydicom
import pandas as pd
import logging
from datetime import datetime
from pydicom.datadict import add_private_dict_entry

# Import the centralized logger
from luwak_logger import get_logger, setup_logger

def setup_deid_repo():
    logger = get_logger('setup_deid_repo')
    
    repo_url = "https://github.com/ZentaLabs/deid.git"
    branch = "master"
    repo_dir = os.path.expanduser("~/deid")  # Set repo_dir to the home directory

    # Check if the repository is already cloned
    if not os.path.exists(repo_dir):
        logger.info("Cloning deid repository...")
        subprocess.check_call(["git", "clone", "--branch", branch, repo_url, repo_dir])
    else:
        # Check if the repository is already up-to-date
        logger.info("Checking for updates in deid repository...")
        subprocess.check_call(["git", "-C", repo_dir, "fetch"])
        status = subprocess.check_output(["git", "-C", repo_dir, "status", "--porcelain", "-b"])
        if b"behind" in status:
            logger.info("Updating deid repository...")
            subprocess.check_call(["git", "-C", repo_dir, "pull"])

    if repo_dir not in sys.path:
        sys.path.insert(0, repo_dir)
    # Check if the repository is installed
    try:
        import deid
    except ImportError:
        logger.info("Installing deid repository...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", repo_dir])

# Call the setup function before importing deid
setup_deid_repo()

from deid.config import DeidRecipe
from deid.dicom import get_files, get_identifiers, replace_identifiers

def tag_str_to_int(group, element):
    """
    Convert a DICOM tag string like (0010,xx10) to an integer tag value.
    
    Args:
        group (str): DICOM group in the format 'GGGG'.
        element (str): DICOM element in the format 'xxEE'.

    Returns:
        int: Integer representation of the DICOM tag.
    """
    logger = get_logger('tag_str_to_int')
    
    try:
        group = int(group, 16)
        if str(element).startswith('xx'):
            element_int = int(str(element)[2:], 16)
        else:
            element_int = int(element, 16)
    except ValueError as e:
        logger.error(f"Invalid tag format: ({group},{element}) - {e}")
        raise ValueError(f"Invalid tag format: ({group},{element})")
    return (group << 16) | element_int

def name_to_keyword(name):
    """
    Convert a descriptive name string to a valid DICOM keyword.
    
    Args:
        name (str): The descriptive name to convert (e.g., 'Patient Age (years)').
    
    Returns:
        str: DICOM keyword (e.g., 'PatientAgeYears').
    """
    # Remove non-alphanumeric characters, except spaces
    cleaned = re.sub(r'[^0-9a-zA-Z ]+', '', name)
    # Split by spaces, capitalize each word, and join
    keyword = ''.join(word.capitalize() for word in cleaned.split())
    # Ensure it starts with a letter (prepend 'X' if not)
    if keyword and not keyword[0].isalpha():
        keyword = 'X' + keyword
    return keyword

def register_private_tags_from_csv(csv_path):
    """
    Register private DICOM tags from a CSV file.
    
    Args:
        csv_path (str): Path to the CSV file containing private tag definitions. The CSV should have at least five columns: tag_str, private_creator, vr, vm, description.
            - tag_str: DICOM tag string in the format '(GGGG,xxEE)', where 'xx' is a placeholder for the private block value.
            - private_creator: Name of the private creator.
            - vr: Value Representation (e.g., 'LO', 'CS').
            - vm: Value Multiplicity (e.g., '1', '1-n').
            - description: Description of the tag.
    
    Returns:
        None
    """
    logger = get_logger('register_private_tags')
    logger.debug(f"Loading private tags from: {csv_path}")
    
    tag_count = 0
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Skip header if present
        for row in reader:
            if not row or len(row) < 6 or all(not cell.strip() for cell in row):
                continue  # Skip empty or incomplete rows
            group, element, private_creator, vr, vm, description = row[:6]
            try:
                tag = tag_str_to_int(group, element)
                description = name_to_keyword(description)
                add_private_dict_entry(private_creator, tag, vr, description, vm)
                tag_count += 1
            except Exception as e:
                logger.warning(f"Skipping row {row}: {e}")
                continue
    
    logger.info(f"Successfully registered {tag_count} private DICOM tags")


class ConfigurationError(Exception):
    """Custom exception for configuration file errors with filename context."""
    
    def __init__(self, message, filename=None, original_exception=None):
        """Initialize configuration error with context.
        
        Args:
            message (str): Error description
            filename (str): Path to configuration file that caused the error
            original_exception (Exception): Original exception that was caught
        """
        self.message = message
        self.filename = filename
        self.original_exception = original_exception
        super().__init__(message)
    
    def __str__(self):
        """Return formatted error message including filename context."""
        if self.filename:
            base_msg = f"Configuration error in '{self.filename}': {self.message}"
        else:
            base_msg = f"Configuration error: {self.message}"
        
        if self.original_exception:
            base_msg += f" (Original error: {self.original_exception})"
        
        return base_msg


class LuwakAnonymizer:
    def __init__(self, config_path):
        """Initialize the anonymizer with configuration from JSON file."""
        self.config_path = config_path
        
        # Check if logger is already configured, if not set it up using this config file
        temp_logger = get_logger('anonymize_init')
        if not temp_logger.handlers and not logging.getLogger().handlers:
            self._setup_logger_if_needed()
        
        # Get logger for this module
        self.logger = get_logger(__name__)
        
        self.logger.info("Initializing Luwak Anonymizer...")
        self.logger.debug(f"Configuration file: {config_path}")
        
        try:
            self.load_config()
            self.setup_paths()
        except ConfigurationError as e:
            self.logger.error(f"Configuration error: {e}")
            sys.exit(1)
            
        # Initialize mapping storage for each file
        self.current_file_mappings = {}
        # Initialize metadata storage for Parquet export
        self.dicom_metadata = []
        # Initialize single date shift for entire project run
        
        self.logger.info("Registering private tags from CSV...")
        # Register private tags from CSV
        register_private_tags_from_csv(
            os.path.join(os.path.dirname(__file__), "data", "TagsArchive", "private_tags_template.csv")
        )
        
        self.logger.info("Luwak Anonymizer initialization completed")
        
    def _setup_logger_if_needed(self):
        """Set up logger if not already configured, using config file information."""
        
        try:
            # Load config to determine log file path (minimal loading, just for paths)
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Resolve log file path from config (same logic as luwakx.py)
            config_dir = os.path.dirname(os.path.abspath(self.config_path))
            output_folder = config.get('outputDeidentifiedFolder', 'output')
            recipe_folder = config.get('recipesFolder', 'recipes')
            
            # Resolve paths relative to config file
            if not os.path.isabs(output_folder):
                if output_folder.startswith('~'):
                    output_folder = os.path.expanduser(output_folder)
                else:
                    output_folder = os.path.join(config_dir, output_folder)
            
            if not os.path.isabs(recipe_folder):
                recipe_folder = os.path.join(output_folder, recipe_folder)
            
            # Create log file path
            os.makedirs(recipe_folder, exist_ok=True)
            log_file_path = os.path.join(recipe_folder, 'luwak.log')
            
            # Configure logging with same settings as luwakx.py
            setup_logger(
                log_level='INFO',
                log_file=log_file_path,
                console_output=False
            )

        except Exception as e:
            # Fallback to basic logging if config loading fails
            setup_logger(
                log_level='INFO',
                log_file=None,
                console_output=False
            )

    def is_tag_private(self, dicom, value, field, item):
        """Check if a DICOM tag is private.
        
        Args:
            dicom: PyDicom dataset object containing DICOM data
            value: Recipe string or value from recipe processing - not the actual DICOM value
            field: DICOM field element containing tag information
            item: Item identifier from deid processing
            
        Returns:
            bool: True if the tag is private (has private creator), False otherwise
            
        Note:
            The 'value' parameter contains recipe-related data, not the actual DICOM field value.
            The actual determination is based on field.element.is_private and private_creator.
        """
        self.logger.info(f"Removed tag {field.element.tag} ({getattr(field.element, 'name', '')}).")
        # Log private tag details at PRIVATE level if it exists
        if hasattr(field.element, 'value'):
            self.logger.private(f"Removed private tag {field.element.tag} with value: {field.element.value}")
        return field.element.is_private and (field.element.private_creator is not None)
    
    def hash_increment_date(self, item, value, field, dicom):
        """Generate single date/time shift value for entire anonymization project.
        
        Args:
            item: Item identifier from deid processing
            value: Recipe string (e.g., "func:hash_increment_date") - not the actual DICOM value
            field: DICOM field element containing the date/time tag
            dicom: PyDicom dataset object
        
        Returns:
            int: Number of days to shift backward (0-maxDateShiftDays days, consistent for entire project)

        Note:
            - Uses project_hash_root to generate single shift for entire project
            - Lazy initialization - calculates shift only once per project
            This method only returns the shift amount. The actual date manipulation
            should be handled by the DEID recipe or calling code.
            The 'value' parameter contains the recipe string, not the actual DICOM value.
        """
        project_hash_root = self.config.get('projectHashRoot')
        try:
            PatientID = dicom.get("PatientID", "")
            PatientName = dicom.get("PatientName", "")
            PatientBirthDate = dicom.get("PatientBirthDate", "")
            # Log sensitive patient data at PRIVATE level
            self.logger.private(f"Using patient data for date shift generation - PatientID: {PatientID}, PatientName: {PatientName}, PatientBirthDate: {PatientBirthDate}")
            # Generate shift for project run and patient
            # Use project_hash_root to generate consistent shift for this project
            project_salt = f"{project_hash_root}{PatientID}{PatientName}{PatientBirthDate}"
            salt_hash = hashlib.sha256(project_salt.encode()).hexdigest()
            hash_int = int(salt_hash[:8], 16)  # Use first 8 hex chars
            # Use configurable max_date_shift_days (default 1095)
            project_date_shift = hash_int % (self.config.get('maxDateShiftDays') + 1)  # 0 to max_date_shift_days
            self.logger.info(f"Replacing tag {field.element.tag} ({getattr(field.element, 'keyword', '')}) with date/time shifted.")
            # Log the computed shift value at PRIVATE level
            self.logger.private(f"For tag {field.element.tag} with value {field.element.value}, computed date shift: -{project_date_shift} days")
            return -project_date_shift
            
        except Exception as e:
            self.logger.error(f"Error in date shift generation: {e}")
            return 0  # Return 0 days shift on error
    
    def set_fixed_datetime(self, item, value, field, dicom):
        """Generate fixed date/time values based on VR type for anonymization.
        
        Args:
            item: Item identifier from deid processing (not used)
            value: Recipe string (e.g., "func:set_fixed_datetime") - not the actual DICOM value
            field: DICOM field element containing the date/time tag
            dicom: PyDicom dataset object
            
        Returns:
            str: fixed date/time value based on VR type
            
        VR-specific Output:
            - DA (Date): Returns "00010101" (January 1, year 1)
            - DT (DateTime): Returns "00010101010101.000000+0000" (January 1, year 1, 01:01:01.000000 UTC)
            - TM (Time): Returns "000000.00" (00:00:00.00)
            
        Note:
            This method provides consistent fixed values for anonymization
            when actual date shifting is not desired.
            The 'value' parameter contains the recipe string, not the actual DICOM value.
        """
        try:
            # Get the VR type from the field
            vr = field.element.VR if hasattr(field, 'element') else None
            tag_str = getattr(field.element, 'tag', 'unknown') if hasattr(field, 'element') else 'unknown'
            keyword_str = getattr(field.element, 'keyword', '') if hasattr(field, 'element') else ''
            if vr == 'DA':  # Date format: YYYYMMDD
                self.logger.info(f"Setting fixed date for tag {tag_str} ({keyword_str}) to '00010101'.")
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(f"Setting fixed date for tag {tag_str} ({keyword_str}) with value {field.element.value} to '00010101'.")
                return "00010101"
            elif vr == 'DT':  # DateTime format: YYYYMMDDHHMMSS.FFFFFF&ZZXX
                self.logger.info(f"Setting fixed datetime for tag {tag_str} ({keyword_str}) to '00010101010101.000000+0000'.")
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(f"Setting fixed datetime for tag {tag_str} ({keyword_str}) with value {field.element.value} to '00010101010101.000000+0000'.")
                return "00010101010101.000000+0000"
            elif vr == 'TM':  # Time format: HHMMSS.FFFFFF
                self.logger.info(f"Setting fixed time for tag {tag_str} ({keyword_str}) to '000000.00'.")
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(f"Setting fixed time for tag {tag_str} ({keyword_str}) with value {field.element.value} to '000000.00'.")
                return "000000.00"
            else:
                # For unknown VR, return the original value
                original_value = field.element.value if hasattr(field, 'element') and hasattr(field.element, 'value') else ""
                tag_str = getattr(field.element, 'tag', 'unknown') if hasattr(field, 'element') else 'unknown'
                self.logger.warning(f"Unknown VR type '{vr}' for tag {tag_str}, returning original value.")
                return str(original_value) if original_value is not None else ""
                
        except Exception as e:
            self.logger.error(f"Error in fixed datetime generation: {e}")
            return ""
    
    def clean_descriptors_with_llm(self, item, value, field, dicom):
        """Clean descriptive text fields using a large language model (LLM) and PHI/PII detector.
           
           Args:
                item: Item identifier from deid processing (not used)
                value: Recipe string (e.g., "func:clean_descriptors_with_llm") - not the actual DICOM value
                field: DICOM field element containing the text tag
                dicom: PyDicom dataset object
            
            Returns:
                str: Cleaned text value or "[REDACTED]" if PHI/PII detected
            
            Note:
                - Uses LLM to clean descriptive text fields
                - Calls PHI/PII detector to check if cleaned text still contains sensitive info
                - If PHI/PII detected, deletes the element and returns ""
                - If no PHI/PII detected, returns original text value
                The 'value' parameter contains the recipe string, not the actual DICOM value.
        """
        from openai import OpenAI
        # Import detector.py as a module (no execution of __main__)
        try:
            if hasattr(field, 'element') and hasattr(field.element, 'value'):
                original_value = str(field.element.value)
            elif hasattr(field, 'value'):
                original_value = str(field.value)
            else:
                original_value = str(value) if value else "unknown"
            # Log original value at PRIVATE level
            self.logger.private(f"Processing original value for tag {field.element.tag} ({getattr(field.element, 'keyword', '')}): {original_value}")
        except Exception as e:
            self.logger.error(f"  ERROR extracting original value: {e}")
            original_value = str(value) if value else "unknown"
        try:
            detector_path = os.path.join(os.path.dirname(__file__), "scripts", "detector", "detector.py")
            spec = importlib.util.spec_from_file_location("detector", detector_path)
            detector = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(detector)
        except Exception as e:
            self.logger.error(f"Failed to import detector.py: {e}")
            return str(field.element.value) if hasattr(field.element, 'value') else str(value)

        # Get LLM config from self.config
        base_url = self.config.get('cleanDescriptorsLlmBaseUrl', "https://api.openai.com/v1")
        model = self.config.get('cleanDescriptorsLlmModel', "openai/gpt-4o-mini")
        api_key_env = self.config.get('cleanDescriptorsLlmApiKeyEnvVar', "ZENTA_OPENAI_API_KEY")
        api_key = os.environ.get(api_key_env, "")

        try:
            client = OpenAI(base_url=base_url, api_key=api_key)
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenAI client: {e}")
            return str(field.element.value) if hasattr(field.element, 'value') else str(value)
        
        tag_desc = f"{getattr(field.element, 'tag', '')} {getattr(field.element, 'keyword', '')}: {str(field.element.value) if hasattr(field.element, 'value') else str(value)}"
        try:
            # Call the function directly, do not execute detector.py as a script
            result = detector.detect_phi_or_pii(client, tag_desc, model=model, dev_mode=False)
            self.logger.private(f"PHI/PII detection result for tag {tag_desc} : {result}")
            if str(result).strip() == "1":
                # Remove the element from the DICOM dataset
                try:
                    del dicom[field.element.tag]
                    self.logger.info(f"Removed tag {field.element.tag} ({getattr(field.element, 'keyword', '')}) from DICOM file.")
                except Exception as e:
                    self.logger.warning(f"Failed to remove element {field.element.tag}: {e}")
                    self.logger.info(f"Replaced tag {field.element.tag} ({getattr(field.element, 'keyword', '')}) to 'ANONYMIZED'.")
                return "ANONYMIZED"  # Empty string for other types
            else:
                self.logger.info(f"Keeping original value for tag {field.element.tag} ({getattr(field.element, 'keyword', '')}).")
                return str(field.element.value) if hasattr(field.element, 'value') else str(value)
        except Exception as e:
            self.logger.error(f"Error in PHI/PII detection: {e}")
            return str(field.element.value) if hasattr(field.element, 'value') else str(value)

    def clean_recognizable_visual_features(self, dicom_dir, output_dir):
        """Clean tags that may contain recognizable visual features using a defacing ML model or an existing mask.
           
           Args:
                dicom_dir: Directory containing DICOM files to process
                output_dir: Directory to save defaced DICOM files
            
            Returns:
                str: Path to the defaced DICOM file or an error message

            Note:
                - Uses defacing ML model to clean data
        """
        if not os.path.exists(input_folder):
            self.logger.error(f"Input folder does not exist: {input_folder}")
            return None
        
        # Check that the log does not leak sensitive info and in case move the log to PRIVATE level
        import SimpleITK
        try:
            defacer_path = os.path.join(os.path.dirname(__file__), "scripts", "defacing", "image_defacer", "image_anonymization.py")
            spec = importlib.util.spec_from_file_location("image_anonymization", defacer_path)
            defacer = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(defacer)
        except Exception as e:
            self.logger.error(f"Failed to import image_anonymization.py: {e}")
            return str(field.element.value) if hasattr(field.element, 'value') else str(value)
        
        reader = SimpleITK.ImageSeriesReader()
        try:
            series_ids = reader.GetGDCMSeriesIDs(dicom_dir)
        except Exception as e:
            self.logger.error(f"Failed to get DICOM series IDs in {dicom_dir}: {e}")
            return

        if not series_ids:
            self.logger.error(f"No DICOM series found in: {dicom_dir}")
            return
        for series_id in series_ids:
            try:
                dicom_filenames = reader.GetGDCMSeriesFileNames(dicom_dir, series_id)
            except Exception as e:
                self.logger.error(f"Failed to get DICOM filenames for series {series_id} in {dicom_dir}: {e}")
                continue
            try:
                ds = pydicom.dcmread(dicom_filenames[0])
                modality = ds.Modality if 'Modality' in ds else None
                body_part = ds.BodyPartExamined if 'BodyPartExamined' in ds else None
            except Exception as e:
                self.logger.error(f"Failed to read DICOM file {dicom_filenames[0]}: {e}")
                continue
            if modality.upper() == "CT" and body_part.upper() in ["HEAD", "BRAIN", "FACE", "NECK"]:
                try:
                    reader.SetFileNames(dicom_filenames)
                    image = reader.Execute()
                    image_face_segmentation = defacer.prepare_face_mask(image, modality)
                    image_defaced = defacer.pixelate_face(image, image_face_segmentation)
                    defaced_array = SimpleITK.GetArrayFromImage(image_defaced) # Shape: [slices, height, width]
                except Exception as e:
                    self.logger.error(f"Defacing failed for series {series_id} in {dicom_dir}: {e}")
                    continue
                # For each slice, copy metadata and replace pixel data
                for i, dicom_file in enumerate(dicom_filenames):
                    try:
                        ds = pydicom.dcmread(dicom_file)
                        ds.PixelData = defaced_array[i].astype(ds.pixel_array.dtype).tobytes()
                        # Optionally update SeriesDescription, etc.
                        output_path = os.path.join(output_dir, os.path.basename(dicom_file))
                        ds.save_as(output_path)
                        self.logger.info(f"Defaced DICOM saved: {output_path}")
                    except Exception as e:
                        self.logger.error(f"Failed to save defaced DICOM for {dicom_file}: {e}")
                        continue
            else:
                self.logger.info(f"Skipping defacing for modality {modality} and body part {body_part}.")
                # Copy all files in dicom_filenames to output_dir
                import shutil
                for src_file in dicom_filenames:
                    output_path = os.path.join(output_dir, os.path.basename(src_file))
                    try:
                        shutil.copy2(src_file, output_path)
                        self.logger.info(f"Copied DICOM file to output: {output_path}")
                    except Exception as e:
                        self.logger.error(f"Failed to copy {src_file} to {output_path}: {e}")
                continue


    def generate_hashuid(self, item, value, field, dicom):
        """Custom UID generation using combined salt as root for deterministic randomization.
        Ensures remapping: the same original UID always maps to the same anonymized UID for a given file and field.
        """
        project_hash_root = self.config.get('projectHashRoot')
        # Extract the original UID value from the DICOM field
        try:
            if hasattr(field, 'element') and hasattr(field.element, 'value'):
                original_uid = str(field.element.value)
            elif hasattr(field, 'value'):
                original_uid = str(field.value)
            else:
                original_uid = str(value) if value else "unknown"
            # Log original UID at PRIVATE level
            self.logger.private(f"Processing original UID for tag {field.element.tag} ({getattr(field.element, 'keyword', '')}): {original_uid}")
        except Exception as e:
            self.logger.error(f"  ERROR extracting original UID: {e}")
            original_uid = str(value) if value else "unknown"

        # Extract file path from the dicom dataset filename attribute
        file_path = getattr(dicom, 'filename', str(dicom))
        if file_path not in self.current_file_mappings:
            self.current_file_mappings[file_path] = {}

        # Get field keyword from the element
        field_keyword = getattr(field.element, 'keyword', field.element.tag)

        # Check if mapping already exists for this file, field, and original UID
        mapping = self.current_file_mappings[file_path].get(field_keyword)
        if mapping and mapping.get('original') == original_uid:
            return mapping['anonymized']

        # Combine project_hash_root and original UID as entropy for deterministic generation
        new_uid = pydicom.uid.generate_uid(entropy_srcs=[project_hash_root, original_uid])
        self.logger.info(f"Replaced tag {field.element.tag} ({getattr(field.element, 'keyword', '')}): {new_uid}")
        # Log the UID mapping at PRIVATE level
        self.logger.private(f"UID mapping created - Original: {original_uid} -> Anonymized: {new_uid}")
        # Store the mapping for this file, field, and original UID
        self.current_file_mappings[file_path][field_keyword] = {
            'original': original_uid,
            'anonymized': new_uid
        }
        return new_uid
    
    def save_all_uid_mappings(self):
        """Save all UID mappings to CSV file with one row per DICOM file, including patient info columns.
        
        Args:
            None (uses self.current_file_mappings and private_map_folder)
        
        Returns:
            None

        Note:
            - Creates/appends to uid_mappings.csv in private mapping folder
            - CSV format: file_path, {field}_original, {field}_anonymized columns
            - Dynamically detects all modified UID fields across all processed files
            - Clears self.current_file_mappings after saving
            
        Output File:
            - CSV with headers: file_path, StudyInstanceUID_original, StudyInstanceUID_anonymized, etc.
            - One row per processed DICOM file
            - Empty cells for fields not present in specific files
        """
        private_map_folder = self.config.get('outputPrivateMappingFolder')
        mapping_file = os.path.join(private_map_folder, "uid_mappings.csv")

        # Check if file exists to determine if we need to write headers
        file_exists = os.path.exists(mapping_file)

        # Dynamically discover all modified fields across all files
        all_modified_fields = set()
        for file_path, mappings in self.current_file_mappings.items():
            all_modified_fields.update(mappings.keys())

        # Sort the fields for consistent column ordering
        sorted_fields = sorted(all_modified_fields)

        # Add patient info columns
        patient_columns = ['PatientName', 'PatientID', 'PatientBirthDate']
        fieldnames = ['file_path'] + patient_columns
        for field in sorted_fields:
            fieldnames.extend([f'{field}_original', f'{field}_anonymized'])

        self.logger.debug(f"Dynamically detected {len(sorted_fields)} modified fields: {sorted_fields}")

        # Open file in append mode
        with open(mapping_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header if file is new
            if not file_exists:
                writer.writeheader()

            # Write one row per file
            for file_path, mappings in self.current_file_mappings.items():
                row = {
                    'file_path': os.path.basename(file_path)
                }

                # Try to read patient info from the DICOM file
                try:
                    ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                    row['PatientName'] = str(getattr(ds, 'PatientName', ''))
                    row['PatientID'] = str(getattr(ds, 'PatientID', ''))
                    row['PatientBirthDate'] = str(getattr(ds, 'PatientBirthDate', ''))
                except Exception as e:
                    self.logger.warning(f"Could not read patient info from {file_path}: {e}")
                    row['PatientName'] = ''
                    row['PatientID'] = ''
                    row['PatientBirthDate'] = ''

                # Add mapping data for each modified field
                for field in sorted_fields:
                    if field in mappings:
                        row[f'{field}_original'] = mappings[field]['original']
                        row[f'{field}_anonymized'] = mappings[field]['anonymized']
                    else:
                        # Field not modified in this particular file
                        row[f'{field}_original'] = ''
                        row[f'{field}_anonymized'] = ''

                writer.writerow(row)

        self.logger.info(f"UID mappings saved for {len(self.current_file_mappings)} files to: {mapping_file}")
        self.logger.info(f"CSV contains mappings for {len(sorted_fields)} different field types")

        # Clear the mappings for next run
        self.current_file_mappings = {}
    
    def extract_dicom_metadata(self, dicom_file, anonymized_file_path):
        """Extract metadata from anonymized DICOM file for Parquet export - only retained tags.
        
        Args:
            dicom_file (str): Path to original DICOM file (for reference/logging)
            anonymized_file_path (str): Path to anonymized DICOM file to extract from
        
        Returns:
            None

        Note:
            - Reads anonymized DICOM file and extracts all retained DICOM elements
            - Appends metadata dict to self.dicom_metadata list
            - Skips file meta information (group 0x0002) and pixel data and excluded tags
            - Converts DICOM values to appropriate Python types based on VR
            
        Extracted Data:
            - AnonymizedFilePath: basename of anonymized file
            - All DICOM elements with keywords (private tags without keywords skipped)
            - Type conversion: PN/DA/TM/etc->str, IS->int, DS->float, multi-value->list
            
        Error Handling:
            - Continues processing if individual elements fail
            - Prints warning if entire file extraction fails
        """
        try:
            self.logger.debug(f"Extracting metadata from: {anonymized_file_path}")
            
            # Read the anonymized DICOM file
            ds = pydicom.dcmread(anonymized_file_path, force=True)
            
            # Start with minimal file tracking information
            metadata = {
                'AnonymizedFilePath': os.path.basename(anonymized_file_path),
            }
            # Initialize private tag counter
            private_tag_counter = 0
            # Dynamically extract all retained DICOM tags using their keyword names
            # Skip file meta information and pixel data and excluded tags
            for elem in ds:
                tag_int = int(elem.tag)
                if elem.tag.group == 0x0002:
                    continue
                if tag_int == 0x7FE00010:
                    continue
                if tag_int in self.excluded_tags_from_parquet:
                    continue

                if elem.is_private and elem.private_creator:
                    try:
                        private_creator = elem.private_creator
                        # Replace spaces with underscores for consistency
                        private_creator = private_creator.replace(' ', '_')
                        if elem.name and elem.name != "Unknown":
                            keyword = f'{private_creator}_{elem.name[1:-1]}'
                        else:
                            # If name is unknown, use tag as fallback
                            keyword = f'{private_creator}_{elem.tag.group:04X}xx{elem.tag.element & 0xFF:02X}'
                    except Exception as e:
                        self.logger.warning(f"Skipping private tag ({elem.tag}): {e}")
                        continue
                else:
                    # Get the keyword name for this DICOM element
                    keyword = elem.keyword
                
                # Extract the value based on element type
                try:        
                    if elem.VR in ['PN']:  # Person Name
                        value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['DA']:  # Date
                        value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['TM']:  # Time
                        value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['DT']:  # DateTime
                        value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['UI']:  # Unique Identifier
                        value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['SH', 'LO', 'ST', 'LT', 'UT', 'AE', 'CS', 'AS']:  # String types
                        value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['IS']:  # Integer String
                        try:
                            if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                                # Multi-value integer field - convert to string list
                                value = str(list(elem.value)) if elem.value else ''
                            else:
                                value = int(elem.value) if elem.value else 0
                        except (ValueError, TypeError):
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['DS']:  # Decimal String
                        try:
                            if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                                # Multi-value decimal field - convert to string list
                                value = str(list(elem.value)) if elem.value else ''
                            else:
                                value = float(elem.value) if elem.value else 0.0
                        except (ValueError, TypeError):
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['US', 'SS']:  # Unsigned/Signed Short
                        try:
                            if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                                # Multi-value field - convert to string list
                                value = str(list(elem.value)) if elem.value else ''
                            else:
                                value = int(elem.value) if elem.value is not None else 0
                        except (ValueError, TypeError):
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['UL', 'SL']:  # Unsigned/Signed Long
                        try:
                            if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                                # Multi-value field - convert to string list
                                value = str(list(elem.value)) if elem.value else ''
                            else:
                                value = int(elem.value) if elem.value is not None else 0
                        except (ValueError, TypeError):
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['FL', 'FD']:  # Float/Double
                        try:
                            if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                                # Multi-value field - convert to string list
                                value = str(list(elem.value)) if elem.value else ''
                            else:
                                value = float(elem.value) if elem.value is not None else 0.0
                        except (ValueError, TypeError):
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['SQ']:  # Sequence - skip for now
                        continue
                    elif hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                        # Multi-value fields - convert to string representation
                        value = str(list(elem.value)) if elem.value else ''
                    else:
                        # Default to string representation
                        value = str(elem.value) if elem.value is not None else ''
                    
                    # Add to metadata using the DICOM keyword as column name
                    metadata[keyword] = value
                    # Log element value at PRIVATE level for audit purposes
                    self.logger.private(f"Extracted element {keyword} ({elem.tag}): {value}")
                    
                except Exception as e:
                    # If there's any issue with this element, skip it
                    self.logger.warning(f"Skipping element {keyword} ({elem.tag}): {e}")
                    continue
            
            # Add to metadata collection
            self.dicom_metadata.append(metadata)
            
        except Exception as e:
            self.logger.warning(f"Could not extract metadata from {dicom_file}: {e}")
    
    def export_metadata_to_parquet(self):
        """Export all collected metadata to Parquet file with dynamic schema based on retained tags.
        
        Args:
            None (uses self.dicom_metadata and private_map_folder)
        
        Returns:
            str: Path to created Parquet file, or None if export failed

        Note:
            - Creates metadata.parquet in private mapping folder
            - Optimizes data types: integers->Int64, floats->float64, strings->string
            - Converts DICOM dates (YYYYMMDD) to pandas datetime objects
            - Clears self.dicom_metadata after successful export
            
        Output File:
            - Parquet format with Snappy compression
            - Dynamic schema based on retained DICOM tags after anonymization
            - One row per processed DICOM file
            - Columns: AnonymizedFilePath + all retained DICOM element keywords
            
        Performance Optimizations:
            - Uses dictionary encoding for repeated values
            - 10k row groups for analytics workloads
            - Automatic type inference and optimization
            
        Error Handling:
            - Returns None if pandas/pyarrow not available
            - Prints warnings for import or export errors
        """
        private_map_folder = self.config.get('outputPrivateMappingFolder')
        try:
            
            if not self.dicom_metadata:
                self.logger.info("No metadata to export")
                return
            
            # Create DataFrame from dynamic metadata
            df = pd.DataFrame(self.dicom_metadata)

            self.logger.debug(f"Dynamic Parquet schema detected {len(df.columns)} columns from retained DICOM tags")

            # Optimize data types for better Parquet performance
            # We'll infer types dynamically since we don't know which columns will exist
            for col in df.columns:
                # Skip our fixed tracking columns
                if col in ['AnonymizedFilePath']:
                    df[col] = df[col].astype('string')
                    continue
                
                # Skip derived boolean fields
                if col in ['HasPixelData', 'IsMultiFrame', 'IsColor', 'IsEnhanced']:
                    continue  # Keep as boolean
                
                # Skip file size columns (keep as int)
                if col in ['OriginalFileSizeBytes', 'AnonymizedFileSizeBytes']:
                    continue
                
                # Try to optimize data types based on current values
                sample_values = df[col].dropna()
                if len(sample_values) == 0:
                    continue  # Skip empty columns
                
                # Check if it's all integers
                if all(isinstance(v, (int, float)) and float(v).is_integer() for v in sample_values):
                    try:
                        df[col] = df[col].astype('Int64')  # Nullable integer
                        continue
                    except:
                        pass
                
                # Check if it's all floats
                if all(isinstance(v, (int, float)) for v in sample_values):
                    try:
                        df[col] = df[col].astype('float64')
                        continue
                    except:
                        pass
                
                # Convert dates to proper datetime format if they look like DICOM dates
                if col.endswith('Date') and all(isinstance(v, str) and len(v) == 8 and v.isdigit() for v in sample_values):
                    try:
                        df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
                        continue
                    except:
                        pass
                
                # Default to string for everything else
                try:
                    df[col] = df[col].astype('string')
                except:
                    pass  # Keep original type if conversion fails
            
            # Create Parquet file path - use fixed name as requested
            parquet_file = os.path.join(private_map_folder, "metadata.parquet")
            
            # Export to Parquet with optimized settings
            df.to_parquet(
                parquet_file,
                engine='pyarrow',
                compression='snappy',
                index=False,
                # Optimize for analytics workloads
                row_group_size=10000,
                use_dictionary=True
            )
            
            self.logger.info(f"Metadata exported to Parquet: {parquet_file}")
            self.logger.info(f"Exported {len(df)} DICOM metadata records with {len(df.columns)} retained tag columns")
            
            # Print schema summary for verification
            self.logger.debug("Dynamic Parquet Schema Summary:")
            self.logger.debug(f"- Total columns: {len(df.columns)}")
            
            # Clear metadata for next run
            self.dicom_metadata = []
            
            return parquet_file
            
        except ImportError:
            self.logger.warning("pandas and pyarrow required for Parquet export. Install with: pip install pandas pyarrow")
        except Exception as e:
            self.logger.error(f"Error exporting metadata to Parquet: {e}")
    
    def load_config(self):
        """Load and parse the JSON configuration file.
        
        Args:
            None (uses self.config_path)
        
        Returns:
            None
        
        Note:
            - Sets instance attributes from JSON config with fallback defaults
            - Prints configuration summary and warnings for missing keys
            - Exits program if config file not found or invalid JSON
            - Configuration structure and defaults are defined in the JSON schema file (data/config.schema.json).
        
        Error Handling:
            - FileNotFoundError: Exits with error message
            - JSONDecodeError: Exits with parse error details
            - Other exceptions: Exits with generic error message
        """
        # Load config JSON
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
        except FileNotFoundError as e:
            raise ConfigurationError(
                f"Configuration file not found",
                filename=self.config_path,
                original_exception=e
            )
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON format - {e.msg} at line {e.lineno}, column {e.colno}",
                filename=self.config_path,
                original_exception=e
            )
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load configuration",
                filename=self.config_path,
                original_exception=e
            )

        # Load schema JSON
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "config.schema.json")
        try:
            with open(schema_path, 'r') as sf:
                schema = json.load(sf)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to read configuration schema file",
                filename=schema_path,
                original_exception=e
            )
        # Recursively apply defaults from schema to config dict
        for key, prop in schema.get('properties', {}).items():
            if key not in config:
                config[key] = prop['default']
        # Validate config against schema
        try:
            jsonschema.validate(instance=config, schema=schema)
        except jsonschema.ValidationError as ve:
            raise ConfigurationError(
                f"Configuration validation error: {ve.message}",
                filename=self.config_path,
                original_exception=ve
            )
        except jsonschema.SchemaError as se:
            raise ConfigurationError(
                f"Configuration schema error: {se.message}",
                filename=schema_path,
                original_exception=se
            )

        # Store the entire config as an object
        self.config = config
        # Set config_dir for use in resolve_path
        self.config_dir = os.path.dirname(os.path.abspath(self.config_path))
        # Excluded tags from Parquet export (list of tag ints or strings)
        excluded_tags = self.config.get('excludedTagsFromParquet')
        self.excluded_tags_from_parquet = set()
        for tag in excluded_tags:
            # Accept int (e.g., 0x7FE00010), string (e.g., "7FE0,0010"), or string with parentheses ("(7FE0,0010)")
            if isinstance(tag, int):
                self.excluded_tags_from_parquet.add(tag)
            elif isinstance(tag, str):
                tag_str = tag.strip().strip('()')
                if ',' in tag_str:
                    parts = tag_str.split(',')
                    if len(parts) == 2:
                        group_str, elem_str = parts
                        try:
                            group = int(group_str.strip(), 16)
                            elem = int(elem_str.strip(), 16)
                            tag_int = (group << 16) | elem
                            self.excluded_tags_from_parquet.add(tag_int)
                        except Exception:
                            pass
                else:
                    try:
                        tag_int = int(tag_str, 16)
                        self.excluded_tags_from_parquet.add(tag_int)
                    except Exception:
                        pass

        self.logger.info(f"Configuration loaded from: {self.config_path}")
        self.logger.debug(f"Config keys: {list(self.config.keys())}")

    def resolve_path(self, path, is_output=False):
        """Resolve a path relative to the config file directory."""
        if not path:
            return path
        if os.path.isabs(path):
            return os.path.expanduser(path) if is_output else path
        if is_output and path.startswith('~'):
            return os.path.expanduser(path)
        # Use self.config_dir set in load_config
        return os.path.abspath(os.path.join(self.config_dir, path))

    
    def setup_paths(self):
        """Resolve and setup all paths relative to the config file location.
        
        Args:
            None (uses loaded config attributes)
            
        Returns:
            None
            
        Note:
            - Converts relative paths to absolute paths relative to config file directory
            - Expands user directories (~) in output paths
            - Creates output directories if they don't exist
            - Replaces {shared_config} placeholder with config directory
            - Validates that input and recipes folders exist (warnings if missing)
            
        Path Resolution Rules:
            - Already absolute paths: Keep as-is (expand ~ for output paths)
            - Relative paths: Make absolute relative to config file directory
            - Output paths with ~: Expand user directory first
            - {shared_config}: Replace with config file directory
            
        Created Directories:
            - output_directory: For anonymized DICOM files
            - private_map_folder: For mappings and metadata exports
            
        Validation:
            - Prints warnings if input_folder or recipes_folder don't exist
            - Does not exit on missing folders (allows processing to continue)
        """
        # Get config directory for resolving relative paths
        config_dir = os.path.dirname(os.path.abspath(self.config_path))
        self.logger.debug(f"Config directory (base for relative paths): {config_dir}")

        # Use config keys
        input_folder = self.config.get('inputFolder')
        output_directory = self.config.get('outputDeidentifiedFolder')
        private_map_folder = self.config.get('outputPrivateMappingFolder')
        recipes_folder = self.config.get('recipesFolder')

        # Resolve {shared_config} placeholder with config directory
        if recipes_folder and '{shared_config}' in recipes_folder:
            recipes_folder = recipes_folder.replace('{shared_config}', config_dir)

        # Convert all relative paths to absolute paths relative to config file
        input_folder = self.resolve_path(input_folder)
        output_directory = self.resolve_path(output_directory, is_output=True)
        private_map_folder = self.resolve_path(private_map_folder, is_output=True)
        recipes_folder = self.resolve_path(recipes_folder)

        # Store resolved paths back in config for consistency
        self.config['inputFolder'] = input_folder
        self.config['outputDeidentifiedFolder'] = output_directory
        self.config['outputPrivateMappingFolder'] = private_map_folder
        # Recipes folder should be a subfolder inside the output directory
        recipes_folder = os.path.join(output_directory, os.path.basename(recipes_folder))
        self.config['recipesFolder'] = recipes_folder
        os.makedirs(recipes_folder, exist_ok=True)

        # Create output directories
        os.makedirs(output_directory, exist_ok=True)
        os.makedirs(private_map_folder, exist_ok=True)

        self.logger.info("Final paths:")
        self.logger.info(f"  Input folder: {input_folder}")
        self.logger.info(f"  Output directory: {output_directory}")
        self.logger.info(f"  Private mapping folder: {private_map_folder}")
        self.logger.info(f"  Recipes folder: {recipes_folder}")
        
        # Log configuration info
        self.logger.info(f"Configuration loaded from: {self.config_path}")
        self.logger.debug(f"  Config keys: {list(self.config.keys())}")

        # Validate that input and recipes folders exist
        if not os.path.exists(input_folder):
            self.logger.warning(f"Input folder does not exist: {input_folder}")
        if not os.path.exists(recipes_folder):
            self.logger.warning(f"Recipes folder does not exist: {recipes_folder}")
            self.logger.warning("  Make sure recipe files are available at this location or adjust the config.")
    
    def get_dicom_files(self, input_folder):
        """Get all DICOM files from the input folder."""
        if not os.path.exists(input_folder):
            self.logger.error(f"Input folder does not exist: {input_folder}")
            return None

        dicom_files = []
        if os.path.isfile(input_folder):
            dicom_files = [input_folder]
        elif os.path.isdir(input_folder):
            for root, dirs, files in os.walk(input_folder):
                for file in files:
                    dicom_files.append(os.path.join(root, file))
        self.logger.info(f"Found {len(dicom_files)} files to process")
        return dicom_files
    
    def _collect_actions_for_row(self, row, recipes_to_process, recipe_column_map):
        """
        Helper function to collect actions from recipe columns for a given row.
        
        Args:
            row: CSV row dictionary
            recipes_to_process: List of recipe names to process
            recipe_column_map: Dictionary mapping recipe names to CSV column names
        
        Returns:
            list: List of non-empty actions from the requested recipe columns
        """
        actions = []
        for recipe in recipes_to_process:
            if recipe not in recipe_column_map:
                continue
                
            column_name = recipe_column_map[recipe]
            action = row[column_name].strip() if row[column_name] else ""
            
            if action:  # Only add non-empty actions
                actions.append(action)
        
        return actions

    def make_recipe_file(self, recipes_to_process, recipe_folder):
        """
        Generate a deid recipe file from standard_tags_template.csv and private_tags_template.csv based on selected recipes.

        Args:
            recipes_to_process: List of recipe names to process (e.g., ['basic_profile', 'retain_uid'])
            recipe_folder: Path to the folder where the recipe file will be saved
        
        Returns:
            str: Path to the generated recipe file
        """        
        self.logger.info(f"Generating recipe file for profiles: {recipes_to_process}")
        self.logger.debug(f"Recipe output folder: {recipe_folder}")
        
        input_standard_template = os.path.join(os.path.dirname(__file__), "data", "TagsArchive", "standard_tags_template.csv")
        input_private_template = os.path.join(os.path.dirname(__file__), "data", "TagsArchive", "private_tags_template.csv")

        # Map recipe names to column names in the CSV
        recipe_column_map = {
            'basic_profile': 'Basic Prof.',
            'retain_uid': 'Rtn. UIDs Opt.',
            'retain_device_id': 'Rtn. Dev. Id. Opt.',
            'retain_institution_id': 'Rtn. Inst. Id. Opt.',
            'retain_patient_chars': 'Rtn. Pat. Chars. Opt.',
            'retain_long_full_dates': 'Rtn. Long. Full Dates Opt.',
            'retain_long_modified_dates': 'Rtn. Long. Modif. Dates Opt.',
            'clean_descriptors': 'Clean Desc. Opt.',
            'clean_structured_content': 'Clean Struct. Cont. Opt.',
            'clean_graphics': 'Clean Graph. Opt.'
        }

        if not os.path.exists(input_standard_template):
            self.logger.error(f"Input file {input_standard_template} not found")
            return None

        if not os.path.exists(input_private_template):
            self.logger.error(f"Input file {input_private_template} not found")
            return None

        # Create recipe folder if it doesn't exist
        os.makedirs(recipe_folder, exist_ok=True)
        
        # Output recipe file path
        output_file = os.path.join(recipe_folder, "deid.dicom.recipe")

        with open(output_file, 'w') as outfile:
            outfile.write("FORMAT dicom\n\n%header\n\n")
            with open(input_standard_template, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    tag = f"({row['Group']},{row['Element']})"
                    
                    name = row['Name']
                    comment = f" # {name}" if name else ""
                    vr = row['VR']
                    # Collect actions from only the requested recipe columns
                    actions = self._collect_actions_for_row(row, recipes_to_process, recipe_column_map)
                    
                    # Skip if no actions found
                    if not actions:
                        continue
                    
                    # Determine final action based on priority rules
                    final_action = None
                    
                    # If any action is 'keep', final action is 'keep'
                    if 'keep' in actions:
                        final_action = 'keep'
                    elif 'func:hash_increment_date' in actions:
                        final_action = 'func:hash_increment_date'
                    elif 'func:generate_hashuid' in actions:
                        final_action = 'func:generate_hashuid'
                    elif 'func:clean_descriptors_with_llm' in actions:
                        if vr == 'SQ':
                            # For sequences, we need manual review
                            final_action = 'manual_review'
                        else:
                            final_action = 'func:clean_descriptors_with_llm'
                    elif 'replace' in actions:
                        final_action = 'replace'
                    elif 'func:set_fixed_datetime' in actions:
                        final_action = 'func:set_fixed_datetime'
                    elif 'blank' in actions:
                        final_action = 'blank'
                    elif 'remove' in actions:
                        final_action = 'remove'
                    # Otherwise, take the first non-empty action from the priority order
                    else:
                        final_action = actions[0]
                    
                    # Write action based on the final determined action
                    line = f"{comment}\n"
                    outfile.write(line)
                    if final_action == 'keep':
                        line = f"KEEP {tag}\n"
                    elif final_action == 'remove':
                        line = f"REMOVE {tag}\n"
                    elif final_action == 'blank':
                        line = f"BLANK {tag}\n"
                    elif final_action == 'replace':
                        if  vr in ["AE", "LO", "LT", "SH", "PN", "CS", "ST", "UT", "UC", "UR"]:
                            line = f"REPLACE {tag} ANONYMIZED\n"
                        elif vr == "UN":
                            line = f"REPLACE {tag} b'Anonymized'\n"
                        elif vr in ["DS", "IS", "FD", "FL", "SS", "US", "SL", "UL"]:
                            line = f"REPLACE {tag} 0 # NEED to BE REVIEWED\n"
                        elif vr == 'AS':
                            line = f"REPLACE {tag} 000D # NEED to BE REVIEWED\n"
                        elif vr in ['SQ', 'OB']:
                            line = f"#REPLACE {tag} NEED to BE REVIEWED\n"
                    elif final_action == 'func:generate_hashuid':
                        line = f"REPLACE {tag} func:generate_hashuid\n"
                    elif final_action == 'func:set_fixed_datetime':
                        line = f"REPLACE {tag} func:set_fixed_datetime\n"
                    elif final_action == 'func:hash_increment_date':
                        line = f"JITTER {tag} func:hash_increment_date\n"
                    elif final_action == 'func:clean_descriptors_with_llm':
                        line = f"REPLACE {tag} func:clean_descriptors_with_llm\n"
                    elif final_action == 'clean_manually':
                        line = f"# REPLACE {tag} CLEANED NEEDS MANUAL REVIEW\n"
                    elif final_action == 'manual_review':
                        line = f"# REPLACE {tag} MANUAL REVIEW NEEDED\n"
                    outfile.write(line)
            
            # Add PatientIdentityRemoved if basic_profile is in the recipe list
            if 'basic_profile' in recipes_to_process:
                outfile.write("ADD PatientIdentityRemoved YES\n")
                # Set DeidentificationMethod based on examples from RSNA anonymizer:
                # ds.DeidentificationMethod = "RSNA DICOM ANONYMIZER"  # (0012,0063)
                outfile.write("ADD DeidentificationMethod LUWAK_ANONYMIZER\n")
                if 'retain_long_full_dates' not in recipes_to_process and 'retain_long_modified_dates' not in recipes_to_process:
                    outfile.write("ADD LongitudinalTemporalInformationModified REMOVED\n")
            if 'retain_long_full_dates' in recipes_to_process:
                outfile.write("ADD LongitudinalTemporalInformationModified UNMODIFIED\n")
            elif 'retain_long_modified_dates' in recipes_to_process:
                outfile.write("ADD LongitudinalTemporalInformationModified MODIFIED\n")
            if 'clean_recognizable_visual_features' in recipes_to_process:
                outfile.write("ADD RecognizableVisualFeatures NO\n")
            
            if 'retain_safe_private_tags' in recipes_to_process:
                with open(input_private_template, 'r') as privfile:
                    privreader = csv.DictReader(privfile)
                    for row in privreader:
                        private_creator = row['Private Creator']
                        group = row['Group']
                        element = row['Element'][-2:]  # Last two hex digits
                        name = row['Meaning']
                        comment = f" # {name}" if name else ""
                        line = f"{comment}\n"
                        outfile.write(line)
                        # For safe private tags, we keep them
                        line = f"KEEP ({group},\"{private_creator}\",{element})\n"
                        outfile.write(line)

            # Add the final line to remove all other private tags
            line = f"REMOVE ALL func:is_tag_private\n"
            outfile.write(line)

        self.logger.info(f"Recipe generated: {output_file}")
        return output_file

    def create_deid_recipe(self):
        """Create the deid recipe based on the recipes list.
        
        Args:
            None (uses recipes_list, recipes_folder)
            
        Returns:
            DeidRecipe: Configured deid recipe object for anonymization
            
        Supported Recipe Types:
            - 'deid.dicom': Built-in deid recipe (default)
            - 'basic_profile': Basic DICOM anonymization profile
            - 'retain_uid': Retain UIDs option
            - 'retain_device_id': Retain device identification option
            - 'retain_institution_id': Retain institution identification option
            - 'retain_patient_chars': Retain patient characteristics option
            - 'retain_long_full_dates': Retain longitudinal full dates option
            - 'retain_long_modified_dates': Retain longitudinal modified dates option
            - 'clean_descriptors': Clean descriptors option (to be checked)
            - 'clean_structured_content': Clean structured content option (to be checked)
            - 'clean_graphics': Clean graphics option (to be checked)
            - 'retain_safe_private_tags': Retain safe private tags option 
            Any final recipe will remove all other private tags not retained.
            
        Path Resolution:
            - Absolute paths: Used as-is
            - Relative paths (with /): Made relative to config file directory  
            - Filenames only: Looked up in recipes_folder
            
        Error Handling:
            - Missing recipe files: Prints warnings but continues with available recipes
            - Invalid recipe types: Treated as custom recipe filenames
        """
        recipe_paths = []
        recipes_list = self.config.get('recipes')
        recipes_folder = self.config.get('recipesFolder')
        # Use the resolved recipes folder from setup_paths
        # No need to create or join paths here

        # Handle single string recipe by converting to list
        if isinstance(recipes_list, str):
            if recipes_list == 'deid.dicom':
                self.logger.info("Using built-in deid.dicom recipe")
                return DeidRecipe()
            else:
                recipes_to_process = [recipes_list]
        else:
            recipes_to_process = recipes_list
        # Generate the recipe file in the recipes folder
        generated_recipe_file = self.make_recipe_file(recipes_to_process, recipes_folder)
        if generated_recipe_file and os.path.exists(generated_recipe_file):
            recipe_paths.append(generated_recipe_file)
            self.logger.info(f"Using generated recipe file: {generated_recipe_file}")
        else:
            self.logger.error("Failed to generate recipe file")
            return None
        missing_recipes = []
        for path in recipe_paths:
            if path != 'deid.dicom' and not os.path.exists(path):
                missing_recipes.append(path)
        if missing_recipes:
            self.logger.warning("The following recipe files are missing:")
            for missing in missing_recipes:
                self.logger.warning(f"  - {missing}")
            self.logger.warning("Continuing with available recipes...")
        # Add burned-in pixel recipe file to the recipe paths
        burnedin_recipe_path = os.path.join(os.path.dirname(__file__), "data", "BurnedPixelLocation", "deid.dicom.burnedin-pixel-recipe")
        if os.path.exists(burnedin_recipe_path):
            recipe_paths.append(burnedin_recipe_path)
            self.logger.info(f"Added burned-in pixel recipe: {burnedin_recipe_path}")
        else:
            self.logger.warning(f"Burned-in pixel recipe not found at: {burnedin_recipe_path}")
        recipe = DeidRecipe(deid=recipe_paths)
        self.logger.debug(f"Created recipe with paths: {recipe_paths}")
        self.logger.debug(f"Recipe content: {recipe}")
        return recipe
    
    def anonymize(self):
        """Perform the complete DICOM anonymization process.
        
        Args:
            None (uses all configured instance attributes)
            
        Returns:
            list: List of processed file paths from deid replace_identifiers
            
        Process Flow:
            1. Get list of DICOM files from input folder
            2. Extract DICOM identifiers using deid library
            3. Create anonymization recipe based on configuration
            4. Inject custom functions (generate_hashuid, is_tag_private) into processing
            5. Perform anonymization with deid replace_identifiers
            6. Extract metadata from anonymized files for Parquet export
            7. Save UID mappings to CSV file
            8. Export metadata to Parquet file
            
        Custom Processing:
            - Injects self.generate_hashuid for deterministic UID replacement
            - Injects self.is_tag_private for private tag detection
            - Uses configured recipes and private tag removal settings
            
        Error Handling:
            - Returns early if no files found to process
            - Continues processing even if individual files fail
            - Metadata extraction failures print warnings but don't stop process
            
        Output Files:
            - Anonymized DICOMs: Same filenames in output_directory
            - uid_mappings.csv: UID mapping table for re-identification
            - metadata.parquet: Structured metadata for analysis
        """
        # Redirect deid.bot output to the same log file used by Luwak logger
        from deid.logger import bot
        log_file_path = os.path.join(self.config['recipesFolder'], 'luwak.log')
        bot_logfile = None
        try:
            bot_logfile = open(log_file_path, "a")
            bot.outputStream = bot_logfile
            bot.errorStream = bot_logfile
            self.logger.info("Redirected deid.bot output and error streams to Luwak log file.")
        except Exception as e:
            self.logger.warning(f"Could not redirect deid.bot output: {e}")
            bot_logfile = None

        self.logger.info("=" * 50)
        self.logger.info("Starting DICOM anonymization process...")
        self.logger.info("=" * 50)
        
        input_folder = self.config.get('inputFolder')

        output_directory = self.config.get('outputDeidentifiedFolder')
        # TODO implement call to clean_recognizable_visual_features if in recipes
        # recipes_list = self.config.get('recipes')
        # if 'clean_recognizable_visual_features' in recipes_list:
        # Create a temporary directory for defaced DICOMs
        #   defaced_dir = os.path.join(output_directory, "defaced_dicom")
        #   os.makedirs(defaced_dir, exist_ok=True)

        #   self.clean_recognizable_visual_features(input_folder, defaced_dir)
        # If defaced_dir contains files, use it as the new input_folder
        #    if any(os.scandir(defaced_dir)):
        #        self.logger.info(f"Defaced DICOMs found in {defaced_dir}, using as new input folder.")
        ##        input_folder = defaced_dir
        #    else:
        #        self.logger.info(f"No defaced DICOMs found in {defaced_dir}, continuing with original input folder.")

        # Get DICOM files
        dicom_files = self.get_dicom_files(input_folder)
        
        if not dicom_files:
            self.logger.warning("No files found to process")
            # Close the bot log file if it was opened
            if bot_logfile:
                try:
                    bot_logfile.close()
                    self.logger.debug("Closed deid.bot log file.")
                except Exception as e:
                    self.logger.warning(f"Error closing bot log file: {e}")
            return
        
        # Get identifiers
        self.logger.info("Getting DICOM identifiers...")
        items = get_identifiers(dicom_files)
        
        # Create recipe
        self.logger.info("Creating anonymization recipe...")
        recipe = self.create_deid_recipe()
        for item in items:
            items[item]["is_tag_private"] = self.is_tag_private
            items[item]["generate_hashuid"] = self.generate_hashuid
            items[item]["hash_increment_date"] = self.hash_increment_date
            items[item]["set_fixed_datetime"] = self.set_fixed_datetime
            items[item]["clean_descriptors_with_llm"] = self.clean_descriptors_with_llm

        # Perform anonymization
        self.logger.info("Performing anonymization...")
        parsed_files = replace_identifiers(
            dicom_files=dicom_files, 
            deid=recipe, 
            strip_sequences=False,
            ids=items,
            remove_private=False,  # Let recipes handle private tag removal
            save=True, 
            output_folder=output_directory,
            overwrite=True,
            force=True
        )
        
        # Extract metadata from anonymized files for Parquet export
        self.logger.info("Extracting metadata for Parquet export...")
        for original_file in dicom_files:
            # Find corresponding anonymized file
            original_basename = os.path.basename(original_file)
            anonymized_file = os.path.join(output_directory, original_basename)
            
            if os.path.exists(anonymized_file):
                self.extract_dicom_metadata(original_file, anonymized_file)
        
        self.logger.info("Anonymization completed!")
        self.logger.info(f"Processed {len(parsed_files)} files")
        self.logger.info(f"Output saved to: {output_directory}")
        
        # Save all UID mappings to CSV after processing is complete
        if self.current_file_mappings:
            self.save_all_uid_mappings()
        
        # Export metadata to Parquet
        if self.dicom_metadata:
            self.export_metadata_to_parquet()
        
        self.logger.info("=" * 50)
        self.logger.info("DICOM anonymization process completed successfully!")
        self.logger.info("=" * 50)
        
        # Close the bot log file if it was opened
        if bot_logfile:
            try:
                bot_logfile.close()
                self.logger.debug("Closed deid.bot log file.")
            except Exception as e:
                self.logger.warning(f"Error closing bot log file: {e}")
        
        # Clean up the defaced_dicom directory if it exists
        #if os.path.exists(defaced_dir):
        #    import shutil
        #    try:
        #        shutil.rmtree(defaced_dir)
        #        self.logger.info(f"Temporary defaced_dicom directory removed: {defaced_dir}")
        #    except Exception as e:
        #        self.logger.warning(f"Could not remove temporary defaced_dicom directory {defaced_dir}: {e}")

        return parsed_files


if __name__ == "__main__":
    # Simple test with default config
    logger = get_logger('anonymize_main')
    logger.info("Running anonymize.py in standalone mode")
    
    try:
        anonymizer = LuwakAnonymizer("data/luwak-config.json")
        anonymizer.anonymize()
    except Exception as e:
        logger.error(f"Standalone execution failed: {e}")
        raise