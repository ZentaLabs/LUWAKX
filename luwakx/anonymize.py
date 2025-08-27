print("start")

#!/usr/bin/env python

import subprocess
import sys
import os
import re
import argparse
import json
import jsonschema
import hashlib
import csv
import pydicom
import pandas as pd
from datetime import datetime
from pydicom.datadict import add_private_dict_entry

def setup_deid_repo():
    repo_url = "https://github.com/Simlomb/deid.git"
    branch = "enhversion"
    repo_dir = os.path.expanduser("~/deid")  # Set repo_dir to the home directory

    # Check if the repository is already cloned
    if not os.path.exists(repo_dir):
        print("Cloning deid repository...")
        subprocess.check_call(["git", "clone", "--branch", branch, repo_url, repo_dir])
    else:
        # Check if the repository is already up-to-date
        print("Checking for updates in deid repository...")
        subprocess.check_call(["git", "-C", repo_dir, "fetch"])
        status = subprocess.check_output(["git", "-C", repo_dir, "status", "--porcelain", "-b"])
        if b"behind" in status:
            print("Updating deid repository...")
            subprocess.check_call(["git", "-C", repo_dir, "pull"])

    if repo_dir not in sys.path:
        sys.path.insert(0, repo_dir)
    # Check if the repository is installed
    try:
        import deid
    except ImportError:
        print("Installing deid repository...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", repo_dir])

# Call the setup function before importing deid
setup_deid_repo()

from deid.config import DeidRecipe
from deid.dicom import get_files, get_identifiers, replace_identifiers

def tag_str_to_int(tag_str):
    """
    Convert a DICOM tag string like (0010,xx10) to an integer tag value.
    
    Args:
        tag_str (str): DICOM tag string in the format '(GGGG,xxEE)', where GGGG is the group and xxEE is the element with 'xx' as a placeholder for the private block value.
            
    Returns:
        int: Integer representation of the DICOM tag.
    
    Raises:
        ValueError: If the tag_str format is invalid.
    """
    m = re.match(r'\((\w{4}),xx(\w{2})\)', tag_str)
    if not m:
        raise ValueError(f"Invalid tag format: {tag_str}")
    group = int(m.group(1), 16)
    element = int(m.group(2), 16)
    return (group << 16) | element

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
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Skip header if present
        for row in reader:
            tag_str, private_creator, vr, vm, description = row[:5]
            try:
                tag = tag_str_to_int(tag_str)
            except Exception as e:
                print(f"Skipping row {row}: {e}")
                continue
            description = name_to_keyword(description)
            add_private_dict_entry(private_creator, tag, vr, description, vm)


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
        try:
            self.load_config()
            self.setup_paths()
        except ConfigurationError as e:
            print(f"ERROR: {e}")
            sys.exit(1)
        # Initialize mapping storage for each file
        self.current_file_mappings = {}
        # Initialize metadata storage for Parquet export
        self.dicom_metadata = []
        # Initialize single date shift for entire project run
        # Register private tags from CSV
        register_private_tags_from_csv(
            os.path.join(os.path.dirname(__file__), "data", "TagsArchive", "DICOM_SAFE_PRIVATE_TAGS.csv")
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
            - Uses project_hash_root to generate single shift for entire project run
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
            # Generate shift for project run and patient
            # Use project_hash_root to generate consistent shift for this project
            project_salt = f"{project_hash_root}{PatientID}{PatientName}{PatientBirthDate}"
            salt_hash = hashlib.sha256(project_salt.encode()).hexdigest()
            hash_int = int(salt_hash[:8], 16)  # Use first 8 hex chars
            # Use configurable max_date_shift_days (default 1095)
            project_date_shift = hash_int % (self.config.get('maxDateShiftDays') + 1)  # 0 to max_date_shift_days
            return -project_date_shift
            
        except Exception as e:
            print(f"Error in date shift generation: {e}")
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
            
            if vr == 'DA':  # Date format: YYYYMMDD
                return "00010101"
            elif vr == 'DT':  # DateTime format: YYYYMMDDHHMMSS.FFFFFF&ZZXX
                return "00010101010101.000000+0000"
            elif vr == 'TM':  # Time format: HHMMSS.FFFFFF
                return "000000.00"
            else:
                # For unknown VR, return the original value
                return original_datetime_value if original_datetime_value is not None else ""
                
        except Exception as e:
            print(f"Error in fixed datetime generation: {e}")
            return ""
    

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
        except Exception as e:
            print(f"  ERROR extracting original UID: {e}")
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

        print(f"Dynamically detected {len(sorted_fields)} modified fields: {sorted_fields}")

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

        print(f"\nUID mappings saved for {len(self.current_file_mappings)} files to: {mapping_file}")
        print(f"CSV contains mappings for {len(sorted_fields)} different field types")

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
                        print(f"Skipping private tag ({elem.tag}): {e}")
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
                    
                except Exception as e:
                    # If there's any issue with this element, skip it
                    print(f"Skipping element {keyword} ({elem.tag}): {e}")
                    continue
            
            # Add to metadata collection
            self.dicom_metadata.append(metadata)
            
        except Exception as e:
            print(f"Warning: Could not extract metadata from {dicom_file}: {e}")
    
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
                print("No metadata to export")
                return
            
            # Create DataFrame from dynamic metadata
            df = pd.DataFrame(self.dicom_metadata)
            
            print(f"Dynamic Parquet schema detected {len(df.columns)} columns from retained DICOM tags")
            
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
            
            print(f"Metadata exported to Parquet: {parquet_file}")
            print(f"Exported {len(df)} DICOM metadata records with {len(df.columns)} retained tag columns")
            
            # Print schema summary for verification
            print(f"\nDynamic Parquet Schema Summary:")
            print(f"- Total columns: {len(df.columns)}")
            
            # Clear metadata for next run
            self.dicom_metadata = []
            
            return parquet_file
            
        except ImportError:
            print("Warning: pandas and pyarrow required for Parquet export. Install with: pip install pandas pyarrow")
        except Exception as e:
            print(f"Error exporting metadata to Parquet: {e}")
    
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

        print(f"\nConfiguration loaded from: {self.config_path}")
        print(f"  Config keys: {list(self.config.keys())}")

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
        print(f"Config directory (base for relative paths): {config_dir}")

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
        self.config['recipesFolder'] = recipes_folder

        # Create output directories
        os.makedirs(output_directory, exist_ok=True)
        os.makedirs(private_map_folder, exist_ok=True)

        print(f"\nFinal paths:")
        print(f"  Input folder: {input_folder}")
        print(f"  Output directory: {output_directory}")
        print(f"  Private mapping folder: {private_map_folder}")
        print(f"  Recipes folder: {recipes_folder}")

        # Validate that input and recipes folders exist
        if not os.path.exists(input_folder):
            print(f"WARNING: Input folder does not exist: {input_folder}")
        if not os.path.exists(recipes_folder):
            print(f"WARNING: Recipes folder does not exist: {recipes_folder}")
            print(f"  Make sure recipe files are available at this location or adjust the config.")
    
    def get_dicom_files(self):
        """Get all DICOM files from the input folder (using self.config)."""
        input_folder = self.config.get('inputFolder')
        if not os.path.exists(input_folder):
            print(f"ERROR: Input folder does not exist: {input_folder}")
            sys.exit(1)
        dicom_files = []
        if os.path.isfile(input_folder):
            dicom_files = [input_folder]
        elif os.path.isdir(input_folder):
            for root, dirs, files in os.walk(input_folder):
                for file in files:
                    dicom_files.append(os.path.join(root, file))
        print(f"Found {len(dicom_files)} files to process")
        return dicom_files
    
    def create_deid_recipe(self):
        """Create the deid recipe based on the recipes list.
        
        Args:
            None (uses recipes_list, recipes_folder)
            
        Returns:
            DeidRecipe: Configured deid recipe object for anonymization
            
        Supported Recipe Types:
            - 'deid.dicom': Built-in deid recipe (default)
            - 'dicom_basic_profile': Uses deid.dicom.basic-profile + removes private tags
            - 'retain_safe_private_tags': Combines safe-private-tags + remove-private-tags recipes  
            - 'retain_uids': Uses UID retention recipe
            - Custom recipes: Resolved as filename, relative path, or absolute path
            
        Path Resolution:
            - Absolute paths: Used as-is
            - Relative paths (with /): Made relative to config file directory  
            - Filenames only: Looked up in recipes_folder
            
        Error Handling:
            - Missing recipe files: Prints warnings but continues with available recipes
            - Invalid recipe types: Treated as custom recipe filenames
        """
        recipe_paths = []
        config_dir = os.path.dirname(os.path.abspath(self.config_path))
        recipes_list = self.config.get('recipes')
        recipes_folder = self.config.get('recipesFolder')

        # Handle single string recipe by converting to list
        if isinstance(recipes_list, str):
            if recipes_list == 'deid.dicom':
                print("Using built-in deid.dicom recipe")
                return DeidRecipe()
            else:
                recipes_to_process = [recipes_list]
        else:
            recipes_to_process = recipes_list

        for recipe in recipes_to_process:
            print(f"Processing recipe: {recipe}")
            if recipe == 'dicom_basic_profile':
                recipe_file = os.path.join(recipes_folder, 'deid.dicom.basic-profile')
                recipe_paths.append(recipe_file)
            elif recipe == 'retain_safe_private_tags':
                recipe_paths.append(os.path.join(recipes_folder, 'deid.dicom.safe-private-tags'))
                print("Using recipe to retain safe private tags and remove others", recipe_paths)
            elif recipe == 'retain_uids':
                recipe_file = os.path.join(recipes_folder, 'deid.dicom.retain-uids')
                recipe_paths.append(recipe_file)
            else:
                if os.path.isabs(recipe):
                    recipe_paths.append(recipe)
                elif recipe.startswith('./') or recipe.startswith('../') or '/' in recipe:
                    recipe_file = os.path.abspath(os.path.join(config_dir, recipe))
                    recipe_paths.append(recipe_file)
                else:
                    recipe_file = os.path.join(recipes_folder, recipe)
                    recipe_paths.append(recipe_file)

        missing_recipes = []
        for path in recipe_paths:
            if path != 'deid.dicom' and not os.path.exists(path):
                missing_recipes.append(path)
        if missing_recipes:
            print(f"WARNING: The following recipe files are missing:")
            for missing in missing_recipes:
                print(f"  - {missing}")
            print("Continuing with available recipes...")

        if len(recipe_paths) > 1:
            recipe = DeidRecipe(deid=recipe_paths)
        else:
            recipe = DeidRecipe(deid=recipe_paths[0])

        print(f"DEBUG: Created recipe with paths: {recipe_paths}")
        print(f"DEBUG: Recipe content: {recipe}")
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
        print("\n" + "="*50)
        print("Starting DICOM anonymization process...")
        print("="*50)
        
        # Get DICOM files
        dicom_files = self.get_dicom_files()
        
        if not dicom_files:
            print("No files found to process")
            return
        
        # Get identifiers
        print("Getting DICOM identifiers...")
        items = get_identifiers(dicom_files)
        
        # Create recipe
        print("Creating anonymization recipe...")
        recipe = self.create_deid_recipe()
        for item in items:
            items[item]["is_tag_private"] = self.is_tag_private
            #print(f'items[{item}]["is_tag_private"]:', items[item]["is_tag_private"])
            items[item]["generate_hashuid"] = self.generate_hashuid
            items[item]["hash_increment_date"] = self.hash_increment_date
            items[item]["set_fixed_datetime"] = self.set_fixed_datetime

        output_directory = self.config.get('outputDeidentifiedFolder')
        # Perform anonymization
        print("Performing anonymization...")
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
        print("Extracting metadata for Parquet export...")
        for original_file in dicom_files:
            # Find corresponding anonymized file
            original_basename = os.path.basename(original_file)
            anonymized_file = os.path.join(output_directory, original_basename)
            
            if os.path.exists(anonymized_file):
                self.extract_dicom_metadata(original_file, anonymized_file)
        
        print(f"\nAnonymization completed!")
        print(f"Processed {len(parsed_files)} files")
        print(f"Output saved to: {output_directory}")
        
        # Save all UID mappings to CSV after processing is complete
        if self.current_file_mappings:
            self.save_all_uid_mappings()
        
        # Export metadata to Parquet
        if self.dicom_metadata:
            self.export_metadata_to_parquet()
        
        return parsed_files


if __name__ == "__main__":
    # Simple test with default config
    anonymizer = LuwakAnonymizer("data/luwak-config.json")
    anonymizer.anonymize()

print("end of anonymization action")