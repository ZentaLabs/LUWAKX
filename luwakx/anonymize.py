print("start")

#!/usr/bin/env python

import subprocess
import sys
import os
import argparse
import json
import hashlib
import csv
import pydicom
import pandas as pd
from datetime import datetime

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
from deid.dicom.actions.uids import pydicom_uuid


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
        # Initialize single date shift for entire job run
        self._job_date_shift = None

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
    
    def generate_modified_datetime(self, item, value, field, dicom):
        """Generate single date/time shift value for entire anonymization job.
        
        Args:
            item: Item identifier from deid processing
            value: Recipe string (e.g., "func:generate_modified_datetime") - not the actual DICOM value
            field: DICOM field element containing the date/time tag
            dicom: PyDicom dataset object
            
        Returns:
            int: Number of days to shift backward (0-1095 days, consistent for entire job)
            
        Side Effects:
            - Uses self.encryption_root to generate single shift for entire job run
            - Lazy initialization - calculates shift only once per job
            - Prints shift value on first calculation
            
        Note:
            This method only returns the shift amount. The actual date manipulation
            should be handled by the DEID recipe or calling code.
            The 'value' parameter contains the recipe string, not the actual DICOM value.
        """
        try:
            # Generate single shift for entire job run (lazy initialization)
            if self._job_date_shift is None:
                # Use encryption_root to generate consistent shift for this job
                job_salt = f"{self.encryption_root}"
                salt_hash = hashlib.sha256(job_salt.encode()).hexdigest()
                hash_int = int(salt_hash[:8], 16)  # Use first 8 hex chars
                self._job_date_shift = hash_int % (3 * 365 + 1)  # 0-1095 days
                print(f"Generated job-wide date shift: {self._job_date_shift} days backward")
            
            return self._job_date_shift
            
        except Exception as e:
            print(f"Error in date shift generation: {e}")
            return 0  # Return 0 days shift on error
    
    def generate_dummy_datetime(self, item, value, field, dicom):
        """Generate dummy date/time values based on VR type for anonymization.
        
        Args:
            item: Item identifier from deid processing (not used)
            value: Recipe string (e.g., "func:generate_dummy_datetime") - not the actual DICOM value
            field: DICOM field element containing the date/time tag
            dicom: PyDicom dataset object
            
        Returns:
            str: Dummy date/time value based on VR type
            
        VR-specific Output:
            - DA (Date): Returns "00010101" (January 1, year 1)
            - DT (DateTime): Returns "00010101010101.000000+0000" (January 1, year 1, 01:01:01.000000 UTC)
            - TM (Time): Returns "000000.00" (00:00:00.00)
            
        Note:
            This method provides consistent dummy values for anonymization
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
            print(f"Error in dummy datetime generation: {e}")
            return ""
    

    def generate_uid(self, item, value, field, dicom):
        """Custom UID generation using combined salt as root for deterministic randomization.
        
        Args:
            item: Item identifier from deid processing
            value: Recipe string (e.g., "func:generate_uid") - not the actual DICOM UID
            field: DICOM field element containing the UID tag
            dicom: PyDicom dataset object
            
        Returns:
            str: New anonymized UID in format "5.25.xxx.xxx.xxx.xxx" (max 64 chars)
            
        Side Effects:
            - Stores original->anonymized UID mapping in self.current_file_mappings
            - Uses self.encryption_root as salt for deterministic generation
            
        Note:
            The 'value' parameter contains the recipe string, not the actual DICOM UID.
            The actual UID is extracted from field.element.value.
        """
        
    
        # Extract the original UID value from the DICOM field
        try:
            # The actual original UID should be in the field element's value
            if hasattr(field, 'element') and hasattr(field.element, 'value'):
                original_uid = str(field.element.value)
            elif hasattr(field, 'value'):
                original_uid = str(field.value)
            else:
                # Fallback to the value parameter, but warn about it
                original_uid = str(value) if value else "unknown"
        except Exception as e:
            print(f"  ERROR extracting original UID: {e}")
            original_uid = str(value) if value else "unknown"
        
        # Combine encryption_root and original UID as salt for deterministic generation
        combined_salt = f"{self.encryption_root}.{original_uid}"
        
        # Create a deterministic hash from the combined salt
        salt_hash = hashlib.sha256(combined_salt.encode()).hexdigest()
        
        # Use the hash as the root for UID generation
        # Take chunks of the hash and convert to decimal segments
        hash_segments = []
        for i in range(0, min(32, len(salt_hash)), 8):
            segment = salt_hash[i:i+8]
            # Convert hex to decimal and limit size for UID component
            decimal_val = int(segment, 16) % 999999999  # Keep it reasonable for UID
            hash_segments.append(str(decimal_val))
        
        # Create UID with our custom root prefix and hash-based segments
        new_uid = f"5.25.{'.'.join(hash_segments[:4])}"  # Use first 4 segments
        
        # Ensure UID doesn't exceed 64 character limit
        if len(new_uid) > 64:
            new_uid = new_uid[:64]
        
        # Store the mapping for this file (will be saved later)
        # Extract file path from the dicom dataset filename attribute
        file_path = getattr(dicom, 'filename', str(dicom))
        if file_path not in self.current_file_mappings:
            self.current_file_mappings[file_path] = {}
        
        # Get field keyword from the element
        field_keyword = getattr(field.element, 'keyword', field.element.tag)
        self.current_file_mappings[file_path][field_keyword] = {
            'original': original_uid,  # Use the extracted original UID
            'anonymized': new_uid
        }
        
        return new_uid
    
    def save_all_uid_mappings(self):
        """Save all UID mappings to CSV file with one row per DICOM file.
        
        Args:
            None (uses self.current_file_mappings and self.private_map_folder)
            
        Returns:
            None
            
        Side Effects:
            - Creates/appends to uid_mappings.csv in private mapping folder
            - CSV format: file_path, {field}_original, {field}_anonymized columns
            - Dynamically detects all modified UID fields across all processed files
            - Clears self.current_file_mappings after saving
            
        Output File:
            - CSV with headers: file_path, StudyInstanceUID_original, StudyInstanceUID_anonymized, etc.
            - One row per processed DICOM file
            - Empty cells for fields not present in specific files
        """
        
        mapping_file = os.path.join(self.private_map_folder, "uid_mappings.csv")
        
        # Check if file exists to determine if we need to write headers
        file_exists = os.path.exists(mapping_file)
        
        # Dynamically discover all modified fields across all files
        all_modified_fields = set()
        for file_path, mappings in self.current_file_mappings.items():
            all_modified_fields.update(mappings.keys())
        
        # Sort the fields for consistent column ordering
        sorted_fields = sorted(all_modified_fields)
        
        # Create column headers dynamically
        fieldnames = ['file_path']
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
                    'file_path': os.path.basename(file_path)  # Just filename for readability
                }
                
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
            
        Side Effects:
            - Reads anonymized DICOM file and extracts all retained DICOM elements
            - Appends metadata dict to self.dicom_metadata list
            - Skips file meta information (group 0x0002) and pixel data
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
            
            # Dynamically extract all retained DICOM tags using their keyword names
            # Skip file meta information and pixel data
            for elem in ds:
                if elem.tag.group == 0x0002:  # Skip file meta information
                    continue
                if elem.tag == 0x7FE00010:  # Skip pixel data
                    continue
                
                # Get the keyword name for this DICOM element
                keyword = elem.keyword
                if not keyword:  # Skip elements without keywords (private tags, etc.)
                    continue
                
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
            None (uses self.dicom_metadata and self.private_map_folder)
            
        Returns:
            str: Path to created Parquet file, or None if export failed
            
        Side Effects:
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
            parquet_file = os.path.join(self.private_map_folder, "metadata.parquet")
            
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
            
        Side Effects:
            - Sets instance attributes from JSON config with fallback defaults
            - Prints configuration summary and warnings for missing keys
            - Exits program if config file not found or invalid JSON
            
        Configuration Keys Loaded:
            - inputFolder: Source directory/file for DICOM files
            - outputDeidentifiedFolder: Destination for anonymized files  
            - outputPrivateMappingFolder: Destination for mappings/metadata
            - recipesFolder: Directory containing deid recipe files
            - recipes: List/string of recipe names to apply
            - encryptionRoot: Salt for deterministic UID generation
            - outputFolderHierarchy: How to structure output folders
            
        Error Handling:
            - FileNotFoundError: Exits with error message
            - JSONDecodeError: Exits with parse error details
            - Other exceptions: Exits with generic error message
        """
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Set attributes from JSON configuration with defaults
            self.input_folder = config.get('inputFolder')
            if not self.input_folder:
                self.input_folder = './inputs'
                print("WARNING: 'inputFolder' not found in config, using default: ./inputs")
            
            # Support both new camelCase and old snake_case for backward compatibility
            self.output_directory = config.get('outputDeidentifiedFolder') or config.get('outputDeidentified_folder')
            if not self.output_directory:
                self.output_directory = '~/luwak_output/deidentified'
                print("WARNING: 'outputDeidentifiedFolder' not found in config, using default: ~/luwak_output/deidentified")
            
            self.private_map_folder = config.get('outputPrivateMappingFolder')
            if not self.private_map_folder:
                self.private_map_folder = '~/luwak_output/privateMapping'
                print("WARNING: 'outputPrivateMappingFolder' not found in config, using default: ~/luwak_output/privateMapping")
            
            self.recipes_folder = config.get('recipesFolder')
            if not self.recipes_folder:
                self.recipes_folder = './scripts/anonymization_recipes'
                print("WARNING: 'recipesFolder' not found in config, using default: ./scripts/anonymization_recipes")
            
            self.recipes_list = config.get('recipes')
            if not self.recipes_list:
                self.recipes_list = 'deid.dicom'
                print("WARNING: 'recipes' not found in config, using default: ['deid.dicom']")
            
            # Support both new camelCase and old snake_case for backward compatibility
            self.encryption_root = config.get('encryptionRoot') or config.get('encryption_root')
            if not self.encryption_root:
                self.encryption_root = ''
                print("WARNING: 'encryptionRoot' not found in config, using empty string")
            
            # Support both new camelCase and old snake_case for backward compatibility
            self.output_folder_hierarchy = config.get('outputFolderHierarchy') or config.get('output_folder_hierarchy')
            if not self.output_folder_hierarchy:
                self.output_folder_hierarchy = 'copy_from_input'
                print("WARNING: 'outputFolderHierarchy' not found in config, using default: copy_from_input")
            
            print(f"\nConfiguration loaded from: {self.config_path}")
            print(f"  Input folder: {self.input_folder}")
            print(f"  Output directory: {self.output_directory}")
            print(f"  Private mapping folder: {self.private_map_folder}")
            print(f"  Recipes folder: {self.recipes_folder}")
            print(f"  Recipes to apply: {self.recipes_list}")
            print(f"  Output hierarchy: {self.output_folder_hierarchy}")
            print(f"  Encryption root: {'*' * len(self.encryption_root) if self.encryption_root else 'Not set'}")
            
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
    
    def setup_paths(self):
        """Resolve and setup all paths relative to the config file location.
        
        Args:
            None (uses loaded config attributes)
            
        Returns:
            None
            
        Side Effects:
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
            - self.output_directory: For anonymized DICOM files
            - self.private_map_folder: For mappings and metadata exports
            
        Validation:
            - Prints warnings if input_folder or recipes_folder don't exist
            - Does not exit on missing folders (allows processing to continue)
        """
        # Get config directory for resolving relative paths
        config_dir = os.path.dirname(os.path.abspath(self.config_path))
        
        print(f"Config directory (base for relative paths): {config_dir}")
        
        # Resolve {shared_config} placeholder with config directory
        if '{shared_config}' in self.recipes_folder:
            self.recipes_folder = self.recipes_folder.replace('{shared_config}', config_dir)
            
        # Convert all relative paths to absolute paths relative to config file
        path_fields = [
            ('input_folder', 'Input folder'),
            ('output_directory', 'Output directory'), 
            ('private_map_folder', 'Private mapping folder'),
            ('recipes_folder', 'Recipes folder')
        ]
        
        for field_name, display_name in path_fields:
            field_value = getattr(self, field_name)
            
            # Skip if already absolute
            if os.path.isabs(field_value):
                # Expand user directories for output paths
                if field_name in ['output_directory', 'private_map_folder']:
                    field_value = os.path.expanduser(field_value)
                    setattr(self, field_name, field_value)
                continue
            
            # Make relative paths absolute relative to config directory
            if field_name in ['output_directory', 'private_map_folder']:
                # For output paths, expand user directory first, then make relative to config if no ~
                if field_value.startswith('~'):
                    field_value = os.path.expanduser(field_value)
                else:
                    field_value = os.path.abspath(os.path.join(config_dir, field_value))
            else:
                # For input and recipes folders, always make relative to config
                field_value = os.path.abspath(os.path.join(config_dir, field_value))
            
            setattr(self, field_name, field_value)
            print(f"  {display_name} resolved to: {field_value}")
        
        # Create output directories
        os.makedirs(self.output_directory, exist_ok=True)
        os.makedirs(self.private_map_folder, exist_ok=True)
        
        print(f"\nFinal paths:")
        print(f"  Input folder: {self.input_folder}")
        print(f"  Output directory: {self.output_directory}")
        print(f"  Private mapping folder: {self.private_map_folder}")
        print(f"  Recipes folder: {self.recipes_folder}")
        
        # Validate that input and recipes folders exist
        if not os.path.exists(self.input_folder):
            print(f"WARNING: Input folder does not exist: {self.input_folder}")
        
        if not os.path.exists(self.recipes_folder):
            print(f"WARNING: Recipes folder does not exist: {self.recipes_folder}")
            print(f"  Make sure recipe files are available at this location or adjust the config.")
    
    def get_dicom_files(self):
        """Get all DICOM files from the input folder.
        
        Args:
            None (uses self.input_folder)
            
        Returns:
            list[str]: List of absolute paths to all files found
            
        Behavior:
            - If input_folder is a file: Returns single-item list with that file
            - If input_folder is a directory: Recursively finds all files in subdirectories
            - No DICOM format validation (processes all files found)
            
        Error Handling:
            - Exits program if input_folder doesn't exist
            - Prints count of files found for verification
            
        Note:
            Returns all files, not just .dcm files. DICOM validation happens during processing.
        """
        if not os.path.exists(self.input_folder):
            print(f"ERROR: Input folder does not exist: {self.input_folder}")
            sys.exit(1)
        
        dicom_files = []
        
        if os.path.isfile(self.input_folder):
            dicom_files = [self.input_folder]
        elif os.path.isdir(self.input_folder):
            # Recursively get all files in the directory and subdirectories
            for root, dirs, files in os.walk(self.input_folder):
                for file in files:
                    dicom_files.append(os.path.join(root, file))
        
        print(f"Found {len(dicom_files)} files to process")
        return dicom_files
    
    def create_deid_recipe(self):
        """Create the deid recipe based on the recipes list.
        
        Args:
            None (uses self.recipes_list, self.recipes_folder)
            
        Returns:
            DeidRecipe: Configured deid recipe object for anonymization
            
        Side Effects:
            - Validates recipe files exist (warnings for missing files)
            - Resolves recipe paths relative to config or absolute
            
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
        
        # Handle single string recipe by converting to list
        if isinstance(self.recipes_list, str):
            if self.recipes_list == 'deid.dicom':
                # Built-in deid recipe
                print("Using built-in deid.dicom recipe")
                return DeidRecipe()
            else:
                # Convert single string recipe to list for consistent processing
                recipes_to_process = [self.recipes_list]
        else:
            # Already a list
            recipes_to_process = self.recipes_list
        
        # Build full paths for each recipe
        for recipe in recipes_to_process:
            print(f"Processing recipe: {recipe}")
            if recipe == 'dicom_basic_profile':
                recipe_file = os.path.join(self.recipes_folder, 'deid.dicom.basic-profile')
                recipe_paths.append(recipe_file)
            elif recipe == 'retain_safe_private_tags':
                # Look for recipe file in recipes folder
                recipe_paths.append(os.path.join(self.recipes_folder, 'deid.dicom.safe-private-tags'))
                print("Using recipe to retain safe private tags and remove others", recipe_paths)
            elif recipe == 'retain_uids':
                # Look for UID retention recipe
                recipe_file = os.path.join(self.recipes_folder, 'deid.dicom.retain-uids')
                recipe_paths.append(recipe_file)
            else:
                # Handle custom recipe - could be relative or absolute path
                if os.path.isabs(recipe):
                    # Absolute path
                    recipe_paths.append(recipe)
                elif recipe.startswith('./') or recipe.startswith('../') or '/' in recipe:
                    # Relative path - make it relative to config file
                    recipe_file = os.path.abspath(os.path.join(config_dir, recipe))
                    recipe_paths.append(recipe_file)
                else:
                    # Just a filename - look in recipes folder
                    recipe_file = os.path.join(self.recipes_folder, recipe)
                    recipe_paths.append(recipe_file)
        
        # Validate recipe files exist (except built-in ones)
        missing_recipes = []
        for path in recipe_paths:
            if path != 'deid.dicom' and not os.path.exists(path):
                missing_recipes.append(path)
        
        if missing_recipes:
            print(f"WARNING: The following recipe files are missing:")
            for missing in missing_recipes:
                print(f"  - {missing}")
            print("Continuing with available recipes...")
        
        # Create the DeidRecipe
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
            4. Inject custom functions (generate_uid, is_tag_private) into processing
            5. Perform anonymization with deid replace_identifiers
            6. Extract metadata from anonymized files for Parquet export
            7. Save UID mappings to CSV file
            8. Export metadata to Parquet file
            
        Side Effects:
            - Creates anonymized DICOM files in output_directory
            - Creates uid_mappings.csv in private_map_folder
            - Creates metadata.parquet in private_map_folder
            - Prints progress information throughout process
            
        Custom Processing:
            - Injects self.generate_uid for deterministic UID replacement
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
            items[item]["generate_uid"] = self.generate_uid
            items[item]["generate_modified_datetime"] = self.generate_modified_datetime
            items[item]["generate_dummy_datetime"] = self.generate_dummy_datetime
        
        # Perform anonymization
        print("Performing anonymization...")
        parsed_files = replace_identifiers(
            dicom_files=dicom_files, 
            deid=recipe, 
            strip_sequences=False,
            ids=items,
            remove_private=False,  # Let recipes handle private tag removal
            save=True, 
            output_folder=self.output_directory,
            overwrite=True,
            force=True
        )
        
        # Extract metadata from anonymized files for Parquet export
        print("Extracting metadata for Parquet export...")
        for original_file in dicom_files:
            # Find corresponding anonymized file
            original_basename = os.path.basename(original_file)
            anonymized_file = os.path.join(self.output_directory, original_basename)
            
            if os.path.exists(anonymized_file):
                self.extract_dicom_metadata(original_file, anonymized_file)
        
        print(f"\nAnonymization completed!")
        print(f"Processed {len(parsed_files)} files")
        print(f"Output saved to: {self.output_directory}")
        
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