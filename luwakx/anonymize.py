print("start")

#!/usr/bin/env python

import subprocess
import sys
import os
import argparse
import json

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


class LuwakAnonymizer:
    def __init__(self, config_path):
        """Initialize the anonymizer with configuration from JSON file."""
        self.config_path = config_path
        self.load_config()
        self.setup_paths()
        # Initialize mapping storage for each file
        self.current_file_mappings = {}
        # Initialize metadata storage for Parquet export
        self.dicom_metadata = []
    
    @staticmethod
    def is_tag_private(dicom, value, field, item):
        """Check if a DICOM tag is private."""
        return field.element.is_private and (field.element.private_creator is not None)
    
    def generate_uid(self, item, value, field, dicom):
        """Custom wrapper around pydicom_uuid with mapping storage."""
        # Use deid's pydicom_uuid for proper UID generation
        kwargs = {
            'extras': 'prefix=2.25. stable_remapping=true'  # or false for random UIDs
        }
        new_uid = pydicom_uuid(item, value, field, **kwargs)
        
        # Store the mapping for this file (will be saved later)
        file_path = item  # item is the file path
        if file_path not in self.current_file_mappings:
            self.current_file_mappings[file_path] = {}
        
        self.current_file_mappings[file_path][field.keyword] = {
            'original': value,
            'anonymized': new_uid
        }
        
        print(f"UID mapping stored: {field.keyword} {value} -> {new_uid}")
        
        return new_uid
    
    def save_all_uid_mappings(self):
        """Save all UID mappings to CSV file with one row per DICOM file."""
        import csv
        import os
        from datetime import datetime
        
        mapping_file = os.path.join(self.private_map_folder, "uid_mappings.csv")
        
        # Check if file exists to determine if we need to write headers
        file_exists = os.path.exists(mapping_file)
        
        # Define all possible UID fields
        uid_fields = ['StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID']
        
        # Create column headers
        fieldnames = ['timestamp', 'file_path']
        for field in uid_fields:
            fieldnames.extend([f'{field}_original', f'{field}_anonymized'])
        
        # Open file in append mode
        with open(mapping_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            # Write one row per file
            for file_path, mappings in self.current_file_mappings.items():
                row = {
                    'timestamp': datetime.now().isoformat(),
                    'file_path': os.path.basename(file_path)  # Just filename for readability
                }
                
                # Add mapping data for each UID field
                for field in uid_fields:
                    if field in mappings:
                        row[f'{field}_original'] = mappings[field]['original']
                        row[f'{field}_anonymized'] = mappings[field]['anonymized']
                    else:
                        # Field not present in this file
                        row[f'{field}_original'] = ''
                        row[f'{field}_anonymized'] = ''
                
                writer.writerow(row)
        
        print(f"\nUID mappings saved for {len(self.current_file_mappings)} files to: {mapping_file}")
        
        # Clear the mappings for next run
        self.current_file_mappings = {}
    
    def extract_dicom_metadata(self, dicom_file, anonymized_file_path):
        """Extract metadata from DICOM file for Parquet export - only retained tags."""
        try:
            import pydicom
            from datetime import datetime
            
            # Read the anonymized DICOM file
            ds = pydicom.dcmread(anonymized_file_path, force=True)
            
            # Start with file tracking information
            metadata = {
                'FilePath': os.path.basename(dicom_file),
                'AnonymizedFilePath': os.path.basename(anonymized_file_path),
                'ProcessingTimestamp': datetime.now().isoformat(),
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
                            value = int(elem.value) if elem.value else 0
                        except (ValueError, TypeError):
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['DS']:  # Decimal String
                        try:
                            value = float(elem.value) if elem.value else 0.0
                        except (ValueError, TypeError):
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['US', 'SS']:  # Unsigned/Signed Short
                        value = int(elem.value) if elem.value is not None else 0
                    elif elem.VR in ['UL', 'SL']:  # Unsigned/Signed Long
                        value = int(elem.value) if elem.value is not None else 0
                    elif elem.VR in ['FL', 'FD']:  # Float/Double
                        value = float(elem.value) if elem.value is not None else 0.0
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
        """Export all collected metadata to Parquet file with dynamic schema based on retained tags."""
        try:
            import pandas as pd
            from datetime import datetime
            
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
                if col in ['FilePath', 'AnonymizedFilePath', 'ProcessingTimestamp']:
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
            
            # Show data type distribution
            type_counts = df.dtypes.value_counts()
            for dtype, count in type_counts.items():
                print(f"- {dtype}: {count} columns")
            
            # Show sample of retained DICOM tag columns (excluding our tracking columns)
            dicom_columns = [col for col in df.columns if col not in 
                           ['FilePath', 'AnonymizedFilePath', 'ProcessingTimestamp', 
                            'OriginalFileSizeBytes', 'AnonymizedFileSizeBytes',
                            'HasPixelData', 'IsMultiFrame', 'IsColor', 'IsEnhanced']]
            
            if dicom_columns:
                print(f"\nRetained DICOM tag columns ({len(dicom_columns)} total):")
                # Show first 10 columns as sample
                sample_columns = dicom_columns[:10]
                print(f"Sample columns: {', '.join(sample_columns)}")
                if len(dicom_columns) > 10:
                    print(f"... and {len(dicom_columns) - 10} more")
                
                # Show sample data for a few key columns if they exist
                key_columns = [col for col in ['StudyDate', 'Modality', 'PatientSex', 'BodyPartExamined'] 
                              if col in df.columns]
                if key_columns:
                    print(f"\nSample data for key retained tags:")
                    print(df[key_columns].head(3).to_string())
            
            # Clear metadata for next run
            self.dicom_metadata = []
            
            return parquet_file
            
        except ImportError:
            print("Warning: pandas and pyarrow required for Parquet export. Install with: pip install pandas pyarrow")
        except Exception as e:
            print(f"Error exporting metadata to Parquet: {e}")
    
    def load_config(self):
        """Load and parse the JSON configuration file."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Set attributes from JSON configuration with defaults
            self.input_folder = config.get('inputFolder')
            if not self.input_folder:
                self.input_folder = './inputs'
                print("WARNING: 'inputFolder' not found in config, using default: ./inputs")
            
            self.output_directory = config.get('outputDeidentified_folder')
            if not self.output_directory:
                self.output_directory = '~/luwak_output/deidentified'
                print("WARNING: 'outputDeidentified_folder' not found in config, using default: ~/luwak_output/deidentified")
            
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
            
            self.encryption_root = config.get('encryption_root')
            if not self.encryption_root:
                self.encryption_root = ''
                print("WARNING: 'encryption_root' not found in config, using empty string")
            
            self.output_folder_hierarchy = config.get('output_folder_hierarchy')
            if not self.output_folder_hierarchy:
                self.output_folder_hierarchy = 'copy_from_input'
                print("WARNING: 'output_folder_hierarchy' not found in config, using default: copy_from_input")
            
            print(f"\nConfiguration loaded from: {self.config_path}")
            print(f"  Input folder: {self.input_folder}")
            print(f"  Output directory: {self.output_directory}")
            print(f"  Private mapping folder: {self.private_map_folder}")
            print(f"  Recipes folder: {self.recipes_folder}")
            print(f"  Recipes to apply: {self.recipes_list}")
            print(f"  Output hierarchy: {self.output_folder_hierarchy}")
            print(f"  Encryption root: {'*' * len(self.encryption_root) if self.encryption_root else 'Not set'}")
            
        except FileNotFoundError:
            print(f"ERROR: Configuration file not found: {self.config_path}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in configuration file: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: Failed to load configuration: {e}")
            sys.exit(1)
    
    def setup_paths(self):
        """Resolve and setup all paths relative to the config file location."""
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
        """Get all DICOM files from the input folder."""
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
        """Create the deid recipe based on the recipes list."""
        recipe_paths = []
        config_dir = os.path.dirname(os.path.abspath(self.config_path))
        self.remove_private = False # Default to not removing private tags unless specified in recipes
        
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
            if recipe == 'dicom_basic_profile':
                recipe_file = os.path.join(self.recipes_folder, 'deid.dicom.basic-profile')
                recipe_paths.append(recipe_file)
                self.remove_private = True  # Basic profile removes private tags
            elif recipe == 'remove_private_tags':
                self.remove_private = True
                return DeidRecipe()
            elif recipe == 'retain_safe_private_tags':
                # Look for recipe file in recipes folder
                recipe_paths.append(os.path.join(self.recipes_folder, 'deid.dicom.safe-private-tags'))
                recipe_paths.append(os.path.join(self.recipes_folder, 'deid.dicom.remove-private-tags'))
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
            recipe = DeidRecipe(deid=recipe_paths, base=True)
        else:
            recipe = DeidRecipe(deid=recipe_paths[0], base=True)

        return recipe
    
    def anonymize(self):
        """Perform the anonymization process."""
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
        
        # Set private tag handling and UID generation for specific UID fields
        uid_fields = ['StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID']
        
        for item in items:
            items[item]["is_private"] = self.is_tag_private
            
            # Only apply UID generation to Study, Series, and SOP Instance UIDs
            for field_name in uid_fields:
                if field_name in items[item]:
                    items[item][field_name]["generate_uid"] = self.generate_uid
        
        # Perform anonymization
        print("Performing anonymization...")
        parsed_files = replace_identifiers(
            dicom_files=dicom_files, 
            deid=recipe, 
            strip_sequences=False,
            ids=items,
            remove_private=self.remove_private,  # Let recipes handle private tag removal
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