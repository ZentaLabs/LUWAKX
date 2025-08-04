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


class LuwakAnonymizer:
    def __init__(self, config_path):
        """Initialize the anonymizer with configuration from JSON file."""
        self.config_path = config_path
        self.load_config()
        self.setup_paths()
    
    @staticmethod
    def is_tag_private(dicom, value, field, item):
        """Check if a DICOM tag is private."""
        return field.element.is_private and (field.element.private_creator is not None)
    
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
        
        # Set private tag handling
        for item in items:
            items[item]["is_private"] = self.is_tag_private
        
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
        
        print(f"\nAnonymization completed!")
        print(f"Processed {len(parsed_files)} files")
        print(f"Output saved to: {self.output_directory}")
        
        return parsed_files


if __name__ == "__main__":
    # Simple test with default config
    anonymizer = LuwakAnonymizer("data/luwak-config.json")
    anonymizer.anonymize()

print("end of anonymization action")