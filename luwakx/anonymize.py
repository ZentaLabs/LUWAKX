#!/usr/bin/env python

import subprocess
import sys
import os
import json
import jsonschema
import logging
import traceback
# Import the centralized logger
from luwak_logger import get_logger, setup_logger, log_project_stacktrace, shutdown_logging
# Import custom exceptions
from exceptions import ConfigurationError
# Import DICOM tag registry functions
from dicom_private_tag_registry import register_private_tags_from_csv
# Import LLM cache
from llm_cache import LLMResultCache
# Import patient UID database
from patient_uid_database import PatientUIDDatabase
# Import recipe builder functions
from anonymization_recipe_builder import make_recipe_file
# Import utilities
from utils import cleanup_gpu_memory


def setup_deid_repo():
    logger = get_logger('setup_deid_repo')
    # Set environment variable for deid's internal verbosity control
    if "MESSAGELEVEL" not in os.environ:
        os.environ["MESSAGELEVEL"] = "1"
    
    # Try to import deid to check if it's already available
    try:
        import deid
        deid_location = os.path.dirname(os.path.abspath(deid.__file__))
        logger.info(f"Using deid from: {deid_location}")
        return
    except ImportError:
        logger.info("deid package not found, installing from GitHub...")
    
    # Install directly from GitHub at a specific commit using pip (no --upgrade needed)
    zip_url = "https://github.com/ZentaLabs/deid/archive/547efb853b03d5e5414a07ddcef5e8bade771c50.zip"
    try:
        logger.info("Installing deid from GitHub (fixed commit)...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", zip_url],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
        
        # Force reimport to ensure we get the newly installed version
        if 'deid' in sys.modules:
            del sys.modules['deid']
        
        # Verify installation
        import deid
        deid_location = os.path.dirname(os.path.abspath(deid.__file__))
        logger.info(f"deid installed at: {deid_location}")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install deid from GitHub: {e}")
        if e.stderr:
            logger.error(f"Error details: {e.stderr.decode()}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during deid installation: {e}")
        raise


class LuwakAnonymizer:
    def __init__(self, config_path):
        """Initialize the anonymizer with configuration from JSON file.
        
        See conformance documentation:
        - LuwakAnonymizer: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#922-core-classes-and-relationships
        - Configuration: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#91-configuration-file
        """
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
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            sys.exit(1)
        
        # Initialize LLM cache if enabled
        self.llm_cache = None
        self.persistent_llm_cache = False  # Track if LLM cache should persist
        if 'clean_descriptors' in self.config.get('recipes', []):
            try:
                cache_folder = self.config.get('analysisCacheFolder')
                if cache_folder:
                    # Using persistent cache folder
                    self.persistent_llm_cache = True
                    cache_file = os.path.join(cache_folder, 'llm_cache.db')
                    self.logger.info(f"Using persistent LLM cache folder: {cache_folder}")
                else:
                    # Use temporary cache in private mapping folder
                    cache_folder = self.config.get('outputPrivateMappingFolder')
                    cache_file = os.path.join(cache_folder, 'llm_cache.db')
                    self.logger.debug("Using temporary LLM cache (will be deleted after run)")
                
                self.llm_cache = LLMResultCache(
                    cache_file_path=cache_file,
                )
                                
                # Log cache statistics
                stats = self.llm_cache.get_cache_stats()
                self.logger.info(f"LLM cache initialized: {stats['total_entries']} entries")
                self.logger.debug(f"Cache file: {cache_file}")
                
            except Exception as e:
                self.logger.warning(f"Failed to initialize LLM cache: {e}")
                self.llm_cache = None
                self.persistent_llm_cache = False
        else:
            self.logger.info("LLM caching disabled by configuration or no LLM calls requested")
        
        # Initialize patient UID database
        self.patient_uid_db = None
        self.persistent_uid_db = False  # Track if uid database should persist
        try:
            patient_id_prefix = self.config.get('patientIdPrefix', 'Zenta')
            project_hash_root = self.config.get('projectHashRoot', '')
            
            # Get analysisCacheFolder from config
            cache_folder = self.config.get('analysisCacheFolder')
            
            if cache_folder:
                # Using persistent cache folder
                self.persistent_uid_db = True
                uid_db_file = os.path.join(cache_folder, 'patient_uid.db')
                db_exists = os.path.exists(uid_db_file)
                
                self.logger.info(f"Using persistent patient UID database: {uid_db_file}")
                if db_exists:
                    self.logger.info("Existing database found - will load and update")
                else:
                    self.logger.info("No existing database - will create new persistent database")
            else:
                # Use temporary database in private mapping folder
                uid_db_folder = self.config.get('outputPrivateMappingFolder')
                uid_db_file = os.path.join(uid_db_folder, 'patient_uid.db')
                self.logger.debug("Using temporary patient UID database (will be deleted after run)")
            
            self.patient_uid_db = PatientUIDDatabase(
                db_path=uid_db_file,
                patient_id_prefix=patient_id_prefix,
                project_hash_root=project_hash_root
            )
            
            stats = self.patient_uid_db.get_stats()
            self.logger.info(f"Patient UID database initialized with prefix '{patient_id_prefix}'")
            self.logger.debug(f"Database file: {uid_db_file}")
            if stats['total_patients'] > 0:
                self.logger.info(f"Loaded {stats['total_patients']} existing patient(s) from database")
            
        except Exception as e:
            self.logger.warning(f"Failed to initialize patient UID database: {e}")
            self.patient_uid_db = None
            self.persistent_uid_db = False
        
        # Setup deid repository before any operations that need it
        self.logger.info("Setting up deid repository...")
        setup_deid_repo()
        
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
            
            # Resolve paths relative to config file
            if not os.path.isabs(output_folder):
                if output_folder.startswith('~'):
                    output_folder = os.path.expanduser(output_folder)
                else:
                    output_folder = os.path.join(config_dir, output_folder)
                        
            # Create log file path
            os.makedirs(output_folder, exist_ok=True)
            log_file_path = os.path.join(output_folder, 'luwak.log')

            # Get log level from config (with fallback to INFO)
            log_level = config.get('logLevel', 'INFO')
            
            # Configure logging with same settings as luwakx.py
            setup_logger(
                log_level=log_level,
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
            
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#91-configuration-file
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
            if key not in config and 'default' in prop:
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
        #recipes_folder = os.path.join(output_directory, os.path.basename(recipes_folder))
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
        
        # Resolve and setup analysisCacheFolder if specified
        cache_folder = self.config.get('analysisCacheFolder')
        if cache_folder:
            cache_folder = self.resolve_path(cache_folder, is_output=True)
            # Ensure directory exists
            os.makedirs(cache_folder, exist_ok=True)
            # Store resolved path in config for later use
            self.config['analysisCacheFolder'] = cache_folder
            self.logger.info(f"  Analysis cache folder: {cache_folder}")
            self.logger.info(f"    - Patient UID database: {os.path.join(cache_folder, 'patient_uid.db')}")
            if 'clean_descriptors' in self.config.get('recipes', []):
                self.logger.info(f"    - LLM cache database: {os.path.join(cache_folder, 'llm_cache.db')}")
        
        # Resolve customTags paths relative to config file
        if 'customTags' in self.config:
            if 'standard' in self.config['customTags'] and self.config['customTags']['standard']:
                resolved_standard = self.resolve_path(self.config['customTags']['standard'])
                self.config['customTags']['standard'] = resolved_standard
                self.logger.debug(f"  Manually revised standard tags: {resolved_standard}")
            if 'private' in self.config['customTags'] and self.config['customTags']['private']:
                resolved_private = self.resolve_path(self.config['customTags']['private'])
                self.config['customTags']['private'] = resolved_private
                self.logger.debug(f"  Manually revised private tags: {resolved_private}")
        
        # Log configuration info
        self.logger.info(f"Configuration loaded from: {self.config_path}")
        self.logger.debug(f"  Config keys: {list(self.config.keys())}")

        # Validate that input and recipes folders exist
        if not os.path.exists(input_folder):
            self.logger.warning(f"Input folder does not exist: {input_folder}")
        if not os.path.exists(recipes_folder):
            self.logger.warning(f"Recipes folder does not exist: {recipes_folder}")
            self.logger.warning("  Make sure recipe files are available at this location or adjust the config.")

    
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
            
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#6-deidentification-recipe-creation-pipeline-stage-3---4
        """
        # Import DeidRecipe here, after deid has been set up
        from deid.config import DeidRecipe
        
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
        # Generate the recipe file in the recipes folder, passing config for manually revised tags
        generated_recipe_file = make_recipe_file(recipes_to_process, recipes_folder, self.config)
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
        """Perform the complete DICOM anonymization process using service architecture.
        
        This method acts as a facade, orchestrating the anonymization workflow
        by delegating to two main service classes:
        - PipelineCoordinator: Manages multi-worker processing pipelines
          (internally uses ProcessingPipeline, DicomProcessor, and DefaceService)
        
        Args:
            None (uses all configured instance attributes)
            
        Returns:
            None
            
        Process Flow:
            1. Get DICOM files from input folder
            2. Create anonymization recipe
            3. Create PipelineCoordinator with service architecture
            4. Execute processing pipeline (organize → deface → anonymize)
            5. Collect results from all workers
            6. Export UID mappings, metadata, and NRRD files
            
        Note:
            Cleanup of temporary directories happens automatically within each
            ProcessingPipeline worker at the end of run_full_pipeline()
            
        Output Files:
            - Anonymized DICOMs: In output_directory/{series_folder}/
            - private/uid_mappings.csv: UID mapping table
            - private/metadata.parquet: Structured metadata
            - private/{series_folder}/image.nrrd: Original volume (if CT)
            - {series_folder}/image_defaced.nrrd: Defaced volume (if CT)
            
        See conformance documentation:
        - Pipeline Architecture: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#32-pipeline-architecture
        - Workflow: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#934-typical-workflow
        - Output Files: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#81-output-files-generated-by-luwak
        """
        from pipeline_coordinator import PipelineCoordinator

        self.logger.info("=" * 50)
        self.logger.info("Starting DICOM anonymization process...")
        self.logger.info("=" * 50)
        
        # Get configuration
        input_folder = self.config.get('inputFolder')
        output_directory = self.config.get('outputDeidentifiedFolder')
        private_folder = self.config.get('outputPrivateMappingFolder')
        num_workers = self.config.get('numWorkers', 1)
        
        # Create recipe once for all processing
        self.logger.info("Creating anonymization recipe...")
        recipe = self.create_deid_recipe()
        
        # Create and execute processing coordinator
        # Pass input_folder directly - PipelineCoordinator will discover files
        self.logger.info(f"Creating processing coordinator with {num_workers} worker(s)...")
        coordinator = PipelineCoordinator.create_from_dicom_files(
            dicom_files=input_folder,  # Can be folder path, file path, or file list
            output_directory=output_directory,
            config=self.config,
            logger=self.logger,
            num_workers=num_workers,
            llm_cache=self.llm_cache,
            patient_uid_db=self.patient_uid_db,
            recipe=recipe
        )
        
        # Execute all pipelines (streaming mode: results exported incrementally)
        self.logger.info("Executing processing pipelines...")
        coordinator.run_all_pipelines_sequential()
        
        # Finalize exports: Verify files were written correctly
        self.logger.info("Finalizing exports: Verifying export files...")
        coordinator.finalize_exports(private_folder)
                
        self.logger.info("=" * 50)
        self.logger.info("DICOM anonymization process completed successfully!")
        self.logger.info(f"Processed {len(coordinator.all_series)} series")
        self.logger.info(f"Output saved to: {output_directory}")
        self.logger.info(f"Private mappings saved to: {private_folder}")
        self.logger.info("=" * 50)
        
        # Close the LLM cache if it was initialized
        if self.llm_cache:
            try:
                stats = self.llm_cache.get_cache_stats()
                self.logger.info(f"LLM cache final stats: {stats['total_entries']} entries")
                cache_file = self.llm_cache.cache_file_path
                self.llm_cache.close()
                self.logger.debug("Closed LLM cache connection.")
                
                # Only remove cache file if NOT using persistent cache
                if not self.persistent_llm_cache:
                    if os.path.exists(cache_file):
                        os.remove(cache_file)
                        self.logger.debug("Temporary LLM cache database cleaned up")
                else:
                    self.logger.info(f"Persistent LLM cache database saved at: {cache_file}")
            except Exception as e:
                self.logger.warning(f"Error closing LLM cache: {e}")
        
        # Close and cleanup patient UID database
        if self.patient_uid_db:
            try:
                stats = self.patient_uid_db.get_stats()
                self.logger.info(f"Patient UID database final stats: {stats['total_patients']} unique patients")
                self.patient_uid_db.close()
                
                # Only remove database file if NOT using persistent database
                if not self.persistent_uid_db:
                    if os.path.exists(self.patient_uid_db.db_path):
                        os.remove(self.patient_uid_db.db_path)
                        self.logger.debug("Temporary patient UID database cleaned up")
                else:
                    self.logger.info(f"Persistent patient UID database saved at: {self.patient_uid_db.db_path}")
            except Exception as e:
                self.logger.warning(f"Error closing patient UID database: {e}")
        
        # Cleanup GPU memory to free resources for other applications
        cleanup_gpu_memory()
        
        # Shutdown logging to ensure clean exit
        shutdown_logging()
        
        # Return list of processed series for backwards compatibility
        return coordinator
    

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