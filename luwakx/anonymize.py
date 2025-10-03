#!/usr/bin/env python

import shutil
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
import sqlite3
import threading
import time
import traceback
from datetime import datetime
from pydicom.datadict import add_private_dict_entry

# Import the centralized logger
from luwak_logger import get_logger, setup_logger, get_log_file_path


class LLMResultCache:
    """
    Thread-safe SQLite-based cache for LLM results in DICOM anonymization.
    
    Provides persistent caching of LLM PHI/PII detection results to avoid
    redundant API calls across parallel processing and multiple runs.
    """
    
    def __init__(self, cache_file_path, project_hash_root="", cache_ttl_days=30):
        """
        Initialize the LLM result cache.
        
        Args:
            cache_file_path (str): Path to SQLite cache file
            project_hash_root (str): Project hash root for cache key generation
            cache_ttl_days (int): Cache TTL in days (default: 30)
        """
        self.cache_file_path = cache_file_path
        self.project_hash_root = project_hash_root
        self.cache_ttl_days = cache_ttl_days
        self.logger = get_logger('llm_cache')
        
        # Thread-local storage for database connections
        self._local = threading.local()
        
        # Initialize database
        self._init_database()
    
    def _get_connection(self):
        """Get thread-local database connection."""
        if not hasattr(self._local, 'connection'):
            # Create connection with thread-safe settings
            self._local.connection = sqlite3.connect(
                self.cache_file_path,
                timeout=30.0,  # 30 second timeout
                check_same_thread=False
            )
            # Enable WAL mode for better concurrent access
            self._local.connection.execute("PRAGMA journal_mode=WAL")
            self._local.connection.execute("PRAGMA synchronous=NORMAL")
            self._local.connection.execute("PRAGMA busy_timeout=30000")  # 30 seconds
        return self._local.connection
    
    def _init_database(self):
        """Initialize the SQLite database with required schema."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.cache_file_path), exist_ok=True)
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create cache table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS llm_phi_cache (
                    cache_key TEXT PRIMARY KEY,
                    input_text TEXT NOT NULL,
                    llm_model TEXT NOT NULL,
                    phi_result INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    project_hash TEXT NOT NULL
                )
            """)
            
            # Create index for faster lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_cache_key_project 
                ON llm_phi_cache(cache_key, project_hash)
            """)
            
            # Create index for cleanup by timestamp
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at 
                ON llm_phi_cache(created_at)
            """)
            
            conn.commit()
            self.logger.debug(f"Initialized LLM cache database: {self.cache_file_path}")
            
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"Failed to initialize LLM cache database: {e}{line_info}")
            raise
    
    def _generate_cache_key(self, input_text, model):
        """
        Generate a deterministic cache key for the input.
        
        Args:
            input_text (str): Input text to cache
            model (str): LLM model name
            
        Returns:
            str: Hexadecimal cache key
        """
        # Combine input text, and model for uniqueness
        key_source = f"{model}:{input_text}"
        return hashlib.sha256(key_source.encode('utf-8')).hexdigest()
    
    def get_cached_result(self, input_text, model):
        """
        Retrieve cached LLM result if available and not expired.
        
        Args:
            input_text (str): Input text to check
            model (str): LLM model name
            
        Returns:
            int or None: Cached PHI result (0/1) or None if not cached/expired
        """
        try:
            cache_key = self._generate_cache_key(input_text, model)
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Query with expiration check
            cursor.execute("""
                SELECT phi_result 
                FROM llm_phi_cache 
                WHERE cache_key = ? 
                  AND datetime(created_at, '+{} days') > datetime('now')
            """.format(self.cache_ttl_days), (cache_key,))
            
            result = cursor.fetchone()
            if result:
                self.logger.debug(f"Cache HIT for key: {cache_key[:16]}...")
                return result[0]
            
            self.logger.debug(f"Cache MISS for key: {cache_key[:16]}...")
            return None
            
        except Exception as e:
            self.logger.warning(f"Error retrieving from LLM cache: {e}")
            return None
    
    def store_result(self, input_text, model, phi_result):
        """
        Store LLM result in cache.
        
        Args:
            input_text (str): Input text that was processed
            model (str): LLM model name used
            phi_result (int): PHI detection result (0 or 1)
        """
        try:
            cache_key = self._generate_cache_key(input_text, model)
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Use INSERT OR REPLACE to handle duplicates
            cursor.execute("""
                INSERT OR REPLACE INTO llm_phi_cache 
                (cache_key, input_text, llm_model, phi_result, project_hash, created_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
            """, (cache_key, input_text, model, phi_result, self.project_hash_root))
            
            conn.commit()
            self.logger.debug(f"Cached result for key: {cache_key[:16]}... -> {phi_result}")
            
        except Exception as e:
            self.logger.warning(f"Error storing to LLM cache: {e}")
    
    def cleanup_expired(self):
        """Remove expired entries from cache."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM llm_phi_cache 
                WHERE datetime(created_at, '+{} days') <= datetime('now')
            """.format(self.cache_ttl_days))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                self.logger.info(f"Cleaned up {deleted_count} expired cache entries")
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up LLM cache: {e}")
    
    def get_cache_stats(self):
        """Get cache statistics."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_entries,
                    COUNT(CASE WHEN datetime(created_at, '+{} days') > datetime('now') THEN 1 END) as valid_entries,
                    MAX(created_at) as latest_entry
                FROM llm_phi_cache 
                WHERE project_hash = ?
            """.format(self.cache_ttl_days), (self.project_hash_root,))
            
            result = cursor.fetchone()
            return {
                'total_entries': result[0] or 0,
                'valid_entries': result[1] or 0,
                'latest_entry': result[2],
                'cache_file': self.cache_file_path
            }
            
        except Exception as e:
            self.logger.warning(f"Error getting cache stats: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Close database connection for current thread."""
        if hasattr(self._local, 'connection'):
            try:
                self._local.connection.close()
                delattr(self._local, 'connection')
            except Exception as e:
                self.logger.warning(f"Error closing LLM cache connection: {e}")
    
    def __del__(self):
        """Destructor to ensure database connections are closed."""
        try:
            self.close()
        except:
            pass  # Ignore errors during cleanup


def setup_deid_repo():
    logger = get_logger('setup_deid_repo')
    
    repo_url = "https://github.com/ZentaLabs/deid.git"
    branch = "speed-optimization"
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
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"Configuration error: {e}{line_info}")
            sys.exit(1)
            
        # Initialize mapping storage for each file
        self.current_file_mappings = {}
        # Initialize metadata storage for Parquet export
        self.dicom_metadata = []
        
        # Initialize LLM cache if enabled
        self.llm_cache = None        
        if 'clean_descriptors' in self.config.get('recipes', []):
            try:
                cache_folder = self.config.get('llmCacheFolder')
                cache_file = os.path.join(cache_folder, 'llm_cache.db')
                cache_ttl_days = self.config.get('llmCacheTtlDays', 30)
                project_hash_root = self.config.get('projectHashRoot', '')
                
                self.llm_cache = LLMResultCache(
                    cache_file_path=cache_file,
                    project_hash_root=project_hash_root,
                    cache_ttl_days=cache_ttl_days
                )
                
                # Clean up expired entries on startup
                self.llm_cache.cleanup_expired()
                
                # Log cache statistics
                stats = self.llm_cache.get_cache_stats()
                self.logger.info(f"LLM cache initialized: {stats['valid_entries']}/{stats['total_entries']} valid entries")
                self.logger.debug(f"Cache file: {cache_file}")
                
            except Exception as e:
                self.logger.warning(f"Failed to initialize LLM cache: {e}")
                self.llm_cache = None
        else:
            self.logger.info("LLM caching disabled by configuration or no LLM calls requested")
        
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
        # Log private tag details at PRIVATE level if it exists
        if field.element.is_private and (field.element.private_creator is not None):
            if hasattr(field.element, 'value'):
                self.logger.private(f"Removed private tag {field.element.tag} with value: {field.element.value}")
                # self.logger.info(f"Removed private tag {field.element.tag} ({getattr(field.element, 'name', '')}).")
            return True
        return False
    
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
            self.logger.debug(f"Replacing tag {field.element.tag} ({getattr(field.element, 'keyword', '')}) with date/time shifted.")
            # Log the computed shift value at PRIVATE level
            self.logger.private(f"For tag {field.element.tag} with value {field.element.value}, computed date shift: -{project_date_shift} days")
            return -project_date_shift
            
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"Error in date shift generation: {e}{line_info}")
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
                self.logger.debug(f"Setting fixed date for tag {tag_str} ({keyword_str}) to '00010101'.")
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(f"Setting fixed date for tag {tag_str} ({keyword_str}) with value {field.element.value} to '00010101'.")
                return "00010101"
            elif vr == 'DT':  # DateTime format: YYYYMMDDHHMMSS.FFFFFF&ZZXX
                self.logger.debug(f"Setting fixed datetime for tag {tag_str} ({keyword_str}) to '00010101010101.000000+0000'.")
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(f"Setting fixed datetime for tag {tag_str} ({keyword_str}) with value {field.element.value} to '00010101010101.000000+0000'.")
                return "00010101010101.000000+0000"
            elif vr == 'TM':  # Time format: HHMMSS.FFFFFF
                self.logger.debug(f"Setting fixed time for tag {tag_str} ({keyword_str}) to '000000.00'.")
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
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"Error in fixed datetime generation: {e}{line_info}")
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
                - Uses LLM to clean descriptive text fields with persistent caching
                - Calls PHI/PII detector to check if cleaned text still contains sensitive info
                - If PHI/PII detected, deletes the element and returns ""
                - If no PHI/PII detected, returns original text value
                - Results are cached to avoid redundant LLM calls
                The 'value' parameter contains the recipe string, not the actual DICOM value.
        """
        from openai import OpenAI
        
        # Extract original value
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
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"  ERROR extracting original value: {e}{line_info}")
            original_value = str(value) if value else "unknown"
        
        # Get LLM config from self.config
        base_url = self.config.get('cleanDescriptorsLlmBaseUrl', "https://api.openai.com/v1")
        model = self.config.get('cleanDescriptorsLlmModel', "gpt-4o-mini")
        api_key_env = self.config.get('cleanDescriptorsLlmApiKeyEnvVar', "")
        api_key = os.environ.get(api_key_env, "")
        
        # Check cache first
        result = None
        if self.llm_cache:
            result = self.llm_cache.get_cached_result(original_value, model)
            self.logger.debug(f"LLM cache result for tag {field.element.tag} ({getattr(field.element, 'keyword', '')}): {result}")
        if result==None:
            # No cache hit - proceed with LLM call
            try:
                # Import detector module
                detector_path = os.path.join(os.path.dirname(__file__), "scripts", "detector", "detector.py")
                spec = importlib.util.spec_from_file_location("detector", detector_path)
                detector = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(detector)
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                self.logger.error(f"Failed to import detector.py: {e}{line_info}")
                return original_value
        
            try:
                client = OpenAI(base_url=base_url, api_key=api_key)
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                self.logger.error(f"Failed to initialize OpenAI client: {e}{line_info}")
                return original_value
        
            try:
                # Prepare input for PHI/PII detection
                tag_desc = f"{getattr(field.element, 'tag', '')} {getattr(field.element, 'keyword', '')}: {original_value}" 
                # Call the LLM for PHI/PII detection
                result = detector.detect_phi_or_pii(client, tag_desc, model=model, dev_mode=False)
                self.logger.private(f"PHI/PII detection result for tag {tag_desc} : {result}")
            
                # Store result in cache
                if self.llm_cache:
                    try:
                        self.llm_cache.store_result(original_value, model, int(str(result).strip()))
                    except Exception as cache_error:
                        self.logger.warning(f"Failed to cache LLM result: {cache_error}")
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                self.logger.error(f"Error in PHI/PII detection: {e}{line_info}")
                return original_value


        # Apply result
        if str(result).strip() == "1":
            # PHI detected - remove/anonymize
            try:
                del dicom[field.element.tag]
                self.logger.debug(f"Removed tag {field.element.tag} ({getattr(field.element, 'keyword', '')}) from DICOM file as the detector found PHI information in its text.")
                return None
            except Exception as e:
                    self.logger.warning(f"Failed to remove element {field.element.tag}: {e}")
                    self.logger.debug(f"Replaced tag {field.element.tag} ({getattr(field.element, 'keyword', '')}) to 'ANONYMIZED' as the detector found PHI information in its text.")
                    return "ANONYMIZED"
        else:
            # No PHI detected - keep original
            self.logger.debug(f"Keeping original value for tag {field.element.tag} ({getattr(field.element, 'keyword', '')}).")
            return original_value
                
    def clean_recognizable_visual_features(self, input_folder, output_dir):
        """Clean tags that may contain recognizable visual features using a defacing ML model or an existing mask.
           
           Args:
                input_folder: Directory containing DICOM files organized by series (from get_dicom_files)
                output_dir: Directory to save defaced DICOM files (maintains folder structure)
            
            Returns:
                list: List of processed DICOM file paths

            Note:
                - Uses defacing ML model to clean data
                - Expects input_folder to be organized by SeriesInstanceUID in subfolders
                - Maintains the folder structure in output_dir
        """
        if not os.path.exists(input_folder):
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = traceback.extract_tb(exc_traceback)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"Input folder does not exist: {input_folder}{line_info}")
            return None
        
        # Import required modules
        import SimpleITK
        try:
            defacer_path = os.path.join(os.path.dirname(__file__), "scripts", "defacing", "image_defacer", "image_anonymization.py")
            spec = importlib.util.spec_from_file_location("image_anonymization", defacer_path)
            defacer = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(defacer)
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"Failed to import image_anonymization.py: {e}{line_info}")
            return None
        
        processed_files = []
        reader = SimpleITK.ImageSeriesReader()
        
        # Process each series folder in the input directory
        series_folders = [f for f in os.listdir(input_folder) if os.path.isdir(os.path.join(input_folder, f))]
        
        if not series_folders:
            self.logger.warning(f"No series folders found in {input_folder}")
            return []
        
        self.logger.info(f"Processing {len(series_folders)} series folders for defacing...")
        
        series_count = 0
        for series_folder_name in series_folders:
            series_folder_path = os.path.join(input_folder, series_folder_name)
            
            # Create corresponding output folder
            output_series_folder = os.path.join(output_dir, series_folder_name)
            os.makedirs(output_series_folder, exist_ok=True)
            
            # Get all DICOM files in this series folder
            try:
                series_ids = reader.GetGDCMSeriesIDs(series_folder_path)
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                self.logger.error(f"Failed to get DICOM series IDs in {series_folder_path}: {e}{line_info}")
                # Copy files without defacing if GDCM fails
                self._copy_files(series_folder_path, output_series_folder, processed_files)
                continue

            if not series_ids:
                self.logger.warning(f"No DICOM series found in {series_folder_path}")
                # Copy files without defacing
                self._copy_files(series_folder_path, output_series_folder, processed_files)
                continue
            
            # Process each series in the folder (typically just one per folder)
            for series_id in series_ids:
                try:
                    dicom_filenames = reader.GetGDCMSeriesFileNames(series_folder_path, series_id)
                except Exception as e:
                    tb = traceback.extract_tb(e.__traceback__)
                    line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                    self.logger.error(f"Failed to get DICOM filenames for series {series_id} in {series_folder_path}: {e}{line_info}")
                    continue
                
                if not dicom_filenames:
                    self.logger.warning(f"No files found for series {series_id} in {series_folder_name}")
                    continue
                    
                try:
                    ds = pydicom.dcmread(dicom_filenames[0])
                    modality = ds.Modality if 'Modality' in ds else None
                    body_part = ds.BodyPartExamined if 'BodyPartExamined' in ds else None
                except Exception as e:
                    tb = traceback.extract_tb(e.__traceback__)
                    line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                    self.logger.error(f"Failed to read DICOM file {dicom_filenames[0]}: {e}{line_info}")
                    continue
                    
                if modality.upper() == "CT": #and body_part.upper() in ["HEAD", "BRAIN", "FACE", "NECK"]:
                    series_count += 1
                    try:
                        self.logger.info(f"Defacing series number {series_count} in folder {series_folder_name}.")
                        self.logger.private(f"Defacing series {series_id} in {series_folder_name} with modality {modality} and body part {body_part}.")
                        
                        reader.SetFileNames(dicom_filenames)
                        image = reader.Execute()
                        image_face_segmentation = defacer.prepare_face_mask(image, modality)
                        image_defaced = defacer.pixelate_face(image, image_face_segmentation)
                        nrrd_image_path = os.path.join(output_series_folder, "image.nrrd")
                        nrrd_defaced_path = os.path.join(output_series_folder, "image_defaced.nrrd")
                        SimpleITK.WriteImage(image, nrrd_image_path)
                        SimpleITK.WriteImage(image_defaced, nrrd_defaced_path)
                        defaced_array = SimpleITK.GetArrayFromImage(image_defaced) # Shape: [slices, height, width]
                        
                        self.logger.info(f"Defacing series number {series_count} completed.")
                        
                        # For each slice, copy metadata and replace pixel data
                        for i, dicom_file in enumerate(dicom_filenames):
                            try:
                                ds = pydicom.dcmread(dicom_file)
                                rescale_slope = getattr(ds, 'RescaleSlope', 1.0)
                                rescale_intercept = getattr(ds, 'RescaleIntercept', 0.0)
                                # Apply inverse scaling to get back to raw values
                                raw_pixels = ((defaced_array[i] - rescale_intercept) / rescale_slope).round().astype(ds.pixel_array.dtype)
                                ds.PixelData = raw_pixels.tobytes()
                                
                                # Save to output folder maintaining structure
                                output_path = os.path.join(output_series_folder, os.path.basename(dicom_file))
                                ds.save_as(output_path)
                                processed_files.append(output_path)
                                
                            except Exception as e:
                                tb = traceback.extract_tb(e.__traceback__)
                                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                                self.logger.error(f"Failed to save defaced DICOM for {dicom_file}: {e}{line_info}")
                                continue
                                
                        self.logger.info(f"Defaced DICOM series number {series_count} saved in {output_series_folder}")
                        
                    except Exception as e: 
                        tb = traceback.extract_tb(e.__traceback__)
                        line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                        self.logger.error(f"Defacing failed for series {series_id} in {series_folder_name}: {e}{line_info}")
                        # Copy files without defacing as fallback
                        self._copy_files(dicom_filenames, output_series_folder, processed_files)
                        continue
                        
                else:
                    series_count += 1
                    self.logger.info(f"Skipping defacing for modality {modality} in series folder {series_folder_name}.")
                    # Copy all files without defacing
                    self._copy_files(dicom_filenames, output_series_folder, processed_files)
        
        self.logger.info(f"Visual feature cleaning completed. Processed {len(processed_files)} files across {series_count} series.")
        return processed_files
    
    def _copy_files(self, source_files, dest_folder, processed_files_list=None):
        """Copy files to destination folder.
        
        Args:
            source_files (str or list): Either a source folder path or list of file paths
            dest_folder (str): Destination folder path
            processed_files_list (list, optional): List to append copied file paths to
            
        Returns:
            list: List of copied file paths
        """
        if processed_files_list is None:
            processed_files_list = []
        
        # Ensure destination folder exists
        os.makedirs(dest_folder, exist_ok=True)
        
        # Handle both folder path and file list inputs
        if isinstance(source_files, str):
            # Source is a folder - copy all files from it
            if not os.path.exists(source_files):
                self.logger.warning(f"Source folder does not exist: {source_files}")
                return processed_files_list
            
            files_to_copy = []
            try:
                for file in os.listdir(source_files):
                    source_file = os.path.join(source_files, file)
                    if os.path.isfile(source_file):
                        files_to_copy.append(source_file)
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                self.logger.error(f"Failed to list files in {source_files}: {e}{line_info}")
                return processed_files_list
        else:
            # Source is a list of files
            files_to_copy = source_files
        
        # Copy each file
        for src_file in files_to_copy:
            try:
                dest_file = os.path.join(dest_folder, os.path.basename(src_file))
                shutil.copy2(src_file, dest_file)
                processed_files_list.append(dest_file)
                self.logger.debug(f"Copied file to output: {dest_file}")
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                self.logger.error(f"Failed to copy {src_file} to {dest_folder}: {e}{line_info}")
        
        return processed_files_list

    def _get_directory_contents(self, directory_path, content_type="both"):
        """Get directory contents with flexible filtering.
        
        Args:
            directory_path (str): Path to directory
            content_type (str): What to return - "folders", "files", or "both"
            
        Returns:
            list: List of paths or names based on content_type
        """
        if not os.path.exists(directory_path):
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = traceback.extract_tb(exc_traceback)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"Directory does not exist: {directory_path}{line_info}")
            return []
        
        try:
            all_items = os.listdir(directory_path)
            
            if content_type == "folders":
                items = [f for f in all_items if os.path.isdir(os.path.join(directory_path, f))]
                item_type = "folders"
            elif content_type == "files":
                items = [os.path.join(directory_path, f) for f in all_items if os.path.isfile(os.path.join(directory_path, f))]
                item_type = "files"
            else:  # both
                items = all_items
                item_type = "items"
            
            if not items:
                self.logger.warning(f"No {item_type} found in {directory_path}")
                return []
            
            self.logger.debug(f"Found {len(items)} {item_type} in {os.path.basename(directory_path)}: {items}")
            return items
            
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"Error reading directory {directory_path}: {e}{line_info}")
            return []

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
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            self.logger.error(f"  ERROR extracting original UID: {e}{line_info}")
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
        self.logger.debug(f"Replaced tag {field.element.tag} ({getattr(field.element, 'keyword', '')}): {new_uid}")
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
                # Calculate the relative path from output folder (including series subfolder)
                output_folder = self.config.get('outputDeidentifiedFolder')
                try:
                    # Get the expected output path for this input file
                    expected_output_path = self.get_output_path_for_file(file_path)
                    # Calculate relative path from output folder
                    rel_path = os.path.relpath(expected_output_path, output_folder)
                    # Ensure we use forward slashes for consistent path representation
                    rel_path = rel_path.replace(os.sep, '/')
                except Exception:
                    rel_path = os.path.basename(file_path)
                
                row = {
                    'file_path': rel_path
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
            
            # Compute relative path to output folder for AnonymizedFilePath
            output_folder = self.config.get('outputDeidentifiedFolder')
            try:
                rel_path = os.path.relpath(anonymized_file_path, output_folder)
                # Ensure we use forward slashes for consistent path representation
                rel_path = rel_path.replace(os.sep, '/')
            except Exception:
                rel_path = os.path.basename(anonymized_file_path)
            metadata = {
                'AnonymizedFilePath': rel_path,
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
                        if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                            # Multi-value date field - convert to string list representation
                            value = str(list(elem.value)) if elem.value else ''
                        else:
                            # Single date value
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['TM']:  # Time
                        if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                            # Multi-value time field - convert to string list representation
                            value = str(list(elem.value)) if elem.value else ''
                        else:
                            # Single time value
                            value = str(elem.value) if elem.value else ''
                    elif elem.VR in ['DT']:  # DateTime
                        if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                            # Multi-value datetime field - convert to string list representation
                            value = str(list(elem.value)) if elem.value else ''
                        else:
                            # Single datetime value
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
                try:
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
                        except Exception as e:
                            self.logger.warning(f"Column '{col}': failed Int64 conversion: {e}")
                    # Check if it's all floats
                    if all(isinstance(v, (int, float)) for v in sample_values):
                        try:
                            df[col] = df[col].astype('float64')
                            continue
                        except Exception as e:
                            self.logger.warning(f"Column '{col}': failed float64 conversion: {e}")
                    # Convert dates to proper datetime format if they look like DICOM dates
                    if col.endswith('Date'):
                        is_date_column = True
                        for v in sample_values:
                            if isinstance(v, str):
                                # Handle single date: "20210715"
                                if len(v) == 8 and v.isdigit():
                                    continue
                                # Handle multi-value date string: "['20210715', '20210506']"
                                elif v.startswith('[') and v.endswith(']'):
                                    try:
                                        import ast
                                        date_list = ast.literal_eval(v)
                                        if isinstance(date_list, list) and all(isinstance(d, str) and len(d) == 8 and d.isdigit() for d in date_list):
                                            continue
                                    except Exception as e:
                                        self.logger.warning(f"Column '{col}': failed to parse multi-value date string '{v}': {e}")
                            is_date_column = False
                            break
                        if is_date_column:
                            try:
                                # Convert to string format - keep multi-value dates as string for now
                                # since pandas datetime doesn't handle multi-value fields well
                                df[col] = df[col].astype('string')
                                continue
                            except Exception as e:
                                self.logger.warning(f"Column '{col}': failed string conversion for date: {e}")
                    # Default to string for everything else
                    try:
                        df[col] = df[col].astype('string')
                    except Exception as e:
                        self.logger.warning(f"Column '{col}': failed string conversion: {e}")
                except Exception as e:
                    tb = traceback.extract_tb(e.__traceback__)
                    line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                    self.logger.error(f"Error processing column '{col}' during type optimization: {e}{line_info}")
                    continue
            
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
        if 'clean_descriptors' in self.config.get('recipes', []):
            cache_folder = self.config.get('llmCacheFolder')
            cache_folder = self.resolve_path(cache_folder, is_output=True)
            self.config['llmCacheFolder'] = cache_folder
            os.makedirs(cache_folder, exist_ok=True)
            self.logger.info(f"  LLM cache folder: {cache_folder}")
        # Log configuration info
        self.logger.info(f"Configuration loaded from: {self.config_path}")
        self.logger.debug(f"  Config keys: {list(self.config.keys())}")

        # Validate that input and recipes folders exist
        if not os.path.exists(input_folder):
            self.logger.warning(f"Input folder does not exist: {input_folder}")
        if not os.path.exists(recipes_folder):
            self.logger.warning(f"Recipes folder does not exist: {recipes_folder}")
            self.logger.warning("  Make sure recipe files are available at this location or adjust the config.")
    
    def get_dicom_files(self, input_folder, create_series_structure=False):
        """Get all DICOM files from the input folder, optionally organizing them by SeriesInstanceUID.
        
        Args:
            input_folder (str): Path to input folder containing DICOM files
            create_series_structure (bool): If True, creates a folder structure organized by SeriesInstanceUID
            
        Returns:
            list: List of DICOM file paths, organized by series if create_series_structure=True
        """
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
        
        if not create_series_structure:
            self.logger.info(f"Found {len(dicom_files)} files to process")
            return dicom_files
        
        # Group files by SeriesInstanceUID and create organized structure
        return self._create_series_organized_structure(dicom_files, input_folder)
    
    def _create_series_organized_structure(self, dicom_files, input_folder):
        """Create a folder structure organized by SeriesInstanceUID.
        
        Args:
            dicom_files (list): List of DICOM file paths
            input_folder (str): Original input folder path
            
        Returns:
            list: List of organized DICOM file paths in series-based structure
        """
        
        # Create a temporary organized structure in the output directory
        output_directory = self.config.get('outputDeidentifiedFolder')
        organized_input_dir = os.path.join(output_directory, "temp_organized_input")
        
        # Clean up any existing organized directory
        if os.path.exists(organized_input_dir):
            shutil.rmtree(organized_input_dir)
        os.makedirs(organized_input_dir, exist_ok=True)
        
        # Group files by SeriesInstanceUID
        series_groups = {}
        series_folder_names = {}  # Track folder names for each series
        
        self.logger.info("Organizing DICOM files by SeriesInstanceUID...")
        
        for file_path in dicom_files:
            try:
                ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                series_uid = getattr(ds, 'SeriesInstanceUID', 'unknown_series')
                
                # Clean series UID for folder name (remove invalid characters)
                clean_series_uid = "".join(c for c in series_uid if c.isalnum() or c in ".-_").rstrip()
                if not clean_series_uid:
                    clean_series_uid = "unknown_series"
                
                # Create a meaningful folder name using additional DICOM info
                try:
                    series_desc = getattr(ds, 'SeriesDescription', '')
                    series_number = getattr(ds, 'SeriesNumber', '')
                    modality = getattr(ds, 'Modality', '')
                    
                    # Create folder name: SeriesNumber_Modality_SeriesDescription
                    folder_parts = []
                    if series_number:
                        folder_parts.append(f"{series_number:03d}" if isinstance(series_number, int) else str(series_number))
                    if modality:
                        folder_parts.append(modality)
                    if series_desc:
                        # Clean series description for folder name
                        clean_desc = "".join(c for c in series_desc if c.isalnum() or c in " -_").strip()
                        clean_desc = "_".join(clean_desc.split())  # Replace spaces with underscores
                        if clean_desc:
                            folder_parts.append(clean_desc)
                    
                    if folder_parts:
                        folder_name = "_".join(folder_parts)
                    else:
                        folder_name = clean_series_uid
                        
                    # Ensure folder name is not too long
                    if len(folder_name) > 100:
                        folder_name = folder_name[:100] + "_" + clean_series_uid[-10:]
                        
                except Exception as e:
                    self.logger.warning(f"Could not create descriptive folder name for {file_path}: {e}")
                    folder_name = clean_series_uid
                
                # Store the folder name for this series (use first file's folder name)
                if series_uid not in series_folder_names:
                    series_folder_names[series_uid] = folder_name
                
                if series_uid not in series_groups:
                    series_groups[series_uid] = []
                
                series_groups[series_uid].append(file_path)
                
            except Exception as e:
                self.logger.warning(f"Could not read DICOM file {file_path}: {e}")
                # Add to unknown series group
                if 'unknown' not in series_groups:
                    series_groups['unknown'] = []
                    series_folder_names['unknown'] = 'unknown_series'
                series_groups['unknown'].append(file_path)
        
        # Create organized folder structure and copy files
        organized_files = []
        
        for series_uid, files in series_groups.items():
            folder_name = series_folder_names[series_uid]
            series_folder = os.path.join(organized_input_dir, folder_name)
            os.makedirs(series_folder, exist_ok=True)
            
            self.logger.debug(f"Created series folder: {series_folder} for SeriesInstanceUID: {series_uid}")
            
            for file_path in files:
                try:
                    # Copy file to organized structure
                    filename = os.path.basename(file_path)
                    organized_file_path = os.path.join(series_folder, filename)
                    shutil.copy2(file_path, organized_file_path)
                    organized_files.append(organized_file_path)
                except Exception as e:
                    self.logger.warning(f"Could not copy file {file_path} to organized structure: {e}")
        
        self.logger.info(f"Organized {len(organized_files)} files into {len(series_groups)} series folders")
        self.logger.info(f"Organized structure created at: {organized_input_dir}")
        
        return organized_files
    
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
                    tag = (f"({row['Group']},{row['Element']})").upper()
                    
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
                        group = row['Group'].upper()
                        element = row['Element'][-2:].upper()  # Last two hex digits
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

        log_file_path = get_log_file_path()
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
        recipes_list = self.config.get('recipes')
        
        # Check if input is a single file or directory
        single_file_processing = os.path.isfile(input_folder)
        
        if single_file_processing:
            self.logger.info("Processing single DICOM file...")
            dicom_files = self.get_dicom_files(input_folder, create_series_structure=False)
            organized_files = dicom_files
            input_folder_for_processing = input_folder
        else:
            # Always organize DICOM files by SeriesInstanceUID for better volume handling
            self.logger.info("Organizing DICOM files by SeriesInstanceUID...")
            organized_files = self.get_dicom_files(input_folder, create_series_structure=True)
            input_folder_for_processing = input_folder
        
        if not organized_files:
            self.logger.warning("No files found to process")
            if bot_logfile:
                try:
                    bot_logfile.close()
                    self.logger.debug("Closed deid.bot log file.")
                except Exception as e:
                    self.logger.warning(f"Error closing bot log file: {e}")
            return
        
        if single_file_processing:
            # For single files, get the organized input directory or use the original file
            input_folder = input_folder_for_processing
        else:
            # Get the organized input directory (temp_organized_input)
            organized_input_dir = os.path.join(output_directory, "temp_organized_input")
            input_folder = organized_input_dir
        
        # Check if visual features cleaning is needed
        if 'clean_recognizable_visual_features' in recipes_list:
            if single_file_processing:
                self.logger.warning("Visual features cleaning not supported for single file processing. Skipping.")
                dicom_files = organized_files
            else:
                # Create a temporary directory for defaced DICOMs with organized structure
                defaced_dir = os.path.join(output_directory, "temp_defaced_organized")
                os.makedirs(defaced_dir, exist_ok=True)
                
                self.logger.info("Cleaning recognizable visual features from organized DICOM files...")
                processed_files = self.clean_recognizable_visual_features(input_folder, defaced_dir)
                
                if processed_files:
                    self.logger.info(f"Visual features cleaned. Using {len(processed_files)} processed files for anonymization.")
                    dicom_files = processed_files
                    # Update input_folder to point to the defaced organized structure
                    input_folder = defaced_dir
                else:
                    self.logger.warning("No files were processed during visual features cleaning. Using original organized files.")
                    dicom_files = organized_files
        else:
            # Use organized files directly without visual features cleaning
            if single_file_processing:
                self.logger.info("Using single DICOM file for anonymization (no visual features cleaning).")
            else:
                self.logger.info("Using organized DICOM files for anonymization (no visual features cleaning).")
            dicom_files = organized_files
        
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
        
        # Create recipe once for all processing
        self.logger.info("Creating anonymization recipe...")
        recipe = self.create_deid_recipe()
        
        if single_file_processing:
            # Process single file directly
            self.logger.info(f"Anonymizing single file: {input_folder}")
            
            # Get identifiers for the single file
            items = get_identifiers([input_folder], expand_sequences=True)
            
            # Inject custom functions
            for item in items:
                items[item]["is_tag_private"] = self.is_tag_private
                items[item]["generate_hashuid"] = self.generate_hashuid
                items[item]["hash_increment_date"] = self.hash_increment_date
                items[item]["set_fixed_datetime"] = self.set_fixed_datetime
                items[item]["clean_descriptors_with_llm"] = self.clean_descriptors_with_llm

            # Perform anonymization
            self.logger.info(f"Anonymizing single file to {output_directory}")
            all_parsed_files = replace_identifiers(
                dicom_files=[input_folder], 
                deid=recipe, 
                strip_sequences=False,
                ids=items,
                remove_private=False,  # Let recipes handle private tag removal
                save=True, 
                output_folder=output_directory,
                overwrite=True,
                force=True
            )
            
            if all_parsed_files:
                self.logger.info(f"Completed anonymizing single file: {len(all_parsed_files)} files processed")
            else:
                all_parsed_files = []
                
        else:
            # Process series folders separately to preserve structure
            self.logger.info("Processing series folders individually to preserve folder structure...")
            
            # Get series folders and process each one
            series_folders = self._get_directory_contents(input_folder, "folders")
            
            if not series_folders:
                self.logger.error(f"No series folders found in {input_folder}")
                if bot_logfile:
                    try:
                        bot_logfile.close()
                        self.logger.debug("Closed deid.bot log file.")
                    except Exception as e:
                        self.logger.warning(f"Error closing bot log file: {e}")
                return
            
            self.logger.info(f"Anonymizing {len(series_folders)} series folders...")
            
            all_parsed_files = []
            total_processed_files = 0
            
            for series_folder_name in series_folders:
                series_folder_path = os.path.join(input_folder, series_folder_name)
                series_output_path = os.path.join(output_directory, series_folder_name)
                # Paths where NRRD files are initially saved (if applicable)
                nrrd_image_src = os.path.join(series_folder_path, "image.nrrd")
                nrrd_defaced_src = os.path.join(series_folder_path, "image_defaced.nrrd")
                # Paths where NRRD files should be moved
                nrrd_image_dst = os.path.join(series_output_path, "image.nrrd")
                nrrd_defaced_dst = os.path.join(series_output_path, "image_defaced.nrrd")

                # Create output directory for this series
                os.makedirs(series_output_path, exist_ok=True)
                # Move files if source and destination differ
                if os.path.exists(nrrd_image_src):
                    shutil.move(nrrd_image_src, nrrd_image_dst)
                if os.path.exists(nrrd_defaced_src):
                    shutil.move(nrrd_defaced_src, nrrd_defaced_dst)
                # Get files in this series folder
                series_files = self._get_directory_contents(series_folder_path, "files")
                
                if not series_files:
                    # Remove the series_output_path directory if needed
                    shutil.rmtree(series_output_path)
                    self.logger.warning(f"No files found in series folder: {series_folder_name}")
                    continue
                                
                # Process this series
                try:
                    self.logger.info(f"Processing series '{series_folder_name}' with {len(series_files)} files...")
                    
                    # Get identifiers for this series
                    series_items = get_identifiers(series_files, expand_sequences=True)
                    
                    # Inject custom functions for this series
                    for item in series_items:
                        series_items[item]["is_tag_private"] = self.is_tag_private
                        series_items[item]["generate_hashuid"] = self.generate_hashuid
                        series_items[item]["hash_increment_date"] = self.hash_increment_date
                        series_items[item]["set_fixed_datetime"] = self.set_fixed_datetime
                        series_items[item]["clean_descriptors_with_llm"] = self.clean_descriptors_with_llm

                    # Perform anonymization for this series
                    self.logger.info(f"Anonymizing series '{series_folder_name}' to {series_output_path}")
                    series_parsed_files = replace_identifiers(
                        dicom_files=series_files, 
                        deid=recipe, 
                        strip_sequences=False,
                        ids=series_items,
                        remove_private=False,  # Let recipes handle private tag removal
                        save=True, 
                        output_folder=series_output_path,
                        overwrite=True,
                        force=True
                    )
                    
                    if series_parsed_files:
                        all_parsed_files.extend(series_parsed_files)
                        total_processed_files += len(series_files)
                        self.logger.info(f"Completed anonymizing series '{series_folder_name}': {len(series_parsed_files)} files processed")
                    
                except Exception as e:
                    self.logger.error(f"Error processing series '{series_folder_name}': {e}")
                    continue
            
            series_count = len(series_folders)
            self.logger.info(f"Processed {total_processed_files} files across {series_count} series")
        
        self.logger.info("Anonymization completed!")
        self.logger.info(f"Processed {len(all_parsed_files)} files")
        self.logger.info(f"Output saved to: {output_directory}")
        
        # Extract metadata from anonymized files for Parquet export
        self.logger.info("Extracting metadata for Parquet export...")
        
        if single_file_processing:
            # For single file, extract metadata directly
            original_file = input_folder_for_processing
            anonymized_file = os.path.join(output_directory, os.path.basename(original_file))
            
            if os.path.exists(anonymized_file):
                self.extract_dicom_metadata(original_file, anonymized_file)
            else:
                self.logger.warning(f"Could not find anonymized file for: {original_file}")
        else:
            # For series folders, extract metadata maintaining structure
            # Use helper method to get all files from organized structure for metadata extraction
            series_folders = self._get_directory_contents(input_folder, "folders")
            for series_folder_name in series_folders:
                series_folder_path = os.path.join(input_folder, series_folder_name)
                series_files = self._get_directory_contents(series_folder_path, "files")
                if series_files:
                    original_file = series_files[0]  # Only process the first file
                    relative_path = os.path.join(series_folder_name, os.path.basename(original_file))
                    anonymized_file = os.path.join(output_directory, relative_path)
                    if os.path.exists(anonymized_file):
                        self.extract_dicom_metadata(original_file, anonymized_file)
                    else:
                        self.logger.warning(f"Could not find anonymized file for: {original_file}")
        
        # Save all UID mappings to CSV after processing is complete
        if self.current_file_mappings:
            self.save_all_uid_mappings()
        
        # Export metadata to Parquet
        if self.dicom_metadata:
            try:
                self.export_metadata_to_parquet()
            except Exception as e:
                self.logger.error(f"Error exporting metadata to Parquet: {e}")

        self.logger.info("=" * 50)
        self.logger.info("DICOM anonymization process completed successfully!")
        self.logger.info("=" * 50)
        
        # Close the LLM cache if it was initialized
        if self.llm_cache:
            try:
                # Final cleanup of expired entries
                self.llm_cache.cleanup_expired()
                # Get final cache statistics
                stats = self.llm_cache.get_cache_stats()
                self.logger.info(f"LLM cache final stats: {stats['valid_entries']} valid entries")
                # Close cache connection
                self.llm_cache.close()
                self.logger.debug("Closed LLM cache connection.")
            except Exception as e:
                self.logger.warning(f"Error closing LLM cache: {e}")
        
        # Close the bot log file if it was opened
        if bot_logfile:
            try:
                bot_logfile.close()
                self.logger.debug("Closed deid.bot log file.")
            except Exception as e:
                self.logger.warning(f"Error closing bot log file: {e}")
        
        # Clean up temporary directories if they exist
        temp_dirs_to_clean = []
        
        if not single_file_processing:
            # Always clean up organized input directory since we always use it for multiple files
            organized_input_dir = os.path.join(output_directory, "temp_organized_input")
            if os.path.exists(organized_input_dir):
                temp_dirs_to_clean.append(("organized input", organized_input_dir))
        
        # Clean up defaced organized directory if visual features cleaning was used
        if 'clean_recognizable_visual_features' in recipes_list and not single_file_processing:
            defaced_dir = os.path.join(output_directory, "temp_defaced_organized")
            if os.path.exists(defaced_dir):
                temp_dirs_to_clean.append(("defaced organized", defaced_dir))
        
        for dir_type, dir_path in temp_dirs_to_clean:
            try:
                shutil.rmtree(dir_path)
                self.logger.info(f"Temporary {dir_type} directory removed: {dir_path}")
            except Exception as e:
                self.logger.warning(f"Could not remove temporary {dir_type} directory {dir_path}: {e}")

        return all_parsed_files

    def get_output_path_for_file(self, input_file_path):
        """Get the expected output path for a given input file, accounting for series organization.
        
        Args:
            input_file_path (str): Path to the original input file
            
        Returns:
            str: Expected path where the anonymized file will be located
            
        Note:
            - For single files: Returns path in output directory (no series organization)
            - For multiple files: Returns path in series subfolder based on SeriesInstanceUID
            - This method reads the DICOM file to determine the series organization
        """
        output_directory = self.config.get('outputDeidentifiedFolder')
        
        # Check if input was a single file
        input_folder = self.config.get('inputFolder')
        if os.path.isfile(input_folder):
            # Single file processing - output goes directly to output directory
            return os.path.join(output_directory, os.path.basename(input_file_path))
        else:
            # Multiple files processing - need to determine series folder
            try:
                # Read DICOM to get SeriesInstanceUID
                ds = pydicom.dcmread(input_file_path, stop_before_pixels=True)
                series_uid = getattr(ds, 'SeriesInstanceUID', 'unknown_series')
                
                # Create the same folder name logic as in _create_series_organized_structure
                clean_series_uid = "".join(c for c in series_uid if c.isalnum() or c in ".-_").rstrip()
                if not clean_series_uid:
                    clean_series_uid = "unknown_series"
                
                # Create meaningful folder name using additional DICOM info
                try:
                    series_desc = getattr(ds, 'SeriesDescription', '')
                    series_number = getattr(ds, 'SeriesNumber', '')
                    modality = getattr(ds, 'Modality', '')
                    
                    # Create folder name: SeriesNumber_Modality_SeriesDescription
                    folder_parts = []
                    if series_number:
                        folder_parts.append(f"{series_number:03d}")
                    if modality:
                        folder_parts.append(modality)
                    if series_desc:
                        # Clean series description for folder name
                        clean_desc = "".join(c for c in series_desc if c.isalnum() or c in " -_").strip()
                        clean_desc = "_".join(clean_desc.split())  # Replace spaces with underscores
                        if clean_desc:
                            folder_parts.append(clean_desc[:30])  # Limit length
                    
                    if folder_parts:
                        folder_name = "_".join(folder_parts)
                    else:
                        folder_name = clean_series_uid
                        
                except Exception:
                    folder_name = clean_series_uid
                
                # Return path in series subfolder
                return os.path.join(output_directory, folder_name, os.path.basename(input_file_path))
                
            except Exception as e:
                self.logger.warning(f"Could not determine series folder for {input_file_path}: {e}")
                # Fallback to direct output path
                return os.path.join(output_directory, os.path.basename(input_file_path))


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