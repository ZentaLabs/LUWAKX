"""DICOM processor service for anonymization computation.

This module provides the DicomProcessor class which handles all DICOM
anonymization logic including UID generation, date shifting, descriptor
cleaning, and private tag handling.

Extracted from anonymize.py in Phase 2 refactoring.
"""

import os
import hashlib
import traceback
import importlib.util
from typing import Any, Dict, List
import pydicom

from dicom_series import DicomSeries
from luwak_logger import log_project_stacktrace


class DicomProcessor:
    """Handles DICOM anonymization computation.
    
    This service contains all anonymization algorithms and computation logic,
    separated from workflow orchestration. Each pipeline worker gets its own
    isolated instance with independent state.
    
    Attributes:
        config: Configuration dictionary (read-only)
        logger: Logger instance
        llm_cache: Shared LLM cache instance (thread-safe)
        current_file_mappings: Dictionary storing UID mappings for this worker
    """
    
    def __init__(self, config: Dict[str, Any], logger, llm_cache=None):
        """Initialize DicomProcessor.
        
        Args:
            config: Configuration dictionary
            logger: Logger instance
            llm_cache: Optional shared LLM cache instance (thread-safe)
        """
        self.config = config
        self.logger = logger
        self.llm_cache = llm_cache
        
        # Isolated state per worker (NOT shared)
        self.current_file_mappings: Dict[str, Any] = {}  # UID mappings
    
    def process_series(self, series: DicomSeries, recipe) -> None:
        """Process (anonymize) a single DICOM series using deid library.
        
        This method applies DEID recipes and custom anonymization functions
        to all files in the series. It should be called AFTER defacing (if needed).
        
        Args:
            series: DicomSeries to process
            recipe: DeidRecipe instance with anonymization rules
            
        Process:
            1. Get DICOM file paths from series (uses current location - defaced or original)
            2. Call deid.get_identifiers() to extract DICOM metadata
            3. Inject custom functions into items dict
            4. Setup progress handler for deid.bot output
            5. Call deid.replace_identifiers() to anonymize files
            6. Update series.files with anonymized paths
        """
        from deid.dicom import get_identifiers, replace_identifiers
        from deid.logger import bot
        from deid_logger_handler import DeidProgressHandler
        
        if self.logger:
            self.logger.info(f"DicomProcessor: Anonymizing series {series.folder_name}")
        
        # 1. Get file paths from series (these may be defaced files if defacing was applied)
        dicom_files = [f.get_current_path() for f in series.files]
        
        if not dicom_files:
            self.logger.warning(f"No DICOM files found for series {series.folder_name}")
            return
        
        # 2. Get identifiers using deid library
        self.logger.debug(f"Getting identifiers for {len(dicom_files)} files")
        items = get_identifiers(dicom_files, expand_sequences=True)
        
        # 3. Inject custom functions into each item
        for item in items:
            items[item]["generate_hashuid"] = self.generate_hashuid
            items[item]["hash_increment_date"] = self.hash_increment_date
            items[item]["set_fixed_datetime"] = self.set_fixed_datetime
            items[item]["clean_descriptors_with_llm"] = self.clean_descriptors_with_llm
            items[item]["is_tag_private"] = self.is_tag_private
        
        # 4. Setup progress handler to redirect deid.bot output to logger
        progress_handler = None
        try:
            progress_handler = DeidProgressHandler(
                self.logger,
                len(dicom_files),
                series_folder_name=series.folder_name
            )
            bot.outputStream = progress_handler
            bot.errorStream = progress_handler
        except Exception as e:
            self.logger.warning(f"Could not setup progress handler: {e}")
        
        # 5. Perform anonymization using deid library
        # Note: Output directory is already created in organize stage
        self.logger.info(f"Anonymizing {len(dicom_files)} files to {series.output_base_path}")
        
        try:
            parsed_files = replace_identifiers(
                dicom_files=dicom_files,
                deid=recipe,
                strip_sequences=False,
                ids=items,
                remove_private=False,  # Let recipes handle private tag removal
                save=True,
                output_folder=series.output_base_path,
                overwrite=True,
                force=True
            )
            
            if not parsed_files:
                self.logger.warning(f"No files were anonymized for series {series.folder_name}")
                return
            
            # 6. Update series files with anonymized paths
            for dicom_file in series.files:
                output_path = os.path.join(series.output_base_path, dicom_file.filename)
                dicom_file.set_anonymized_path(output_path)
            
            self.logger.info(f"Successfully anonymized {len(parsed_files)} files")
            
        except Exception as e:
            self.logger.error(f"Error during anonymization of series {series.folder_name}: {e}")
            raise
        finally:
            # Close progress handler
            if progress_handler:
                try:
                    progress_handler.close()
                except Exception as e:
                    self.logger.warning(f"Error closing progress handler: {e}")
        
        if self.logger:
            self.logger.debug(f"DicomProcessor: Completed series {series.folder_name}")
    
    # Methods to be fully implemented in Phase 2:
    
    # =================================================================
    # DEID Custom Functions - Injected into DEID recipe processing
    # =================================================================
    
    def find_sequence_path(self, ds, target_uid, target_keyword, path_prefix=""):
        """Helper to recursively search for UID in sequences and build path.
        
        Args:
            ds: PyDicom dataset to search
            target_uid: UID value to find
            target_keyword: Keyword of the tag containing the UID
            path_prefix: Current path prefix for nested sequences
            
        Returns:
            str or None: Path to the sequence element if found, None otherwise
        """
        for elem in ds:
            if elem.VR == 'SQ':
                for idx, item in enumerate(elem.value):
                    # Recursively search in sequence item
                    result = self.find_sequence_path(
                        item, target_uid, target_keyword, 
                        f"{path_prefix}{elem.keyword}[{idx}]."
                    )
                    if result:
                        return result
            else:
                if elem.keyword == target_keyword and str(elem.value) == str(target_uid):
                    return f"{path_prefix}{elem.keyword}"
        return None
    
    def generate_hashuid(self, item, value, field, dicom):
        """Custom UID generation using combined salt as root for deterministic randomization.
        
        Ensures remapping: the same original UID always maps to the same anonymized UID 
        for a given file and field.
        
        Args:
            item: Item identifier from deid processing
            value: Recipe string or value from deid processing
            field: DICOM field element containing the UID tag
            dicom: PyDicom dataset object
            
        Returns:
            str: Newly generated anonymized UID
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
            self.logger.private(
                f"Processing original UID for tag {field.element.tag} "
                f"({getattr(field.element, 'keyword', '')}): {original_uid}"
            )
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            original_uid = str(value) if value else "unknown"

        # Extract file path from the dicom dataset filename attribute
        file_path = f"{getattr(dicom, 'filename', str(dicom))}"
        if file_path not in self.current_file_mappings:
            self.current_file_mappings[file_path] = {}

        # Get field keyword from the element
        field_keyword = getattr(field.element, 'keyword', field.element.tag)

        # Check if mapping already exists for this file, field, and original UID
        mapping = self.current_file_mappings[file_path].get(field_keyword)
        if mapping and mapping.get('original') == original_uid:
            return mapping['anonymized']

        # If mapping exists but with a different UID, check for nested sequence origin
        if mapping and mapping.get('original') != original_uid:
            seq_path = self.find_sequence_path(dicom, original_uid, field_keyword)
            if seq_path:
                field_keyword = seq_path

        # Combine project_hash_root and original UID as entropy for deterministic generation
        new_uid = pydicom.uid.generate_uid(entropy_srcs=[project_hash_root, original_uid])
        self.logger.debug(
            f"Replaced tag {field.element.tag} "
            f"({getattr(field.element, 'keyword', '')}): {new_uid}"
        )
        # Log the UID mapping at PRIVATE level
        self.logger.private(f"UID mapping created - Original: {original_uid} -> Anonymized: {new_uid}")
        
        # Store the mapping for this file, field, and original UID
        self.current_file_mappings[file_path][field_keyword] = {
            'original': original_uid,
            'anonymized': new_uid
        }
        return new_uid
    
    def hash_increment_date(self, item, value, field, dicom):
        """Generate single date/time shift value for entire anonymization project.
        
        Args:
            item: Item identifier from deid processing
            value: Recipe string (e.g., "func:hash_increment_date")
            field: DICOM field element containing the date/time tag
            dicom: PyDicom dataset object
        
        Returns:
            int: Number of days to shift backward (0-maxDateShiftDays days, consistent for entire project)
        """
        project_hash_root = self.config.get('projectHashRoot')
        try:
            PatientID = dicom.get("PatientID", "")
            PatientName = dicom.get("PatientName", "")
            PatientBirthDate = dicom.get("PatientBirthDate", "")
            
            # Log sensitive patient data at PRIVATE level
            self.logger.private(
                f"Using patient data for date shift generation - "
                f"PatientID: {PatientID}, PatientName: {PatientName}, "
                f"PatientBirthDate: {PatientBirthDate}"
            )
            
            # Generate shift for project run and patient
            # Use project_hash_root to generate consistent shift for this project
            project_salt = f"{project_hash_root}{PatientID}{PatientName}{PatientBirthDate}"
            salt_hash = hashlib.sha256(project_salt.encode()).hexdigest()
            hash_int = int(salt_hash[:8], 16)  # Use first 8 hex chars
            
            # Use configurable max_date_shift_days (default 1095)
            project_date_shift = hash_int % (self.config.get('maxDateShiftDays') + 1)
            
            self.logger.debug(
                f"Replacing tag {field.element.tag} "
                f"({getattr(field.element, 'keyword', '')}) with date/time shifted."
            )
            # Log the computed shift value at PRIVATE level
            self.logger.private(
                f"For tag {field.element.tag} with value {field.element.value}, "
                f"computed date shift: -{project_date_shift} days"
            )
            return -project_date_shift
            
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            return 0  # Return 0 days shift on error
    
    def set_fixed_datetime(self, item, value, field, dicom):
        """Generate fixed date/time values based on VR type for anonymization.
        
        Args:
            item: Item identifier from deid processing (not used)
            value: Recipe string (e.g., "func:set_fixed_datetime")
            field: DICOM field element containing the date/time tag
            dicom: PyDicom dataset object
            
        Returns:
            str: fixed date/time value based on VR type
            
        VR-specific Output:
            - DA (Date): Returns "00010101" (January 1, year 1)
            - DT (DateTime): Returns "00010101010101.000000+0000"
            - TM (Time): Returns "000000.00"
        """
        try:
            # Get the VR type from the field
            vr = field.element.VR if hasattr(field, 'element') else None
            tag_str = getattr(field.element, 'tag', 'unknown') if hasattr(field, 'element') else 'unknown'
            keyword_str = getattr(field.element, 'keyword', '') if hasattr(field, 'element') else ''
            
            if vr == 'DA':  # Date format: YYYYMMDD
                self.logger.debug(f"Setting fixed date for tag {tag_str} ({keyword_str}) to '00010101'.")
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(
                        f"Setting fixed date for tag {tag_str} ({keyword_str}) "
                        f"with value {field.element.value} to '00010101'."
                    )
                return "00010101"
            elif vr == 'DT':  # DateTime format: YYYYMMDDHHMMSS.FFFFFF&ZZXX
                self.logger.debug(
                    f"Setting fixed datetime for tag {tag_str} ({keyword_str}) "
                    f"to '00010101010101.000000+0000'."
                )
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(
                        f"Setting fixed datetime for tag {tag_str} ({keyword_str}) "
                        f"with value {field.element.value} to '00010101010101.000000+0000'."
                    )
                return "00010101010101.000000+0000"
            elif vr == 'TM':  # Time format: HHMMSS.FFFFFF
                self.logger.debug(f"Setting fixed time for tag {tag_str} ({keyword_str}) to '000000.00'.")
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(
                        f"Setting fixed time for tag {tag_str} ({keyword_str}) "
                        f"with value {field.element.value} to '000000.00'."
                    )
                return "000000.00"
            else:
                # For unknown VR, return the original value
                original_value = field.element.value if hasattr(field, 'element') and hasattr(field.element, 'value') else ""
                self.logger.warning(f"Unknown VR type '{vr}' for tag {tag_str}, returning original value.")
                return str(original_value) if original_value is not None else ""
                
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            return ""
    
    def clean_descriptors_with_llm(self, item, value, field, dicom):
        """Clean descriptive text fields using a large language model (LLM) and PHI/PII detector.
        
        Uses shared LLM cache for efficiency across all workers.
        
        Args:
            item: Item identifier from deid processing (not used)
            value: Recipe string (e.g., "func:clean_descriptors_with_llm")
            field: DICOM field element containing the text tag
            dicom: PyDicom dataset object
        
        Returns:
            str or None: Cleaned text value, or None if PHI/PII detected (element deleted)

         Note:
            - Uses LLM to clean descriptive text fields with persistent caching
            - Calls PHI/PII detector to check if cleaned text still contains sensitive info
            - If PHI/PII detected, deletes the element and returns ""
            - If no PHI/PII detected, returns original text value
            - Results are cached to avoid redundant LLM calls
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
            self.logger.private(
                f"Processing original value for tag {field.element.tag} "
                f"({getattr(field.element, 'keyword', '')}): {original_value}"
            )
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            original_value = str(value) if value else "unknown"
        
        # Get LLM config
        base_url = self.config.get('cleanDescriptorsLlmBaseUrl', "https://api.openai.com/v1")
        model = self.config.get('cleanDescriptorsLlmModel', "gpt-4o-mini")
        api_key_env = self.config.get('cleanDescriptorsLlmApiKeyEnvVar', "")
        api_key = os.environ.get(api_key_env, "")
        
        # Check shared cache first
        result = None
        if self.llm_cache:
            result = self.llm_cache.get_cached_result(original_value, model)
            self.logger.debug(
                f"LLM cache result for tag {field.element.tag} "
                f"({getattr(field.element, 'keyword', '')}): {result}"
            )
        
        if result is None:
            # No cache hit - proceed with LLM call
            try:
                # Import detector module
                detector_path = os.path.join(
                    os.path.dirname(__file__), "scripts", "detector", "detector.py"
                )
                spec = importlib.util.spec_from_file_location("detector", detector_path)
                detector = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(detector)
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                log_project_stacktrace(self.logger, e)
                return original_value
        
            try:
                client = OpenAI(base_url=base_url, api_key=api_key)
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                log_project_stacktrace(self.logger, e)
                return original_value
        
            try:
                # Prepare input for PHI/PII detection
                tag_desc = (
                    f"{getattr(field.element, 'tag', '')} "
                    f"{getattr(field.element, 'keyword', '')}: {original_value}"
                )
                # Call the LLM for PHI/PII detection
                result = detector.detect_phi_or_pii(client, tag_desc, model=model, dev_mode=False)
                self.logger.private(f"PHI/PII detection result for tag {tag_desc}: {result}")
            
                # Store result in shared cache
                if self.llm_cache:
                    try:
                        self.llm_cache.store_result(original_value, model, int(str(result).strip()))
                    except Exception as cache_error:
                        self.logger.warning(f"Failed to cache LLM result: {cache_error}")
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
                log_project_stacktrace(self.logger, e)
                return original_value

        # Apply result
        if str(result).strip() == "1":
            # PHI detected - remove/anonymize
            try:
                del dicom[field.element.tag]
                self.logger.debug(
                    f"Removed tag {field.element.tag} ({getattr(field.element, 'keyword', '')}) "
                    f"from DICOM file as the detector found PHI information in its text."
                )
                return None
            except Exception as e:
                self.logger.warning(f"Failed to remove element {field.element.tag}: {e}")
                self.logger.debug(
                    f"Replaced tag {field.element.tag} ({getattr(field.element, 'keyword', '')}) "
                    f"to 'ANONYMIZED' as the detector found PHI information in its text."
                )
                return "ANONYMIZED"
        else:
            # No PHI detected - keep original
            self.logger.debug(
                f"Keeping original value for tag {field.element.tag} "
                f"({getattr(field.element, 'keyword', '')})."
            )
            return original_value
    
    def is_tag_private(self, dicom, value, field, item):
        """Check if a DICOM tag is private.
        
        Args:
            dicom: PyDicom dataset object containing DICOM data
            value: Recipe string or value from recipe processing
            field: DICOM field element containing tag information
            item: Item identifier from deid processing
            
        Returns:
            bool: True if the tag is private (has private creator), False otherwise
        """
        # Log private tag details at PRIVATE level if it exists
        if field.element.is_private and (field.element.private_creator is not None):
            if hasattr(field.element, 'value'):
                self.logger.private(f"Removed private tag {field.element.tag} with value: {field.element.value}")
            return True
        return False
    
    # ============================================================================
    # Streaming Export Support Methods (Memory Management)
    # ============================================================================
    
    def get_series_uid_mappings(self, series: DicomSeries) -> Dict[str, Any]:
        """Get UID mappings for a specific series only.
        
        For streaming export, we need series-specific data. This method
        extracts mappings that belong to the given series based on file paths.
        
        Args:
            series: DicomSeries to get mappings for
            
        Returns:
            Dictionary of UID mappings for this series only
        """
        # For now, return all mappings since they're series-specific
        # In the future, we could filter by series if needed
        series_mappings = dict(self.current_file_mappings)
        return series_mappings
    
    def clear_series_data(self, series: DicomSeries) -> None:
        """Clear data for a processed series to free memory.
        
        After exporting series results, we clear its data from memory
        to keep memory usage constant regardless of dataset size.
        
        Args:
            series: DicomSeries whose data should be cleared
        """
        # Clear UID mappings for this series
        self.current_file_mappings.clear()
        
        if self.logger:
            self.logger.debug(f"Cleared memory for series {series.folder_name}")
