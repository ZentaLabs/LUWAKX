"""DICOM processor service for anonymization computation.

This module provides the DicomProcessor class which handles all DICOM
anonymization logic including UID generation, date shifting, descriptor
cleaning, and private tag handling.

Extracted from anonymize.py in Phase 2 refactoring.
"""

import os
import gc
import hashlib
import hmac
import traceback
import importlib.util
from typing import Any, Dict
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
    
    def __init__(self, config: Dict[str, Any], logger, llm_cache=None, patient_uid_db=None):
        """Initialize DicomProcessor.
        
        Args:
            config: Configuration dictionary
            logger: Logger instance
            llm_cache: Optional shared LLM cache instance (thread-safe)
            patient_uid_db: Optional shared patient UID database instance (thread-safe)
        """
        self.config = config
        self.logger = logger
        self.llm_cache = llm_cache
        self.patient_uid_db = patient_uid_db
        self.series = None
        
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
        
        self.series = series
        series_display = f"series:{self.series.anonymized_series_uid}, of study:{self.series.anonymized_study_uid}, for patient:{self.series.anonymized_patient_id}"
        self.logger.info(f"DicomProcessor: Anonymizing series {series_display}")
        
        # 1. Get file paths from series (these may be defaced files if defacing was applied)
        dicom_files = [f.get_current_path() for f in self.series.files]
        
        if not dicom_files:
            self.logger.warning(f"No DICOM files found for series {series_display}")
            return
        
        # 2. Get identifiers using deid library
        self.logger.debug(f"Getting identifiers for {len(dicom_files)} files")
        items = get_identifiers(dicom_files, expand_sequences=True)
        
        # 3. Inject custom functions into each item
        for item in items:
            items[item]["generate_hmacuid"] = self.generate_hmacuid
            items[item]["generate_patient_id"] = self.generate_patient_id
            items[item]["generate_hmacdate_shift"] = self.generate_hmacdate_shift
            items[item]["set_fixed_datetime"] = self.set_fixed_datetime
            items[item]["clean_descriptors_with_llm"] = self.clean_descriptors_with_llm
            items[item]["is_tag_private"] = self.is_tag_private
            items[item]["is_curve_or_overlay_tag"] = self.is_curve_or_overlay_tag
        
        # 4. Setup progress handler to redirect deid.bot output to logger
        progress_handler = None
        try:
            series_folder = os.path.basename(self.series.output_base_path)
            progress_handler = DeidProgressHandler(
                self.logger,
                len(dicom_files),
                series_folder_name=series_folder
            )
            bot.outputStream = progress_handler
            bot.errorStream = progress_handler
        except Exception as e:
            self.logger.warning(f"Could not setup progress handler: {e}")
        
        # 5. Perform anonymization using deid library
        # Note: Output directory is already created in organize stage
        self.logger.info(f"Anonymizing {len(dicom_files)} files to {self.series.output_base_path}")
        
        try:
            parsed_files = replace_identifiers(
                dicom_files=dicom_files,
                deid=recipe,
                strip_sequences=False,
                ids=items,
                remove_private=False,  # Let recipes handle private tag removal
                save=True,
                output_folder=self.series.output_base_path,
                overwrite=True,
                force=True
            )
            
            if not parsed_files:
                self.logger.warning(f"No files were anonymized for series {series_display}")
                return
            
            # 6. Update series files with anonymized paths
            for dicom_file in self.series.files:
                output_path = os.path.join(self.series.output_base_path, dicom_file.filename)
                dicom_file.set_anonymized_path(output_path)
            
            self.logger.info(f"Successfully anonymized {len(parsed_files)} files")
            
        except Exception as e:
            series_display = os.path.basename(self.series.output_base_path)
            self.logger.error(f"Error during anonymization of series {series_display}: {e}")
            raise
        finally:
            # Close progress handler
            if progress_handler:
                try:
                    progress_handler.close()
                except Exception as e:
                    self.logger.warning(f"Error closing progress handler: {e}")
            
            # Free large memory structures immediately after anonymization completes
            try:
                del items
                del parsed_files
                gc.collect()
                self.logger.debug("Freed items and parsed_files memory after anonymization")
            except (NameError, UnboundLocalError):
                # Variables may not exist if exception occurred before they were created
                pass
            
            # NOTE: self.series and current_file_mappings remain in memory for export stage
            # They are cleared later in clear_series_data() after export completes
            
        self.logger.debug(f"DicomProcessor: Completed series {series_display}")
    
    # =================================================================
    # DEID Custom Functions - Injected into DEID recipe processing
    # =================================================================
    
    def generate_patient_id(self, item, value, field, dicom):
        """Generate consistent patient ID using patient UID database.
        
        This function is injected into deid recipe processing and can be called
        via REPLACE statements in recipe files.
        
        Args:
            item: Item identifier from deid processing
            value: Recipe string (e.g., "func:generate_patient_id")
            field: DICOM field element containing the PatientID tag
            dicom: PyDicom dataset object
            
        Returns:
            str: Anonymized patient ID (e.g., "Zenta00", "Zenta01")
            
        Example recipe usage:
            REPLACE PatientID func:generate_patient_id
        """
        if not self.patient_uid_db:
            # Fallback if database not initialized
            self.logger.warning("Patient UID database not initialized, using default ID")
            return "ANON00"
        
        # Extract original identifiers from DICOM dataset
        original_patient_id = self.series.original_patient_id
        original_patient_name = self.series.original_patient_name
        original_patient_birthdate = self.series.original_patient_birthdate
        # Check cache first (read-only, thread-safe for concurrent reads)
        cached_result = self.patient_uid_db.get_cached_patient_id(
            original_patient_id,
            original_patient_name,
            original_patient_birthdate
        )
        if cached_result is None:
            # No cache hit - create and store new ID and random token
            self.logger.debug(
                f"No cached patient ID found, creating new entry for tag {field.element.tag}"
            )
            # Log original values at PRIVATE level
            self.logger.private(
                f"Generating patient ID from - PatientID: {original_patient_id}, "
                f"PatientName: {original_patient_name}, BirthDate: {original_patient_birthdate}"
            )
            anonymized_id, _ = self.patient_uid_db.store_patient_id(
                original_patient_id,
                original_patient_name,
                original_patient_birthdate
            )
            self.logger.warning(
                f"Generating patient ID from - PatientID: {original_patient_id}, "
                f"PatientName: {original_patient_name}, BirthDate: {original_patient_birthdate}, new ID: {anonymized_id}"
            )

        else:
            anonymized_id, _ = cached_result
            self.logger.debug(
                f"Using cached patient ID for tag {field.element.tag}"
            )
        self.logger.debug(
            f"Generated patient ID for tag {field.element.tag} "
            f"({getattr(field.element, 'keyword', '')}): {anonymized_id}"
        )
        return anonymized_id
    
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
    
    def _compute_hmac(self, key: bytes, project_root: str, original_uid: str) -> str:
        """Compute HMAC-SHA512 of project_root and original UID using patient's secret key.
        
        Args:
            key: Raw bytes secret key (from patient UID database random token)
            project_root: Project hash root for isolation
            original_uid: Original DICOM UID to be anonymized
            
        Returns:
            Hex string of HMAC-SHA512 digest for use as entropy (128 hex characters)
        """
        # Use original UID as-is to maintain 1:1 mapping uniqueness        
        # Combine with separator to avoid ambiguity
        data = f"{project_root}||{original_uid}".encode('utf-8')
        
        # Compute HMAC-SHA512 and return hex string (pydicom expects strings)
        mac = hmac.new(key, data, hashlib.sha512)
        
        # Return hex digest string for use as entropy source
        return mac.hexdigest()
    
    def generate_hmacuid(self, item, value, field, dicom):
        """Custom UID generation using HMAC-512 with patient-specific key.
        
        Uses the patient's cryptographic random token from the UID database as the
        HMAC key to generate deterministic but unpredictable UIDs. This ensures:
        - Same original UID always maps to same anonymized UID for a patient
        - Different patients get different anonymized UIDs even for same original UID
        - UIDs are cryptographically secure and cannot be reverse-engineered
        
        Args:
            item: Item identifier from deid processing
            value: Recipe string or value from deid processing
            field: DICOM field element containing the UID tag
            dicom: PyDicom dataset object
            
        Returns:
            str: Newly generated anonymized UID
        """
        project_hash_root = self.config.get('projectHashRoot', '')
        
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

        # Get patient's random token from database to use as HMAC key
        hmac_key = None
        if self.patient_uid_db:
            try:
                # Extract patient identifiers
                original_patient_id = self.series.original_patient_id
                original_patient_name = self.series.original_patient_name
                original_patient_birthdate = self.series.original_patient_birthdate
                # Get cached patient data (includes random token)
                cached_result = self.patient_uid_db.get_cached_patient_id(
                    original_patient_id,
                    original_patient_name,
                    original_patient_birthdate
                )
                
                # Use existing patient's random token
                _, random_token = cached_result
                hmac_key = random_token
                    
            except Exception as e:
                self.logger.warning(f"Could not retrieve patient random token: {e}")
        
        # Generate UID using HMAC-512 if we have a key, otherwise fall back to old method
        if hmac_key:
            # Use HMAC-512 to generate deterministic entropy from patient's key
            # This creates a unique, unpredictable UID that's consistent per patient
            hmac_hex = self._compute_hmac(hmac_key, project_hash_root, original_uid)
            # HMAC hex string provides entropy for pydicom UID generation
            new_uid = pydicom.uid.generate_uid(entropy_srcs=[hmac_hex])
        else:
            # Fallback to simple project+UID entropy if no HMAC key available
            self.logger.debug("No HMAC key available, using fallback UID generation")
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
    
    def generate_hmacdate_shift(self, item, value, field, dicom):
        """Generate date/time shift using HMAC with patient-specific key.
        
        Uses the patient's cryptographic random token (same as UID generation)
        to ensure consistent, secure, unpredictable date shifts per patient.
        This approach provides:
        - Cryptographically secure randomness (HMAC-SHA512)
        - Patient-specific deterministic shifts
        
        Args:
            item: Item identifier from deid processing
            value: Recipe string (e.g., "func:generate_hmacdate_shift")
            field: DICOM field element containing the date/time tag
            dicom: PyDicom dataset object
        
        Returns:
            int: Number of days to shift backward (0-maxDateShiftDays days, consistent per patient)
        """
        project_hash_root = self.config.get('projectHashRoot', '')
        
        # Get patient's random token from database to use as HMAC key
        hmac_key = None
        if self.patient_uid_db:
            try:
                original_patient_id = self.series.original_patient_id
                original_patient_name = self.series.original_patient_name
                original_patient_birthdate = self.series.original_patient_birthdate
                
                # Get cached patient data (includes random token)
                cached_result = self.patient_uid_db.get_cached_patient_id(
                    original_patient_id,
                    original_patient_name,
                    original_patient_birthdate
                )
                
                if cached_result:
                    _, random_token = cached_result
                    hmac_key = random_token
            except Exception as e:
                self.logger.warning(f"Could not retrieve patient random token for date shift: {e}")
        
        try:
            # Generate date shift using HMAC if we have a key, otherwise fall back to old method
            if hmac_key:
                # Use HMAC-512 for cryptographically secure date shift
                # Use project_hash_root as data (not in key) for project-specific isolation
                data = f"{project_hash_root}".encode('utf-8')
                mac = hmac.new(hmac_key, data, hashlib.sha512)
                hash_hex = mac.hexdigest()
                hash_int = int(hash_hex[:16], 16)  # Use first 16 hex chars for more entropy
                self.logger.debug("Using HMAC-based date shift generation")
            else:
                # Fallback to old method if no HMAC key available
                self.logger.warning("No HMAC key available, using fallback date shift generation")
                project_salt = f"{project_hash_root}{self.series.original_patient_id}{self.series.original_patient_name}{self.series.original_patient_birthdate}"
                salt_hash = hashlib.sha256(project_salt.encode()).hexdigest()
                hash_int = int(salt_hash[:8], 16)
            
            # Scale to max_date_shift_days (default 1095)
            # Ensure shift is always at least 1 day (never 0) for proper anonymization
            max_shift = self.config.get('maxDateShiftDays', 1095)
            project_date_shift = (hash_int % max_shift) + 1
            
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
            return 1  # Return 1 day shift on error
    
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
    
    def is_curve_or_overlay_tag(self, dicom, value, field, item):
        """Check if a DICOM tag is Curve Data, Overlay Data, or Overlay Comments.
        
        These tags are defined in specific group ranges per DICOM standard:
        - Curve Data: (50xx,xxxx) where xx is 00-FF (even numbers only)
        - Overlay Data: (60xx,3000) where xx is 00-FF (even numbers only)  
        - Overlay Comments: (60xx,4000) where xx is 00-FF (even numbers only)
        
        Args:
            dicom: PyDicom dataset object containing DICOM data
            value: Recipe string or value from recipe processing
            field: DICOM field element containing tag information
            item: Item identifier from deid processing
            
        Returns:
            bool: True if the tag matches curve/overlay patterns, False otherwise
        """
        tag = field.element.tag
        group = tag.group
        element = tag.element
        
        # Check for Curve Data (50xx,xxxx) - any element in group 50xx (even)
        if 0x5000 <= group <= 0x50FF and group % 2 == 0:
            self.logger.debug(f"Found Curve Data tag: {tag}")
            return True
        
        # Check for Overlay Data (60xx,3000)
        if 0x6000 <= group <= 0x60FF and group % 2 == 0 and element == 0x3000:
            self.logger.debug(f"Found Overlay Data tag: {tag}")
            return True
        
        # Check for Overlay Comments (60xx,4000)
        if 0x6000 <= group <= 0x60FF and group % 2 == 0 and element == 0x4000:
            self.logger.debug(f"Found Overlay Comments tag: {tag}")
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
        
        This method is called AFTER the series has been fully processed and exported.
        Memory cleanup now happens in two stages:
        
        1. Immediately after anonymization (in process_series finally block):
           - items dict - freed immediately
           - parsed_files list - freed immediately
           
        2. After export completes (this method):
           - current_file_mappings - cleared here
           - self.series reference - cleared here
        
        This two-stage approach minimizes peak memory usage while ensuring
        data availability when needed.
        
        Args:
            series: DicomSeries whose data should be cleared
        """
        # Clear UID mappings for this series
        self.current_file_mappings.clear()
        
        # Clear series reference to allow garbage collection
        self.series = None
        
        # Force garbage collection to free memory immediately
        gc.collect()
        
        if self.logger:
            series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
            self.logger.debug(f"Cleared memory for series {series_display}")
