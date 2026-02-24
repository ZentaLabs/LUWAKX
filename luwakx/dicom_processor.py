"""DICOM processor service for anonymization computation.

This module provides the DicomProcessor class which handles all DICOM
anonymization logic including UID generation, date shifting, descriptor
cleaning, and private tag handling.

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
        self.warned_non_modified_tags: set = set()  # Track tags warned about wrong VR types per series which are not modified
        self.llm_verified_clean_tags: set = set()  # Track tags where LLM found no PHI (requires manual verification)
    
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
        # Reset LLM-verified clean tags for this series
        self.llm_verified_clean_tags = set()
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
            items[item]["check_patient_age"] = self.check_patient_age
        
        # 4. Setup progress handler to redirect deid.bot output to logger
        progress_handler = None
        try:
            series_uid = self.series.anonymized_series_uid
            progress_handler = DeidProgressHandler(
                self.logger,
                len(dicom_files),
                series_uid_name=series_uid
            )
            bot.outputStream = progress_handler
            bot.errorStream = progress_handler
        except Exception as e:
            self.logger.warning(f"Could not setup progress handler: {e}")
        
        # 5. Perform anonymization using deid library
        # Note: Output directory is already created in organize stage
        self.logger.info(f"Anonymizing {len(dicom_files)} files to {self.series.output_base_path}")
        
        try:
            # Initialise tqdm bar and reset per-series counters
            if progress_handler:
                progress_handler.init_progress(len(dicom_files))

            for dicom_file in dicom_files:

                parsed_file = replace_identifiers(
                    dicom_files=dicom_file,
                    deid=recipe,
                    strip_sequences=False,
                    ids=items,
                    remove_private=False,  # Let recipes handle private tag removal
                    save=True,
                    output_folder=self.series.output_base_path,
                    overwrite=True,
                    force=True
                )

                if not parsed_file:
                    self.logger.warning(f"No files were anonymized for series {series_display}")
                    return

                # Advance progress bar and emit interval log messages
                if progress_handler:
                    progress_handler.update_progress(dicom_file)
            
            # 6. Update series files with anonymized paths
            for dicom_file in self.series.files:
                output_path = os.path.join(self.series.output_base_path, dicom_file.filename)
                dicom_file.set_anonymized_path(output_path)
                        
            # 7. Inject DeidentificationMethodCodeSequence into anonymized files
            self.inject_deidentification_method_code_sequence()
            
            # 8. Warn about LLM-verified clean tags that need manual verification
            if self.llm_verified_clean_tags:
                tag_list = ', '.join(sorted(self.llm_verified_clean_tags))
                self.logger.warning(
                    f"MANUAL VERIFICATION REQUIRED: The following tags were verified by LLM as containing no PHI, "
                    f"but additional manual checks are recommended to ensure content validity:\n"
                    f"   Series Information:\n"
                    f"     - Patient ID: {self.series.anonymized_patient_id}\n"
                    f"     - Study UID: {self.series.anonymized_study_uid}\n"
                    f"     - Series UID: {self.series.anonymized_series_uid}\n"
                    f"   Tags to verify: {tag_list}"
                )
            
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
                del parsed_file
                gc.collect()
                self.logger.debug("Freed items and parsed_file memory after anonymization")
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
            
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#536-patient-id-generation-funcgenerate_patient_id
        """
        if not self.patient_uid_db:
            # Fallback if database not initialized - log once per series
            warn_key = "patient_id_db_not_init"
            if warn_key not in self.warned_non_modified_tags:
                self.warned_non_modified_tags.add(warn_key)
                series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                self.logger.warning(
                    f"Patient UID database not initialized, using default ID. "
                    f"Series: {series_info}"
                )
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
            
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#534-uid-generation-funcgenerate_hmacuid
        """
        # Check VR type - only apply UID replacement to UI (Unique Identifier)
        field_vr = None
        if hasattr(field, 'element') and hasattr(field.element, 'VR'):
            field_vr = str(field.element.VR)
        
        if field_vr != 'UI':
            tag_str = str(getattr(field.element, 'tag', 'unknown')) if hasattr(field, 'element') else 'unknown'
            keyword_str = getattr(field.element, 'keyword', '') if hasattr(field, 'element') else ''
            
            # Only log warning once per unique tag per series
            tag_key = f"replaceuid_{tag_str}_{keyword_str}"
            if tag_key not in self.warned_non_modified_tags:
                self.warned_non_modified_tags.add(tag_key)
                series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                self.logger.warning(
                    f"Tag {tag_str} ({keyword_str}) has VR={field_vr}, which cannot have UID replacement applied. "
                    f"UID replacement only applies to VR type UI. Please verify this tag manually. "
                    f"Series: {series_info}"
                )
            
            # Return original value for non-UI VR types
            return str(field.element.value)
        
        project_hash_root = self.config.get('projectHashRoot', '')
        
        # Extract the original UID value from the DICOM field
        try:
            original_uid = str(field.element.value)
            # Log original UID at PRIVATE level
            self.logger.private(
                f"Processing original UID for tag {field.element.tag} "
                f"({getattr(field.element, 'keyword', '')}): {original_uid}"
            )
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            original_uid = "unknown"

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

        # If no exact match found, check if UID is in a nested sequence
        # This handles cases where the UID appears in a sequence
        if not mapping or mapping.get('original') != original_uid:
            seq_path = self.find_sequence_path(dicom, original_uid, field_keyword)
            if seq_path and seq_path != field_keyword:
                field_keyword = seq_path
                # Check if mapping exists for the sequence path
                mapping = self.current_file_mappings[file_path].get(field_keyword)
                if mapping and mapping.get('original') == original_uid:
                    return mapping['anonymized']

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
                 or 0 if VR type is not DA or DT (no shift applied)
                 
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#535-date-shifting-funcgenerate_hmacdate_shift
        """
        # Check VR type - only apply date shift to DA (Date) and DT (DateTime)
        field_vr = None
        if hasattr(field, 'element') and hasattr(field.element, 'VR'):
            field_vr = str(field.element.VR)
        
        if field_vr not in ['DA', 'DT']:
            tag_str = str(getattr(field.element, 'tag', 'unknown')) if hasattr(field, 'element') else 'unknown'
            keyword_str = getattr(field.element, 'keyword', '') if hasattr(field, 'element') else ''
            # Only log warning once per unique tag per series
            tag_key = f"dateshift_{tag_str}_{keyword_str}"
            if tag_key not in self.warned_non_modified_tags:
                self.warned_non_modified_tags.add(tag_key)
                series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                self.logger.warning(
                    f"Tag {tag_str} ({keyword_str}) has VR={field_vr}, which cannot be jittered. "
                    f"Date shift only applies to VR types DA and DT. Please verify this tag manually. "
                    f"Series: {series_info}"
                )
            
            return 0  # Return 0 (no shift) for non-date VR types
        
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
            
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#538-fixed-datetime-funcset_fixed_datetime
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
                # For unknown VR, log warning once per unique tag per series
                tag_key = f"fixeddatetime_{tag_str}_{keyword_str}"
                if tag_key not in self.warned_non_modified_tags:
                    self.warned_non_modified_tags.add(tag_key)
                    series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                    original_value = field.element.value if hasattr(field, 'element') and hasattr(field.element, 'value') else ""
                    self.logger.warning(
                        f"Unknown VR type '{vr}' for tag {tag_str} ({keyword_str}). "
                        f"Expected DA, DT, or TM. Returning original value. "
                        f"Series: {series_info}"
                    )
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
            
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#537-llm-descriptor-cleaning-funcclean_descriptors_with_llm
        """
        from openai import OpenAI
        
        # Extract original value
        try:
            original_value = str(field.element.value)
            # Log original value at PRIVATE level
            self.logger.private(
                f"Processing original value for tag {field.element.tag} "
                f"({getattr(field.element, 'keyword', '')}): {original_value}"
            )
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            original_value = "unknown"
        
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
            tag_keyword = getattr(field.element, 'keyword', 'Unknown')
            self.logger.debug(
                f"Keeping original value for tag {field.element.tag} "
                f"({tag_keyword})."
            )
            # Track this tag for manual verification warning
            self.llm_verified_clean_tags.add(f"{field.element.tag} ({tag_keyword})")
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
            
        See conformance documentation:
        - Private Tags Template: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#52-private-tags-template
        - Private Tag Removal ("Private Tag Removal" paragraph): https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#642-additional-recipe-directives
        """
        # Log private tag details at PRIVATE level if it exists
        if field.element.is_private and (field.element.private_creator is not None):
            if hasattr(field.element, 'value'):
                self.logger.private(f"Removed private tag {field.element.tag} with value: {field.element.value}")
            return True
        return False

    def check_patient_age(self, dicom, value, field, item):
        """Check Patient Age tag (0010,1010) and return 90 if age > 89, else original value.
        
        Args:
            dicom: PyDicom dataset object containing DICOM data
            value: Recipe string or value from recipe processing
            field: DICOM field element containing tag information
            item: Item identifier from deid processing
        
        Returns:
            str: '90' if Patient Age > 89, else original value
        
        See conformance documentation:
        - https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md
        """
        age_value = getattr(field.element, 'value', None)
        if not age_value:
            self.logger.debug(f"Patient Age tag {field.element.tag} is empty or not filled.")
            return ""
        age_str = str(age_value)
        # Patient Age is usually formatted as nnnD, nnnW, nnnM, or nnnY
        if age_str.endswith("Y"):
            try:
                age_val = int(age_str[:-1])
                if age_val > 89:
                    self.logger.private(f"Patient Age tag {field.element.tag} value {age_str} replaced with '90Y'")
                    return "090Y"
                else:
                    return age_str
            except Exception:
                warn_key = "patient_age_format_invalid"
                if warn_key not in self.warned_non_modified_tags:
                    self.warned_non_modified_tags.add(warn_key)
                    series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                    self.logger.warning(
                        f"Patient Age tag {field.element.tag} format not recognized. "
                        f"Please manually check the validity of this tag. Series: {series_info}"
                    )
                return age_str
        else:
            warn_key = "patient_age_format_invalid"
            if warn_key not in self.warned_non_modified_tags:
                self.warned_non_modified_tags.add(warn_key)
                series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                self.logger.warning(
                    f"Patient Age tag {field.element.tag} format not recognized. "
                    f"Please manually check the validity of this tag. Series: {series_info}"
                )
            return age_value
    
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
            
        See conformance documentation ("If basic_profile is selected" paragraph):
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#642-additional-recipe-directives
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
        
    def inject_deidentification_method_code_sequence(self):
        """Inject DeidentificationMethodCodeSequence with child tags into anonymized DICOM files.
        
        This method loops through the anonymized DICOM files in the series, reads the recipes
        from the config, maps them to DICOM CID 7050 De-identification Method codes, and adds
        the DeidentificationMethodCodeSequence tag with appropriate child tags for each recipe.
        
        The mapping follows DICOM PS3.16 CID 7050 standard codes.
        Additionally if defacing was performed, the RecognizableVisualFeatures tag is set to "NO".
        
        Note:
            This should be called after anonymization but before cleanup/deletion of files.
            
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-5
        """
        # Mapping of recipe names to DICOM CID 7050 De-identification Method codes
        # Based on DICOM PS3.16 CID 7050 - https://dicom.nema.org/medical/dicom/current/output/chtml/part16/sect_CID_7050.html
        RECIPE_TO_CID7050_MAP = {
            'basic_profile': ('113100', 'DCM', 'Basic Application Confidentiality Profile'),
            'clean_pixel_data': ('113101', 'DCM', 'Clean Pixel Data Option'),
            'clean_recognizable_visual_features': ('113102', 'DCM', 'Clean Recognizable Visual Features Option'),
            'clean_graphics': ('113103', 'DCM', 'Clean Graphics Option'),
            'clean_structured_content': ('113104', 'DCM', 'Clean Structured Content Option'),
            'clean_descriptors': ('113105', 'DCM', 'Clean Descriptors Option'),
            'retain_long_full_dates': ('113106', 'DCM', 'Retain Longitudinal Temporal Information Full Dates Option'),
            'retain_long_modified_dates': ('113107', 'DCM', 'Retain Longitudinal Temporal Information Modified Dates Option'),
            'retain_patient_chars': ('113108', 'DCM', 'Retain Patient Characteristics Option'),
            'retain_device_id': ('113109', 'DCM', 'Retain Device Identity Option'),
            'retain_uid': ('113110', 'DCM', 'Retain UIDs Option'),
            'retain_safe_private_tags': ('113111', 'DCM', 'Retain Safe Private Option'),
            'retain_institution_id': ('113112', 'DCM', 'Retain Institution Identity Option'),
        }
        
        if not self.series or not hasattr(self.series, 'files'):
            self.logger.warning("No series or files found for DeidentificationMethodCodeSequence injection")
            return
        
        # Get recipes from config
        recipes_list = self.config.get('recipes', [])
        if not recipes_list:
            self.logger.warning("No recipes found in config, skipping DeidentificationMethodCodeSequence injection")
            return
        
        # Normalize to list if single string
        if isinstance(recipes_list, str):
            recipes_list = [recipes_list]
        
        # Check if defacing was actually performed (for clean_recognizable_visual_features)
        # See conformance documentation (§7.2 - "Conditionally includes defacing code"):
        # https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#72-implementation
        defacing_performed = getattr(self.series, 'defacing_succeeded', False)
        
        # Build sequence items for all matching recipes
        # Maps recipe profiles to DICOM CID 7050 codes (§7.3):
        # https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#73-code-mapping
        sequence_items = []
        for recipe_name in recipes_list:
            # Skip clean_recognizable_visual_features if defacing was not performed
            if recipe_name == 'clean_recognizable_visual_features' and not defacing_performed:
                self.logger.debug(f"Skipping DeidentificationMethodCodeSequence for '{recipe_name}': defacing was not performed for this series")
                continue
                
            if recipe_name in RECIPE_TO_CID7050_MAP:
                code_value, coding_scheme, code_meaning = RECIPE_TO_CID7050_MAP[recipe_name]
                seq_item = pydicom.Dataset()
                seq_item.CodeValue = code_value
                seq_item.CodingSchemeDesignator = coding_scheme
                seq_item.CodeMeaning = code_meaning
                sequence_items.append(seq_item)
                self.logger.debug(f"Adding DeidentificationMethodCodeSequence item for recipe '{recipe_name}': {code_value}")
            else:
                self.logger.debug(f"Recipe '{recipe_name}' has no CID 7050 mapping, skipping")
        
        # Sort sequence items by code value (numerical order)
        # See conformance documentation (§7.2 - "Sorts sequence items"):
        # https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#72-implementation
        sequence_items.sort(key=lambda item: item.CodeValue)
        
        if not sequence_items:
            self.logger.warning("No valid CID 7050 codes found for configured recipes")
            return
        
        # Inject sequence into all files
        injected_count = 0
        failed_count = 0
        
        for dicom_file in self.series.files:
            # Get the current path (anonymized path)
            try:
                path = dicom_file.get_current_path()
            except AttributeError:
                path = getattr(dicom_file, 'filename', None)
            
            if not path or not os.path.exists(path):
                self.logger.warning(f"File not found for DeidentificationMethodCodeSequence injection: {path}")
                failed_count += 1
                continue
            
            try:
                # Read the DICOM file
                ds = pydicom.dcmread(path)
                
                # Inject the DeidentificationMethodCodeSequence with all recipe items
                ds.DeidentificationMethodCodeSequence = sequence_items
                
                # Add RecognizableVisualFeatures tag if defacing was performed
                if defacing_performed:
                    ds.RecognizableVisualFeatures = "NO"

                # Save the file (overwrite)
                ds.save_as(path, enforce_file_format=True)
                
                injected_count += 1
                self.logger.debug(f"Injected DeidentificationMethodCodeSequence with {len(sequence_items)} items into {os.path.basename(path)}")
                
            except Exception as e:
                self.logger.error(f"Failed to inject DeidentificationMethodCodeSequence into {path}: {e}")
                failed_count += 1
        
        self.logger.debug(f"DeidentificationMethodCodeSequence injection completed: {injected_count} succeeded, {failed_count} failed, {len(sequence_items)} codes per file")
    
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
        
        # Clear warning tracking for this series
        self.warned_non_modified_tags.clear()
        
        # Clear series reference to allow garbage collection
        self.series = None
        
        # Force garbage collection to free memory immediately
        gc.collect()
        
        if self.logger:
            self.logger.debug(f"Cleared memory for series")
