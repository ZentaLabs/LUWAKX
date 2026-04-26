"""DICOM processor service for anonymization computation.

This module provides the DicomProcessor class which handles all DICOM
anonymization logic including UID generation, date shifting, descriptor
cleaning, and private tag handling.

"""

import os
import gc
import re
import hashlib
import hmac
import traceback
import importlib.util
from typing import Any, Dict
import pydicom

# Allow DICOM files whose private tags have a stored byte-length that is not an
# exact multiple of the declared VR's element size written as 6 bytes
# instead of 8 for VR 'UN'/'FD').  Without this flag pydicom raises a
# ValueError and the entire series fails; with it the tag is silently re-typed to
# UN so parsing continues.
pydicom.config.convert_wrong_length_to_UN = True

from .dicom_series import DicomSeries
from ..logging.luwak_logger import log_project_stacktrace
from ..export.review_flag_collector import ReviewFlagCollector


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
    
    def __init__(self, config: Dict[str, Any], logger, llm_cache=None, patient_uid_db=None,
                 review_collector=None):
        """Initialize DicomProcessor.
        
        Args:
            config: Configuration dictionary
            logger: Logger instance
            llm_cache: Optional shared LLM cache instance (thread-safe)
            patient_uid_db: Optional shared patient UID database instance (thread-safe)
            review_collector: Optional ReviewFlagCollector instance (created and owned by
                ProcessingPipeline, injected here to keep I/O concerns out of the processor)
        """
        self.config = config
        self.logger = logger
        self.llm_cache = llm_cache
        self.patient_uid_db = patient_uid_db
        self.series = None
        
        # Isolated state per worker (NOT shared)
        self.current_file_mappings: Dict[str, Any] = {}  # UID mappings

        # Mapping of private creator strings -> set of (group, element_offset) pairs
        # that have at least one KEEP/REPLACE/JITTER rule in the active recipe.
        # Consulted by is_tag_private() to preserve creator block elements (e.g. (0071,0010))
        # only when the DICOM file actually contains at least one of those kept elements.
        # Populated at the start of process_series() via _build_kept_private_creators().
        self._kept_private_creators: dict = {}

        # Fallback dedup set for logger warnings when review_collector is None.
        # When review_collector is available, _first_occurrence() uses
        # is_first_flag() instead, so this set stays empty in normal operation.
        self.warned_non_modified_tags: set = set()

        # Injected by ProcessingPipeline (same pattern as uid_mappings_file).
        # None when outputPrivateMappingFolder is not configured.
        self.review_collector = review_collector


    
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
        from ..logging.deid_logger_handler import DeidProgressHandler
        
        self.series = series
        # Initialise review-flags context for this series
        if self.review_collector:
            self.review_collector.set_series_context(
                self.series.anonymized_patient_id,
                self.series.anonymized_study_uid,
                self.series.anonymized_series_uid,
            )
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
            items[item]["sq_keep_original_with_review"] = self.sq_keep_original_with_review
        
        # 4. Setup progress handler to redirect deid.bot output to logger
        progress_handler = None
        try:
            series_uid = self.series.anonymized_series_uid
            progress_handler = DeidProgressHandler(
                self.logger,
                len(dicom_files),
                series_uid_name=series_uid,
                review_collector=self.review_collector,
            )
            bot.outputStream = progress_handler
            bot.errorStream = progress_handler
        except Exception as e:
            self.logger.warning(f"Could not setup progress handler: {e}")
        
        # 5. Perform anonymization using deid library
        # Note: Output directory is already created in organize stage
        self.logger.info(f"Anonymizing {len(dicom_files)} files to {self.series.output_base_path}")

        try:
            self._kept_private_creators = DicomProcessor._build_kept_private_creators(recipe)
        except Exception as _e:
            self.logger.warning(f"Could not build kept private creators set: {_e}")
            self._kept_private_creators = {}

        try:
            # Initialise tqdm bar and reset per-series counters
            if progress_handler:
                progress_handler.init_progress(len(dicom_files))

            for dicom_file in dicom_files:
                # Update handler with current instance UID so VR-format warnings
                # emitted by deid/pydicom can be attributed to the right instance.
                if progress_handler:
                    progress_handler.set_current_instance_uid(
                        str(items.get(dicom_file, {}).get('SOPInstanceUID', '*'))
                    )

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

            # 8. Warn about LLM-verified clean tags that need manual verification.
            # Query the review_collector buffer BEFORE flushing so it still holds
            # this series' flags.
            if self.review_collector:
                _llm_tags = self.review_collector.get_pending_keywords_by_reason(
                    ReviewFlagCollector.REASON_LLM_VERIFIED_CLEAN
                )
                if _llm_tags:
                    tag_list = ', '.join(sorted(_llm_tags))
                    self.logger.warning(
                        f"MANUAL VERIFICATION REQUIRED: The following tags were verified by LLM as containing no PHI, "
                        f"but additional manual checks are recommended to ensure content validity:\n"
                        f"   Series Information:\n"
                        f"     - Patient ID: {self.series.anonymized_patient_id}\n"
                        f"     - Study UID: {self.series.anonymized_study_uid}\n"
                        f"     - Series UID: {self.series.anonymized_series_uid}\n"
                        f"   Tags to verify: {tag_list}"
                    )
            # flush_series() and CSV writing are handled by ProcessingPipeline
            # via MetadataExporter after process_series() returns.
            
        except Exception as e:
            series_display = os.path.basename(self.series.output_base_path)
            self.logger.error(f"Error during anonymization of series {series_display}: {e}")
            raise
        finally:
            # Close progress handler and detach from the deid bot singleton.
            # Without the reset, bot.outputStream keeps the handler (and all objects
            # it references) alive until the next series replaces it.  The last
            # series' handler would never be freed at all.
            if progress_handler:
                try:
                    progress_handler.close()
                except Exception as e:
                    self.logger.warning(f"Error closing progress handler: {e}")
            try:
                bot.outputStream = None
                bot.errorStream  = None
                # bot.history is a list that accumulates every log message the deid
                # library emits and is never cleared automatically.  With hundreds of
                # DICOM files per series this grows continuously and is the primary
                # source of unbounded memory growth across series.
                if hasattr(bot, 'history') and isinstance(bot.history, list):
                    bot.history.clear()
            except Exception:
                pass
            
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
            
        if self.review_collector:
            pending = len(self.review_collector._flags)
            self.logger.debug(
                f"DicomProcessor: series {series_display} completed - "
                f"{pending} review-flag key(s) pending in collector"
            )
        self.logger.debug(f"DicomProcessor: Completed series {series_display}")
    
    # =================================================================
    # DEID Custom Functions - Injected into DEID recipe processing
    # =================================================================

    def _first_occurrence(self, tag_group: str, tag_element: str, reason: str,
                          fallback_key: str) -> bool:
        """Dedup guard - True on the first occurrence of (tag, reason) within this series.

        When a review_collector is present, delegates to
        ``review_collector.is_first_flag()`` so the collector buffer is the single
        source of truth (no duplicate state).  Falls back to the local
        ``warned_non_modified_tags`` set otherwise.

        Must be called *before* the corresponding ``review_collector.add_flag()``
        call so the buffer is still empty for that key.

        Args:
            tag_group:    4-char hex group string.
            tag_element:  4-char hex element string.
            reason:       ``ReviewFlagCollector.REASON_*`` constant.
            fallback_key: Key used in ``warned_non_modified_tags`` when no collector.

        Returns:
            bool: True if this is the first time this (tag, reason) has fired.
        """
        if self.review_collector:
            return self.review_collector.is_first_flag(tag_group, tag_element, reason)
        # Fallback: no output folder / collector not initialised
        if fallback_key in self.warned_non_modified_tags:
            return False
        self.warned_non_modified_tags.add(fallback_key)
        return True

    def _flag_params(self, field, dicom_dataset) -> dict:
        """Extract common tag-identification parameters for ReviewFlagCollector.add_flag().

        Args:
            field:          deid field wrapper whose ``.element`` is a pydicom DataElement.
            dicom_dataset:  PyDicom Dataset for the file being processed (provides SOPInstanceUID).

        Returns:
            Dict with keys: tag_group, tag_element, attribute_name, keyword, vr, vm,
            sop_instance_uid.  Safe to spread (``**``) directly into add_flag().
        """
        try:
            tag         = field.element.tag
            tag_group   = f"{tag.group:04X}"
            tag_element = f"{tag.element:04X}"
            keyword     = getattr(field.element, 'keyword', '') or ''
            vr          = str(getattr(field.element, 'VR',  '') or '')
            vm          = str(getattr(field.element, 'VM',  '') or '')
        except Exception:
            tag_group = tag_element = keyword = vr = vm = ''
        sop_uid = str(getattr(dicom_dataset, 'SOPInstanceUID', '*') or '*')
        return dict(
            tag_group       = tag_group,
            tag_element     = tag_element,
            attribute_name  = keyword,
            keyword         = keyword,
            vr              = vr,
            vm              = vm,
            sop_instance_uid= sop_uid,
        )

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
            _fp = self._flag_params(field, dicom)
            is_first = self._first_occurrence(
                _fp['tag_group'], _fp['tag_element'],
                ReviewFlagCollector.REASON_PATIENT_DB_UNAVAILABLE,
                "patient_id_db_not_init"
            )
            if self.review_collector:
                try:
                    self.review_collector.add_flag(
                        reason         = ReviewFlagCollector.REASON_PATIENT_DB_UNAVAILABLE,
                        original_value = str(getattr(field.element, 'value', '')),
                        keep           = 0,
                        output_value   = 'ANON00',
                        **_fp,
                    )
                except Exception:
                    pass
            if is_first:
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
            self.logger.private(
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
        if getattr(field.element, 'keyword', '') == "PatientName":
            m = re.match(r"([A-Za-z]+)(\d+)", anonymized_id)
            if m:
                prefix, number = m.groups()
                anonymized_id = f"{prefix}^{int(number):04d}"

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
        
        if field_vr not in ('UI', 'LO'):
            tag_str = str(getattr(field.element, 'tag', 'unknown')) if hasattr(field, 'element') else 'unknown'
            keyword_str = getattr(field.element, 'keyword', '') if hasattr(field, 'element') else ''
            # Avoid serialising raw binary data into the review CSV for blob VR types
            _is_binary_vr = field_vr in ('OB', 'OW', 'UN')
            _orig = f'<binary {field_vr} data>' if _is_binary_vr else str(getattr(field.element, 'value', ''))
            _fp = self._flag_params(field, dicom)

            # Tag has a VR incompatible with UID replacement: attempt removal.
            try:
                del dicom[field.element.tag]
                self.logger.debug(
                    f"Removed tag {tag_str} ({keyword_str}) with VR={field_vr}: "
                    f"VR is incompatible with UID replacement."
                )
                return None
            except Exception as remove_err:
                # Removal failed (e.g. tag is nested inside a sequence).
                # Warn once per tag/series and record every instance in the review CSV.
                if self._first_occurrence(_fp['tag_group'], _fp['tag_element'],
                                          ReviewFlagCollector.REASON_VR_MISMATCH,
                                          f"replaceuid_{tag_str}_{keyword_str}"):
                    series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                    self.logger.warning(
                        f"Tag {tag_str} ({keyword_str}) has VR={field_vr}, which is incompatible with UID "
                        f"replacement, and could not be removed (tag may be nested inside a sequence). "
                        f"Please verify this tag manually. Error: {remove_err}. Series: {series_info}"
                    )
                if self.review_collector:
                    try:
                        self.review_collector.add_flag(
                            reason         = ReviewFlagCollector.REASON_VR_MISMATCH,
                            original_value = _orig,
                            keep           = 1,
                            output_value   = _orig,
                            **_fp,
                        )
                    except Exception:
                        pass
                # Return the actual element value (not the placeholder) so deid writes it back unchanged
                return field.element.value
        
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

        # The value can be empty (tag present but no value set).
        # Do not generate a UID from empty entropy - leave the tag unchanged.
        if not original_uid:
            return field.element.value

        # Extract file path from the dicom dataset filename attribute
        file_path = f"{getattr(dicom, 'filename', str(dicom))}"
        if file_path not in self.current_file_mappings:
            self.current_file_mappings[file_path] = {}

        # Get field keyword from the element.
        # For private tags use the same naming convention as the parquet exporter so
        # that UID mapping CSV columns are human-readable and collision-free:
        #   <private_creator>_<tag_name>         e.g. Siemens_CSA_Image_Header_Info
        #   <private_creator>_<ggggxx>ee          e.g. PHILIPS_MR_IMAGING_0019xx10
        # Public tags use the standard pydicom keyword; fall back to str(tag) for
        # orphaned private tags that have no private creator block.
        _elem = field.element
        if _elem.is_private and _elem.private_creator:
            _private_creator = _elem.private_creator.replace(' ', '_')
            if _elem.name and _elem.name != 'Unknown':
                # pydicom wraps private names in square brackets, e.g. '[CSA Header]'
                field_keyword = f'{_private_creator}_{_elem.name[1:-1]}'
            else:
                field_keyword = (
                    f'{_private_creator}_'
                    f'{_elem.tag.group:04X}xx{_elem.tag.element & 0xFF:02X}'
                )
        elif _elem.keyword:
            field_keyword = _elem.keyword
        else:
            # Orphaned private tag (no private_creator block) or unrecognised public tag
            field_keyword = str(_elem.tag)

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
        field_vr = str(field.element.VR) if hasattr(field, 'element') and hasattr(field.element, 'VR') else None

        # For non-DA/DT VRs (e.g. UN misidentified as a date tag by a private
        # dictionary): validate the stored value against DA/DT patterns.
        # If the value is not a valid date/datetime, remove the tag and bail out.
        # If the value IS a valid date/datetime string, fall through to the shift
        # computation below so the date is still properly jittered.
        if field_vr not in ('DA', 'DT'):
            tag_str = str(getattr(field.element, 'tag', 'unknown')) if hasattr(field, 'element') else 'unknown'
            keyword_str = getattr(field.element, 'keyword', '') if hasattr(field, 'element') else ''
            _raw_val = getattr(field.element, 'value', '') if hasattr(field, 'element') else ''
            # For VR=UN pydicom stores bytes; decode to ASCII for pattern matching.
            if isinstance(_raw_val, (bytes, bytearray)):
                try:
                    _orig = _raw_val.decode('ascii').strip('\x00').strip()
                except (UnicodeDecodeError, AttributeError):
                    _orig = ''
            else:
                _orig = str(_raw_val)
            _fp = self._flag_params(field, dicom)

            _da_pattern = r"^\d{8}$"                                       # YYYYMMDD
            _dt_pattern = r"^\d{8}(\d{6}(\.\d{1,6})?)?([\+\-]\d{4})?$"   # YYYYMMDD[HHMMSS[.F]][+/-ZZZZ]
            if not (re.match(_da_pattern, _orig) or re.match(_dt_pattern, _orig)):
                series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                if self._first_occurrence(_fp['tag_group'], _fp['tag_element'],
                                          ReviewFlagCollector.REASON_VR_FORMAT_INVALID,
                                          f"dateshift_badvalue_{tag_str}_{keyword_str}"):
                    self.logger.warning(
                        f"Tag {tag_str} ({keyword_str}) (VR={field_vr}) value {_orig!r} "
                        f"does not match DICOM DA/DT format. Tag will be removed. "
                        f"Series: {series_info}"
                    )
                try:
                    del dicom[field.element.tag]
                    if self.review_collector:
                        try:
                            self.review_collector.add_flag(
                                reason         = ReviewFlagCollector.REASON_VR_FORMAT_INVALID,
                                original_value = _orig,
                                keep           = 0,
                                output_value   = '',
                                **_fp,
                            )
                        except Exception:
                            pass
                except Exception:
                    if self._first_occurrence(_fp['tag_group'], _fp['tag_element'],
                                              ReviewFlagCollector.REASON_PHI_REMOVAL_FAILED,
                                              f"dateshift_badvalue_remove_{tag_str}_{keyword_str}"):
                        self.logger.warning(
                            f"Tag {tag_str} ({keyword_str}) could not be removed. "
                            f"Original value kept for manual review. Series: {series_info}"
                        )
                    if self.review_collector:
                        try:
                            self.review_collector.add_flag(
                                reason         = ReviewFlagCollector.REASON_PHI_REMOVAL_FAILED,
                                original_value = _orig,
                                keep           = 1,
                                output_value   = _orig,
                                **_fp,
                            )
                        except Exception:
                            pass
                # Return None - NOT 0.  Deid's parser checks `if value is not None`
                # before calling jitter_timestamp; returning 0 passes that check and
                # jitter_timestamp still writes the value back via replace_field.
                # Returning None makes deid skip the jitter entirely, so the deletion
                # above sticks.
                return None
            # Value looks like a valid date/datetime despite the wrong VR - fall
            # through to compute the shift normally.

        # Compute HMAC-based date shift (reached for DA/DT tags always, and for
        # non-DA/DT tags whose stored value is a valid date/datetime string).
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
            - DA (Date): Returns "19000101" (January 1, year 1900)
            - DT (DateTime): Returns "19000101000000.000000+0000"
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
                self.logger.debug(f"Setting fixed date for tag {tag_str} ({keyword_str}) to '19000101'.")
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(
                        f"Setting fixed date for tag {tag_str} ({keyword_str}) "
                        f"with value {field.element.value} to '19000101'."
                    )
                return "19000101"
            elif vr == 'DT':  # DateTime format: YYYYMMDDHHMMSS.FFFFFF&ZZXX
                self.logger.debug(
                    f"Setting fixed datetime for tag {tag_str} ({keyword_str}) "
                    f"to '19000101000000.000000+0000'."
                )
                if hasattr(field, 'element') and hasattr(field.element, 'value'):
                    self.logger.private(
                        f"Setting fixed datetime for tag {tag_str} ({keyword_str}) "
                        f"with value {field.element.value} to '19000101000000.000000+0000'."
                    )
                return "19000101000000.000000+0000"
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
                # original_value is assigned here (outside the dedup guard) so it
                # is always defined for the add_flag call and return below.
                original_value = field.element.value if hasattr(field, 'element') and hasattr(field.element, 'value') else ""
                _orig = str(original_value) if original_value is not None else ''
                _fp = self._flag_params(field, dicom)
                if self._first_occurrence(_fp['tag_group'], _fp['tag_element'],
                                          ReviewFlagCollector.REASON_VR_MISMATCH,
                                          f"fixeddatetime_{tag_str}_{keyword_str}"):
                    series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                    self.logger.warning(
                        f"Unknown VR type '{vr}' for tag {tag_str} ({keyword_str}). "
                        f"Expected DA, DT, or TM. Returning original value. "
                        f"Series: {series_info}"
                    )
                if self.review_collector:
                    try:
                        self.review_collector.add_flag(
                            reason         = ReviewFlagCollector.REASON_VR_MISMATCH,
                            original_value = _orig,
                            keep           = 1,
                            output_value   = _orig,
                            **_fp,
                        )
                    except Exception:
                        pass
                return _orig
                
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
        # Bypass LLM: treat result as 0 (no PHI) and keep the tag unchanged
        if self.config.get('bypassCleanDescriptorsLlm', False):
            try:
                original_value = str(field.element.value)
            except Exception:
                original_value = "unknown"
            tag_keyword = getattr(field.element, 'keyword', 'Unknown')
            self.logger.debug(
                f"LLM bypass enabled: keeping original value for tag "
                f"{field.element.tag} ({tag_keyword})."
            )
            if self.review_collector:
                try:
                    self.review_collector.add_flag(
                        reason         = ReviewFlagCollector.REASON_LLM_VERIFIED_CLEAN,
                        original_value = original_value,
                        keep           = 1,
                        output_value   = original_value,
                        **self._flag_params(field, dicom),
                    )
                except Exception:
                    pass
            return original_value

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
                    os.path.dirname(os.path.dirname(__file__)), "scripts", "detector", "detector.py"
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
                _fp = self._flag_params(field, dicom)
                is_first = self._first_occurrence(
                    _fp['tag_group'], _fp['tag_element'],
                    ReviewFlagCollector.REASON_PHI_REMOVAL_FAILED,
                    f"failed_remove_{field.element.tag}"
                )
                if self.review_collector:
                    try:
                        self.review_collector.add_flag(
                            reason         = ReviewFlagCollector.REASON_PHI_REMOVAL_FAILED,
                            original_value = str(getattr(field.element, 'value', '')),
                            keep           = 0,
                            output_value   = 'ANONYMIZED',
                            **_fp,
                        )
                    except Exception:
                        pass
                if is_first:
                    series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                    self.logger.warning(
                        f"Failed to remove element {field.element.tag} ({_fp['keyword']}): "
                        f"tag is likely nested inside a sequence and deid does not allow direct removal from outside it. "
                        f"Value was replaced with 'ANONYMIZED' as a fallback. Error: {e}. Series: {series_info}"
                    )
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
            # Record in review CSV (the pending buffer is queried at end of
            # process_series to produce the MANUAL VERIFICATION warning log)
            if self.review_collector:
                try:
                    self.review_collector.add_flag(
                        reason         = ReviewFlagCollector.REASON_LLM_VERIFIED_CLEAN,
                        original_value = original_value,
                        keep           = 1,
                        output_value   = original_value,
                        **self._flag_params(field, dicom),
                    )
                except Exception:
                    pass
            return original_value
    
    @staticmethod
    def _build_kept_private_creators(recipe) -> dict:
        """Return a mapping of private creator strings -> kept (group, element_offset) pairs.

        Scans all actions in the recipe and extracts creator names and the
        corresponding tag coordinates from private-tag expressions such as:
          KEEP    (0071,"Siemens MR Header",22)
          REPLACE (0071,"Siemens MR Header",22) func:generate_hmacuid
          JITTER  (0009,"GEMS_PETD_01",05) func:generate_hmacdate_shift

        The creator block element must be preserved only when at least one of its
        data elements is both kept/replaced/jittered by the recipe AND actually
        present in the DICOM file being processed - checked at runtime in
        is_tag_private().

        Removal actions (REMOVE, BLANK) are excluded: if all data elements for a
        creator are removed, the creator block itself can be removed too.

        Returns:
            dict mapping creator_name (str) -> set of (group: int, element_offset: int)
            where element_offset is the low byte of the element number (0x00-0xFF).

        Called once per series via process_series(); the result is an in-memory
        dict built from the already-loaded recipe, so it is O(recipe_actions) with
        no disk I/O.
        """
        # Actions that preserve or modify a private data element - the
        # corresponding creator block must therefore be retained.
        PRESERVING_ACTIONS = {'KEEP', 'REPLACE', 'JITTER'}

        creators: dict = {}
        if not (recipe and recipe.deid):
            return creators

        for action in recipe.get_actions():
            if action.get("action", "").upper() not in PRESERVING_ACTIONS:
                continue
            field_expr = action.get("field", "") or ""
            # Match (group,"creator",element_offset) - all parts are hex strings
            m = re.search(r'\(([0-9a-fA-F]+)\s*,\s*"([^"]+)"\s*,\s*([0-9a-fA-F]+)\)', field_expr)
            if m:
                group = int(m.group(1), 16)
                creator = m.group(2).strip()
                elem_offset = int(m.group(3), 16)
                creators.setdefault(creator, set()).add((group, elem_offset))
        return creators

    def is_tag_private(self, dicom, value, field, item):
        """Check if a DICOM tag is private.
        
        Args:
            dicom: PyDicom dataset object containing DICOM data
            value: Recipe string or value from recipe processing
            field: DICOM field element containing tag information
            item: Item identifier from deid processing
            
        Returns:
            bool: True if the tag is private, False otherwise
            
        See conformance documentation:
        - Private Tags Template: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#52-private-tags-template
        - Private Tag Removal ("Private Tag Removal" paragraph): https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#642-additional-recipe-directives
        """
        # Match any private tag regardless of whether a private creator block is present.
        # Orphaned private tags (private_creator is None) must also be removed.
        #
        # Special case - private creator BLOCK elements (tag format (gggg, 00CC) where
        # 0x10 <= CC <= 0xFF): these hold the creator identification string that makes
        # the corresponding data elements (gggg, CCxx) interpretable.  They must be kept
        # when at least one of their data elements is preserved by a KEEP rule; otherwise
        # the kept data elements become unreadable (dangling, no creator declaration).
        if field.element.is_private:
            elem_num = field.element.tag.element
            is_creator_block = (elem_num & 0xFF00) == 0x0000 and (elem_num & 0x00FF) >= 0x10
            if is_creator_block:
                creator_value = field.element.value
                if isinstance(creator_value, bytes):
                    creator_value = creator_value.decode("utf-8", errors="ignore").strip("\x00")
                creator_value = creator_value.strip() if creator_value else ""
                if creator_value and creator_value in self._kept_private_creators:
                    # The creator is referenced by the recipe; only preserve the
                    # creator block if at least one of those kept elements is
                    # actually present in this specific DICOM file.
                    kept_coords = self._kept_private_creators[creator_value]
                    for elem in dicom.iterall():
                        if (elem.tag.is_private
                                and elem.private_creator == creator_value
                                and (elem.tag.group, elem.tag.element & 0x00FF) in kept_coords):
                            return False
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
                _fp = self._flag_params(field, dicom)
                if self._first_occurrence(_fp['tag_group'], _fp['tag_element'],
                                          ReviewFlagCollector.REASON_VR_MISMATCH,
                                          "patient_age_format_invalid"):
                    series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                    self.logger.warning(
                        f"Patient Age tag {field.element.tag} format not recognized. "
                        f"Please manually check the validity of this tag. Series: {series_info}"
                    )
                if self.review_collector:
                    try:
                        self.review_collector.add_flag(
                            reason         = ReviewFlagCollector.REASON_VR_MISMATCH,
                            original_value = age_str,
                            keep           = 1,
                            output_value   = age_str,
                            **_fp,
                        )
                    except Exception:
                        pass
                return age_str
        else:
            _fp = self._flag_params(field, dicom)
            if self._first_occurrence(_fp['tag_group'], _fp['tag_element'],
                                      ReviewFlagCollector.REASON_VR_MISMATCH,
                                      "patient_age_format_invalid"):
                series_info = f"series:{self.series.anonymized_series_uid}, study:{self.series.anonymized_study_uid}, patient:{self.series.anonymized_patient_id}"
                self.logger.warning(
                    f"Patient Age tag {field.element.tag} format not recognized. "
                    f"Please manually check the validity of this tag. Series: {series_info}"
                )
            if self.review_collector:
                try:
                    self.review_collector.add_flag(
                        reason         = ReviewFlagCollector.REASON_VR_MISMATCH,
                        original_value = str(age_value),
                        keep           = 1,
                        output_value   = str(age_value),
                        **_fp,
                    )
                except Exception:
                    pass
            return age_value
    
    def sq_keep_original_with_review(self, item, value, field, dicom):
        """Keep the original value of a VR=SQ tag and flag it for manual review.

        This function is injected into deid recipe processing and is called for SQ
        (Sequence) tags that are marked for ``replace`` in the anonymization template
        but have no automatic replacement logic available (i.e. the Final CTP Script
        column does not specify ``@remove()`` or ``removed``).

        The tag value is returned unchanged so the sequence content is preserved;
        a review-flag row is written to the review CSV so a human reviewer can
        decide whether the sequence contains PHI and act accordingly.

        Args:
            item:  Item identifier from deid processing.
            value: Recipe string (``"func:sq_keep_original_with_review"``).
            field: DICOM field element containing the SQ tag.
            dicom: PyDicom dataset object.

        Returns:
            The original field value, unchanged.

        Example recipe usage::

            REPLACE (0040,A730) func:sq_keep_original_with_review
        """
        original_value = getattr(field.element, 'value', None)
        _fp = self._flag_params(field, dicom)
        if self._first_occurrence(_fp['tag_group'], _fp['tag_element'],
                                  ReviewFlagCollector.REASON_SQ_REPLACE_NEEDS_REVIEW,
                                  f"sq_review_{_fp['tag_group']}_{_fp['tag_element']}"):
            series_info = (
                f"series:{self.series.anonymized_series_uid}, "
                f"study:{self.series.anonymized_study_uid}, "
                f"patient:{self.series.anonymized_patient_id}"
            )
            self.logger.warning(
                f"MANUAL REVIEW REQUIRED: Tag ({_fp['tag_group']},{_fp['tag_element']}) "
                f"({_fp['keyword']}) has VR=SQ and is marked for replacement, but no "
                f"automatic replacement is available. The tags within the sequence might "
                f"be already correctly modified. Please review this tag manually. Series: {series_info}"
            )
        if self.review_collector:
            try:
                sq_str = str(original_value).replace("\n", " | ") if original_value is not None else ""
                self.review_collector.add_flag(
                    reason         = ReviewFlagCollector.REASON_SQ_REPLACE_NEEDS_REVIEW,
                    original_value = sq_str,
                    keep           = 1,
                    output_value   = sq_str,
                    **_fp,
                )
            except Exception:
                pass
        return original_value

    def is_curve_or_overlay_tag(self, dicom, value, field, item):
        """Check if a DICOM tag is Curve Data, Overlay Data, or Overlay Comments.
        
        These tags are defined in specific group ranges per DICOM standard:
        - Curve Data: (50xx,xxxx) where xx is 00-FF (even numbers only)
        - Overlay Data: (60xx,3000) where xx is 00-FF (even numbers only)  
        - Overlay Comments: (60xx,4000) where xx is 00-FF (even numbers only)

        When removing the Overlay Data (60xx,3000), all the group 6000 
        must be removed too (60xx,xxxx), this includes Overlay Comments (60xx,4000)
        
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
        
        # Check for Curve Data (50xx,xxxx) - any element in group 50xx (even)
        if 0x5000 <= group <= 0x50FF and group % 2 == 0:
            self.logger.debug(f"Found Curve Data tag: {tag}")
            return True
        
        # When removing the Overlay Data (60xx,3000), all the group 6000 
        # must be removed too, this includes Overlay Comments (60xx,4000)
        # hence no explicit calls are necessary for those tags anymore
        
        if 0x6000 <= group <= 0x60FF and group % 2 == 0:
            self.logger.debug(f"Found Overlay Data group tag: {tag}")
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
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#7-deidentificationmethodcodesequence-attribute-injection-pipeline-stage-6
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
        # See conformance documentation (sec.7.2 - "Conditionally includes defacing code"):
        # https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#72-implementation
        defacing_performed = getattr(self.series, 'defacing_succeeded', False)

        # Check if pixel cleaning was actually performed (or bypassed) for clean_pixel_data
        pixel_cleaning_performed = getattr(self.series, 'pixel_cleaning_succeeded', False)
        
        # Build sequence items for all matching recipes
        # Maps recipe profiles to DICOM CID 7050 codes (sec.7.3):
        # https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#73-code-mapping
        sequence_items = []
        for recipe_name in recipes_list:
            # Skip clean_pixel_data if pixel cleaning was not performed
            if recipe_name == 'clean_pixel_data' and not pixel_cleaning_performed:
                self.logger.debug(f"Skipping DeidentificationMethodCodeSequence for '{recipe_name}': pixel cleaning was not performed for this series")
                continue

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
        # See conformance documentation (sec.7.2 - "Sorts sequence items"):
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
                ds = pydicom.dcmread(path, stop_before_pixels=False)
                
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

        # Clear the fallback dedup set (only populated when review_collector is None)
        self.warned_non_modified_tags.clear()

        # Clear series reference to allow garbage collection
        self.series = None
        
        # Force garbage collection to free memory immediately
        gc.collect()

        #  deid @cache leak fix 
        # deid.dicom.fields._get_fields_inner is decorated with
        # @functools.cache (unbounded).  It is keyed by id(FileDataset), so
        # every slice processed during this series is pinned in the cache
        # forever, preventing GC.  Clearing it after each series releases all
        # those FileDataset references immediately.
        try:
            from deid.dicom.fields import _get_fields_inner
            _get_fields_inner.cache_clear()
        except (ImportError, AttributeError):
            pass  # deid not installed or API changed - silently skip

        if self.logger:
            self.logger.debug(f"Cleared memory for series")
