"""Factory for creating DicomSeries objects from DICOM files.

This module provides the DicomSeriesFactory class which handles the discovery,
reading, and grouping of DICOM files into DicomSeries objects. It centralizes
file reading to a single pass and manages patient UID pre-computation.
"""

import os
from typing import Any, Dict, List, Set, Tuple
import pydicom
from dicom_series import DicomSeries, PathTooLongError
from dicom_file import DicomFile


class DicomSeriesFactory:
    """Factory for creating DicomSeries objects from DICOM files.
    
    This factory handles:
    - File discovery from paths (files/directories)
    - Single-pass DICOM file reading
    - Patient UID database pre-computation
    - Series grouping by Patient/Study/Series hierarchy
    - DicomSeries object creation with anonymized UIDs
    
    Attributes:
        patient_uid_db: Patient UID database for anonymization
        config: Configuration dictionary
        logger: Logger instance
        output_directory: Base output directory for series paths
    """
    
    def __init__(self, patient_uid_db, config: Dict[str, Any], logger, 
                 output_directory: str):
        """Initialize DicomSeriesFactory.
        
        Args:
            patient_uid_db: Patient UID database instance (thread-safe)
            config: Configuration dictionary
            logger: Logger instance
            output_directory: Base output directory for processed files
        """
        self.patient_uid_db = patient_uid_db
        self.config = config
        self.logger = logger
        self.output_directory = output_directory
    
    def discover_files(self, input_path: str) -> List[str]:
        """Discover DICOM files from input path (file, directory, or list).
        
        Args:
            input_path: Either a single file path, directory path, or list of paths
            
        Returns:
            List[str]: List of discovered file paths
        """
        if isinstance(input_path, list):
            # Already a list of files
            return input_path
        
        if os.path.isfile(input_path):
            # Single file
            self.logger.info("Processing single DICOM file")
            return [input_path]
        
        if os.path.isdir(input_path):
            # Directory - walk and collect all files
            self.logger.info(f"Discovering DICOM files from: {input_path}")
            dicom_files = []
            for root, dirs, files in os.walk(input_path):
                for file in files:
                    dicom_files.append(os.path.join(root, file))
            self.logger.info(f"Found {len(dicom_files)} files in directory")
            return dicom_files
        
        self.logger.error(f"Input path does not exist: {input_path}")
        return []
    
    def create_series_from_files(self, dicom_files) -> List[DicomSeries]:
        """Create DicomSeries objects from DICOM files (single-pass read).
        
        This method reads each DICOM file once and:
        1. Pre-computes patient UID mappings
        2. Groups files by Patient/Study/Series hierarchy
        3. Creates DicomSeries objects with metadata
        4. Generates anonymized UIDs used for output paths
        5. Builds output paths with collision detection
        
        Args:
            dicom_files: List of DICOM file paths, single file path, or directory path
            
        Returns:
            List[DicomSeries]: List of created DicomSeries objects
        """
        # Discover files if input is a path
        if isinstance(dicom_files, str):
            dicom_files = self.discover_files(dicom_files)
        
        self.logger.info(f"Creating series from {len(dicom_files)} DICOM files")
        
        # Single-pass read: precompute patients AND group series simultaneously
        series_groups: Dict[Tuple[str, str, str, str, str], List[str]] = {}
        series_metadata: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
        seen_patients: Set[Tuple[str, str, str]] = set()
        patient_count = 0
        
        for file_path in dicom_files:
            try:
                ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                
                # Extract patient identifiers
                patient_id = str(getattr(ds, 'PatientID', ''))
                patient_name = str(getattr(ds, 'PatientName', ''))
                birthdate = str(getattr(ds, 'PatientBirthDate', ''))

                # Extract modality and filter if selectedModalities is configured
                modality = str(getattr(ds, 'Modality', ''))
                selected_modalities = self.config.get('selectedModalities', [])
                if selected_modalities and modality not in selected_modalities:
                    continue

                # PRE-COMPUTATION: Populate patient UID database on first encounter
                if self.patient_uid_db:
                    patient_key = (patient_id, patient_name, birthdate)
                    if patient_key not in seen_patients:
                        self.patient_uid_db.store_patient_id(patient_id, patient_name, birthdate)
                        seen_patients.add(patient_key)
                        patient_count += 1
                        
                        if patient_count % 10 == 0:
                            self.logger.debug(f"Pre-computed mappings for {patient_count} patients...")
                
                # Extract study and series UIDs
                study_uid = getattr(ds, 'StudyInstanceUID', 'unknown_study')
                series_uid = getattr(ds, 'SeriesInstanceUID', 'unknown_series')
                
                # Create grouping key: (patient_id, patient_name, birthdate, study_uid, series_uid)
                grouping_key = (patient_id, patient_name, birthdate, study_uid, series_uid)
                
                if grouping_key not in series_groups:
                    series_groups[grouping_key] = []
                    
                    # Extract metadata (once per series)
                    series_desc = getattr(ds, 'SeriesDescription', '')
                    series_number = getattr(ds, 'SeriesNumber', '')
                    
                    series_metadata[grouping_key] = {
                        'series_description': series_desc,
                        'series_number': series_number,
                        'modality': modality
                    }
                
                series_groups[grouping_key].append(file_path)
                
            except Exception as e:
                self.logger.warning(f"Could not read DICOM file {file_path}: {e}")
                # Add to unknown series group with placeholder patient info
                unknown_key = ('unknown', '', '', 'unknown_study', 'unknown_series')
                if unknown_key not in series_groups:
                    series_groups[unknown_key] = []
                    series_metadata[unknown_key] = {
                        'series_description': None,
                        'series_number': None,
                        'modality': None
                    }
                series_groups[unknown_key].append(file_path)
        
        self.logger.info(f"Pre-computed {patient_count} unique patients during file grouping")
        self.logger.info(f"Created {len(series_groups)} series groups from {len(dicom_files)} files")
        
        # Create DicomSeries objects with Patient/Study/Series UID hierarchy
        all_series = []
        
        for grouping_key, files in series_groups.items():
            patient_id, patient_name, birthdate, study_uid, series_uid = grouping_key
            metadata = series_metadata[grouping_key]
            
            # Create DicomSeries object
            series = DicomSeries(
                original_patient_id=patient_id,
                original_patient_name=patient_name,
                original_patient_birthdate=birthdate,
                original_study_uid=study_uid,
                original_series_uid=series_uid
            )
            
            # Set series-level metadata (stored once, accessible everywhere)
            series.series_description = metadata['series_description']
            series.series_number = metadata['series_number']
            series.modality = metadata['modality']
            
            # Generate anonymized UIDs if patient_uid_db is available
            if self.patient_uid_db and patient_id != 'unknown':
                try:
                    # Get patient's anonymized ID and random token from database
                    cached_result = self.patient_uid_db.get_cached_patient_id(
                        patient_id, patient_name, birthdate
                    )
                    
                    if cached_result:
                        anonymized_patient_id, random_token = cached_result
                        
                        # Set anonymized patient ID
                        series.anonymized_patient_id = anonymized_patient_id
                        
                        # Generate anonymized Study and Series UIDs using HMAC
                        project_hash_root = self.config.get('projectHashRoot', '')
                        series.generate_anonymized_uids(random_token, project_hash_root)
                        
                        # Build hierarchical output path with deterministic hashing
                        try:
                            series.build_output_path(
                                self.output_directory,
                                max_path_length=200
                            )
                            self.logger.debug(
                                f"Built output path for series: {series.output_base_path}"
                            )
                        except PathTooLongError as e:
                            self.logger.error(f"Path too long error: {e}")
                            # Fall back to using a simple series UID-based folder
                            clean_uid = "".join(c for c in series_uid if c.isalnum())
                            fallback_folder = f"series_{clean_uid[-10:]}"
                            series.output_base_path = os.path.join(
                                self.output_directory, fallback_folder
                            )
                    else:
                        self.logger.warning(
                            f"Could not find patient mapping for {patient_id}, "
                            f"skipping UID-based path generation"
                        )
                except Exception as e:
                    self.logger.warning(
                        f"Error generating anonymized UIDs for series: {e}, "
                        f"skipping UID-based path generation"
                    )
            
            # Create and add DicomFile objects with sequential naming
            files.sort()  # Ensure consistent ordering
            for idx, file_path in enumerate(files, start=1):
                dicom_file = DicomFile(file_path, series_uid)
                # Assign sequential filename (000001.dcm, 000002.dcm, etc.)
                dicom_file.filename = f"{idx:06d}.dcm"
                series.add_file(dicom_file)
            
            all_series.append(series)
        
        self.logger.info(f"Created {len(all_series)} DicomSeries objects")
        
        return all_series
