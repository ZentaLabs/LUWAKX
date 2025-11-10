"""DICOM series representation for managing related files.

This module provides the DicomSeries class which groups related DICOM files
by Patient/Study/Series UID hierarchy and manages their collective processing.
"""

import os
import hashlib
import hmac
from typing import Any, Dict, List, Optional, Tuple
import pydicom
from dicom_file import DicomFile
from processing_status import ProcessingStatus


class PathTooLongError(Exception):
    """Raised when output path exceeds maximum length limit."""
    pass


class DicomSeries:
    """Represents a DICOM series grouped by Patient/Study/Series UID hierarchy.
    
    This class manages a collection of DicomFile objects that belong to the same
    patient, study, and series, providing high-level operations for batch processing
    and hierarchical path management.
    
    Attributes:
        # Original identifiers (from DICOM metadata)
        original_patient_id: Original Patient ID from DICOM
        original_patient_name: Original Patient Name from DICOM  
        original_patient_birthdate: Original Patient Birth Date from DICOM
        original_study_uid: Original Study Instance UID
        original_series_uid: Original Series Instance UID
        
        # Anonymized identifiers (generated during pre-computation)
        anonymized_patient_id: Anonymized patient ID (e.g., "Zenta00")
        anonymized_study_uid: Anonymized Study Instance UID
        anonymized_series_uid: Anonymized Series Instance UID
        
        # File collection
        files: List of DicomFile objects in this series
        
        # DICOM metadata
        series_description: Series description from DICOM metadata
        series_number: Series number from DICOM metadata
        modality: Imaging modality (CT, MR, etc.)
        
        # Processing state
        processing_status: Overall processing status of the series
        
        # Base paths for different processing stages (all use UID hierarchy)
        organized_base_path: Base path for organized structure (PatientID/StudyUID/SeriesUID)
        defaced_base_path: Base path for defaced structure (PatientID/StudyUID/SeriesUID)
        output_base_path: Base path for final output (PatientID/StudyUID/SeriesUID)
        
        # Metadata
        metadata: Dictionary for tracking additional artifacts during processing
    """
    
    def __init__(self, 
                 original_patient_id: str,
                 original_patient_name: str,
                 original_patient_birthdate: str,
                 original_study_uid: str,
                 original_series_uid: str,
                 anonymized_patient_id: str = None):
        """Initialize a DicomSeries instance.
        
        Args:
            original_patient_id: Original Patient ID from DICOM
            original_patient_name: Original Patient Name from DICOM
            original_patient_birthdate: Original Patient Birth Date from DICOM
            original_study_uid: Original Study Instance UID
            original_series_uid: Original Series Instance UID
            anonymized_patient_id: Pre-computed anonymized patient ID (optional)
        """
        # Original identifiers
        self.original_patient_id = original_patient_id
        self.original_patient_name = original_patient_name
        self.original_patient_birthdate = original_patient_birthdate
        self.original_study_uid = original_study_uid
        self.original_series_uid = original_series_uid
        
        # Anonymized identifiers (will be set during pre-computation)
        self.anonymized_patient_id = anonymized_patient_id
        self.anonymized_study_uid: Optional[str] = None
        self.anonymized_series_uid: Optional[str] = None
        
        # File collection
        self.files: List[DicomFile] = []
        
        # DICOM metadata attributes
        self.series_description: Optional[str] = None
        self.series_number: Optional[str] = None
        self.modality: Optional[str] = None
        
        # Processing state
        self.processing_status = ProcessingStatus.ORIGINAL
        
        # Base paths for different processing stages
        self.organized_base_path: Optional[str] = None
        self.defaced_base_path: Optional[str] = None
        self.output_base_path: Optional[str] = None
        
        # Metadata dictionary for tracking additional artifacts during processing
        # Used to store NRRD file paths, processing metrics, etc.
        self.metadata: Dict[str, Any] = {}
        # Expected keys:
        # - 'nrrd_image_path': str - temp path to image.nrrd (original volume)
        # - 'nrrd_defaced_path': str - temp path to image_defaced.nrrd (defaced volume)
        # - 'series_folder_path': str - folder path for this series in organized structure
        # - Any other series-specific data needed during pipeline execution
    
    @property
    def grouping_key(self) -> Tuple[str, str, str, str, str]:
        """Get unique grouping key for this series.
        
        Returns:
            Tuple of (patient_id, patient_name, birthdate, study_uid, series_uid)
        """
        return (
            self.original_patient_id,
            self.original_patient_name,
            self.original_patient_birthdate,
            self.original_study_uid,
            self.original_series_uid
        )
    
    def set_anonymized_uids(self, patient_id: str, study_uid: str, series_uid: str) -> None:
        """Set all anonymized UIDs at once.
        
        Args:
            patient_id: Anonymized patient ID (e.g., "Zenta00")
            study_uid: Anonymized Study Instance UID
            series_uid: Anonymized Series Instance UID
        """
        self.anonymized_patient_id = patient_id
        self.anonymized_study_uid = study_uid
        self.anonymized_series_uid = series_uid
    
    @staticmethod
    def _compute_hmac(key: bytes, project_root: str, original_uid: str) -> str:
        """Compute HMAC-SHA512 for UID generation.
        
        Args:
            key: Raw bytes secret key (from patient UID database random token)
            project_root: Project hash root for isolation
            original_uid: Original DICOM UID to be anonymized
            
        Returns:
            Hex string of HMAC-SHA512 digest
        """
        data = f"{project_root}||{original_uid}".encode('utf-8')
        mac = hmac.new(key, data, hashlib.sha512)
        return mac.hexdigest()
    
    def generate_anonymized_uids(self, patient_random_token: bytes, 
                                 project_hash_root: str) -> None:
        """Generate anonymized Study and Series UIDs using HMAC.
        
        Args:
            patient_random_token: Patient's cryptographic random token
            project_hash_root: Project hash root for isolation
        """
        if not pydicom:
            raise ImportError("pydicom is required for UID generation")
        
        # Generate Study UID
        study_hmac = self._compute_hmac(
            patient_random_token,
            project_hash_root,
            self.original_study_uid
        )
        self.anonymized_study_uid = pydicom.uid.generate_uid(entropy_srcs=[study_hmac])
        
        # Generate Series UID
        series_hmac = self._compute_hmac(
            patient_random_token,
            project_hash_root,
            self.original_series_uid
        )
        self.anonymized_series_uid = pydicom.uid.generate_uid(entropy_srcs=[series_hmac])
    
    def build_output_path(self, base_output_dir: str, 
                         max_path_length: int = 200,
                         used_paths: Optional[set] = None) -> str:
        """Build hierarchical output path: PatientID/StudyUID/SeriesUID/
        
        Handles collision detection by appending _1, _2, etc. if path already exists.
        
        Args:
            base_output_dir: Base output directory
            max_path_length: Maximum allowed path length (default: 200)
            used_paths: Set of already-used paths for collision detection
            
        Returns:
            str: Built output path
            
        Raises:
            ValueError: If anonymized UIDs not set
            PathTooLongError: If path exceeds maximum length
        """
        if not all([self.anonymized_patient_id, 
                   self.anonymized_study_uid, 
                   self.anonymized_series_uid]):
            raise ValueError(
                f"Anonymized UIDs not set for series {self.original_series_uid}"
            )
        
        # Build base path
        base_path = os.path.join(
            base_output_dir,
            self.anonymized_patient_id,
            self.anonymized_study_uid,
            self.anonymized_series_uid
        )
        
        # Handle collision detection
        if used_paths is not None:
            original_path = base_path
            suffix = 1
            while base_path in used_paths:
                base_path = f"{original_path}_{suffix}"
                suffix += 1
            
            # Add to used paths
            used_paths.add(base_path)
        
        # Validate path length (reserve space for filename like /000001.dcm)
        max_filename_length = 12  # "/000001.dcm" = 11 chars + 1 for safety
        if len(base_path) + max_filename_length > max_path_length:
            raise PathTooLongError(
                f"Output path exceeds {max_path_length} characters: {base_path}\n"
                f"Length: {len(base_path)} + {max_filename_length} = "
                f"{len(base_path) + max_filename_length}"
            )
        
        self.output_base_path = base_path
        return base_path
    
    def add_file(self, file: DicomFile) -> None:
        """Add a DicomFile to this series.
        
        Args:
            file: DicomFile instance to add
            
        Raises:
            ValueError: If file's series_uid doesn't match this series
        """
        if file.series_uid != self.original_series_uid:
            raise ValueError(
                f"File series_uid '{file.series_uid}' does not match "
                f"series '{self.original_series_uid}'"
            )
        self.files.append(file)
    
    def get_files_by_status(self, status: ProcessingStatus) -> List[DicomFile]:
        """Get all files with a specific processing status.
        
        Args:
            status: ProcessingStatus to filter by
            
        Returns:
            List[DicomFile]: Files matching the specified status
        """
        return [f for f in self.files if f.processing_status == status]
    
    def get_original_files(self) -> List[str]:
        """Get list of original file paths.
        
        Returns:
            List[str]: Original file paths for all files in series
        """
        return [f.original_path for f in self.files]
    
    def get_organized_files(self) -> List[str]:
        """Get list of organized file paths.
        
        Returns:
            List[str]: Organized file paths (may contain None values)
        """
        return [f.organized_path for f in self.files if f.organized_path is not None]
    
    def get_defaced_files(self) -> List[str]:
        """Get list of defaced file paths.
        
        Returns:
            List[str]: Defaced file paths (may contain None values)
        """
        return [f.defaced_path for f in self.files if f.defaced_path is not None]
    
    def get_anonymized_files(self) -> List[str]:
        """Get list of anonymized file paths.
        
        Returns:
            List[str]: Anonymized file paths (may contain None values)
        """
        return [f.anonymized_path for f in self.files if f.anonymized_path is not None]
    
    def update_base_paths(self, organized: Optional[str] = None, 
                         defaced: Optional[str] = None) -> None:
        """Update base directory paths for this series using UID hierarchy.
        
        Requires anonymized UIDs to be set before calling.
        
        Args:
            organized: Base directory for organized structure
            defaced: Base directory for defaced structure
            
        Raises:
            ValueError: If anonymized UIDs not set
        """
        if not all([self.anonymized_patient_id, 
                   self.anonymized_study_uid, 
                   self.anonymized_series_uid]):
            raise ValueError(
                f"Anonymized UIDs must be set before calling update_base_paths for series {self.original_series_uid}"
            )
        
        # Build UID-based path hierarchy
        uid_path = os.path.join(
            self.anonymized_patient_id,
            self.anonymized_study_uid,
            self.anonymized_series_uid
        )
        
        if organized is not None:
            self.organized_base_path = os.path.join(organized, uid_path)
        if defaced is not None:
            self.defaced_base_path = os.path.join(defaced, uid_path)
    
    def calculate_file_paths_for_stage(self, stage_path: str, 
                                       path_setter: str) -> None:
        """Calculate and set file paths for a specific processing stage.
        
        Args:
            stage_path: Base directory for this stage (e.g., organized_base_path)
            path_setter: Method name to call on files ('set_organized_path', etc.)
        """
        if not stage_path:
            return
        
        for file in self.files:
            file_path = os.path.join(stage_path, file.filename)
            getattr(file, path_setter)(file_path)
    
    def is_ready_for_processing(self) -> bool:
        """Check if series has files and is ready for processing.
        
        Returns:
            bool: True if series contains files, False otherwise
        """
        return len(self.files) > 0
    
    def get_file_count(self) -> int:
        """Get the number of files in this series.
        
        Returns:
            int: Number of files
        """
        return len(self.files)
    
    def get_series_info(self) -> Dict[str, Any]:
        """Get dictionary of series information.
        
        Returns:
            Dict[str, Any]: Dictionary containing series metadata
        """
        return {
            'original_patient_id': self.original_patient_id,
            'original_patient_name': self.original_patient_name,
            'original_patient_birthdate': self.original_patient_birthdate,
            'original_study_uid': self.original_study_uid,
            'original_series_uid': self.original_series_uid,
            'anonymized_patient_id': self.anonymized_patient_id,
            'anonymized_study_uid': self.anonymized_study_uid,
            'anonymized_series_uid': self.anonymized_series_uid,
            'series_description': self.series_description,
            'series_number': self.series_number,
            'modality': self.modality,
            'file_count': self.get_file_count(),
            'processing_status': str(self.processing_status),
            'output_base_path': self.output_base_path
        }
    
    def __repr__(self) -> str:
        """Return detailed string representation.
        
        Returns:
            str: String representation with series info
        """
        anon_info = ""
        if self.anonymized_patient_id and self.anonymized_study_uid and self.anonymized_series_uid:
            anon_info = f", anon='{self.anonymized_patient_id}/{self.anonymized_study_uid[:10]}.../{self.anonymized_series_uid[:10]}...'"
        
        return (f"DicomSeries(patient='{self.original_patient_id}', "
                f"study='{self.original_study_uid[:10]}...', "
                f"series='{self.original_series_uid[:10]}...', "
                f"files={len(self.files)}, "
                f"status={self.processing_status}{anon_info})")
    
    def __str__(self) -> str:
        """Return human-readable string representation.
        
        Returns:
            str: Simple string with patient ID and file count
        """
        patient_display = self.anonymized_patient_id or self.original_patient_id
        series_display = f"{self.series_description or self.original_series_uid[:15]}"
        return f"{patient_display}/{series_display} ({len(self.files)} files) [{self.processing_status}]"
