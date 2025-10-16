"""DICOM series representation for managing related files.

This module provides the DicomSeries class which groups related DICOM files
(sharing the same SeriesInstanceUID) and manages their collective processing.
"""

import os
from typing import Any, Dict, List, Optional
from dicom_file import DicomFile
from processing_status import ProcessingStatus


class DicomSeries:
    """Represents a DICOM series (collection of related files with same SeriesInstanceUID).
    
    This class manages a collection of DicomFile objects that belong to the same
    series, providing high-level operations for batch processing and path management.
    
    Attributes:
        series_uid: SeriesInstanceUID for this series
        folder_name: Descriptive folder name for this series
        files: List of DicomFile objects in this series
        series_description: Series description from DICOM metadata
        series_number: Series number from DICOM metadata
        modality: Imaging modality (CT, MR, etc.)
        processing_status: Overall processing status of the series
        organized_base_path: Base path for organized structure
        defaced_base_path: Base path for defaced structure
        output_base_path: Base path for final output
    """
    
    def __init__(self, series_uid: str, folder_name: str):
        """Initialize a DicomSeries instance.
        
        Args:
            series_uid: SeriesInstanceUID for this series
            folder_name: Descriptive folder name (e.g., "003_MR_T2_FLAIR")
        """
        self.series_uid = series_uid
        self.folder_name = folder_name
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
    
    def add_file(self, file: DicomFile) -> None:
        """Add a DicomFile to this series.
        
        Args:
            file: DicomFile instance to add
            
        Raises:
            ValueError: If file's series_uid doesn't match this series
        """
        if file.series_uid != self.series_uid:
            raise ValueError(
                f"File series_uid '{file.series_uid}' does not match "
                f"series '{self.series_uid}'"
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
                         defaced: Optional[str] = None, 
                         output: Optional[str] = None) -> None:
        """Update base directory paths for this series.
        
        Args:
            organized: Base path for organized structure
            defaced: Base path for defaced structure
            output: Base path for final output
        """
        if organized is not None:
            self.organized_base_path = os.path.join(organized, self.folder_name)
        if defaced is not None:
            self.defaced_base_path = os.path.join(defaced, self.folder_name)
        if output is not None:
            self.output_base_path = os.path.join(output, self.folder_name)
    
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
            'series_uid': self.series_uid,
            'folder_name': self.folder_name,
            'series_description': self.series_description,
            'series_number': self.series_number,
            'modality': self.modality,
            'file_count': self.get_file_count(),
            'processing_status': str(self.processing_status)
        }
    
    def __repr__(self) -> str:
        """Return detailed string representation.
        
        Returns:
            str: String representation with series info
        """
        return (f"DicomSeries(uid='{self.series_uid}', "
                f"folder='{self.folder_name}', "
                f"files={len(self.files)}, "
                f"status={self.processing_status})")
    
    def __str__(self) -> str:
        """Return human-readable string representation.
        
        Returns:
            str: Simple string with folder name and file count
        """
        return f"{self.folder_name} ({len(self.files)} files) [{self.processing_status}]"
