"""DICOM file representation for tracking through processing pipeline.

This module provides the DicomFile class which encapsulates all state and path
information for a single DICOM file as it moves through the anonymization pipeline.
"""

import os
from typing import Any, Dict, Optional
from ..pipeline.processing_status import ProcessingStatus


class DicomFile:
    """Represents a single DICOM file and tracks its state through processing stages.
    
    This class encapsulates all path information and metadata for a DICOM file,
    providing controlled access to file state and preventing direct manipulation
    of internal data structures.
    
    Attributes:
        original_path: Original input file path
        filename: Sequential filename assigned by DicomSeriesFactory (e.g., 000001.dcm)
                 Initialized from original_path basename, then overwritten with sequential name
        series_uid: SeriesInstanceUID this file belongs to
        organized_path: Path after series organization (or None)
        defaced_path: Path after visual defacing (or None)
        anonymized_path: Path after DICOM anonymization (or None)
        processing_status: Current processing status
        metadata: Additional metadata dictionary for file-specific data
    """
    
    def __init__(self, original_path: str, series_uid: str):
        """Initialize a DicomFile instance.
        
        Args:
            original_path: Full path to the original DICOM file
            series_uid: SeriesInstanceUID this file belongs to
        """
        self.original_path = original_path
        self.filename = os.path.basename(original_path)
        self.series_uid = series_uid
        
        # Path tracking through processing stages
        self.organized_path: Optional[str] = None
        self.defaced_path: Optional[str] = None
        self.anonymized_path: Optional[str] = None
        
        # Status tracking
        self.processing_status = ProcessingStatus.ORIGINAL
        
        # Extensible metadata storage
        self.metadata: Dict[str, Any] = {}
    
    def get_current_path(self) -> str:
        """Get the most recent path for this file based on processing status.
        
        Returns:
            str: The current file path (most recently processed location)
        """
        if self.processing_status == ProcessingStatus.ANONYMIZED and self.anonymized_path:
            return self.anonymized_path
        elif self.processing_status == ProcessingStatus.DEFACED and self.defaced_path:
            return self.defaced_path
        elif self.processing_status == ProcessingStatus.ORGANIZED and self.organized_path:
            return self.organized_path
        else:
            return self.original_path
    
    def set_organized_path(self, path: str) -> None:
        """Set the organized path and update status.
        
        Args:
            path: Path to the file in the organized structure
        """
        self.organized_path = path
        if self.processing_status == ProcessingStatus.ORIGINAL:
            self.processing_status = ProcessingStatus.ORGANIZED
    
    def set_defaced_path(self, path: str) -> None:
        """Set the defaced path and update status.
        
        Args:
            path: Path to the defaced file
        """
        self.defaced_path = path
        if self.processing_status in (ProcessingStatus.ORIGINAL, ProcessingStatus.ORGANIZED):
            self.processing_status = ProcessingStatus.DEFACED
    
    def set_anonymized_path(self, path: str) -> None:
        """Set the anonymized path and update status.
        
        Args:
            path: Path to the fully anonymized file
        """
        self.anonymized_path = path
        if self.processing_status != ProcessingStatus.FAILED:
            self.processing_status = ProcessingStatus.ANONYMIZED
    
    def update_status(self, status: ProcessingStatus) -> None:
        """Manually update the processing status.
        
        Args:
            status: New processing status
        """
        self.processing_status = status
    
    def is_processed(self) -> bool:
        """Check if file has been fully processed (anonymized).
        
        Returns:
            bool: True if file is anonymized, False otherwise
        """
        return self.processing_status == ProcessingStatus.ANONYMIZED
    
    def get_metadata(self, key: str) -> Optional[Any]:
        """Retrieve metadata value by key.
        
        Args:
            key: Metadata key to retrieve
            
        Returns:
            Any: Metadata value, or None if key doesn't exist
        """
        return self.metadata.get(key)
    
    def set_metadata(self, key: str, value: Any) -> None:
        """Store metadata value.
        
        Args:
            key: Metadata key
            value: Metadata value to store
        """
        self.metadata[key] = value
    
    def get_relative_original_path(self, input_folder: str) -> str:
        """Get original file path relative to input folder.
        
        Args:
            input_folder: Input directory base path from config
            
        Returns:
            str: Relative path from input folder (with forward slashes)
        """
        try:
            rel_path = os.path.relpath(self.original_path, input_folder)
            # Normalize to forward slashes for cross-platform consistency
            return rel_path.replace(os.sep, '/')
        except ValueError:
            # If paths are on different drives (Windows), return basename
            return self.filename
    
    def get_relative_anonymized_path(self, output_folder: str) -> str:
        """Get anonymized file path relative to output folder.
        
        Args:
            output_folder: Output directory base path from config
            
        Returns:
            str: Relative path from output folder (with forward slashes), or empty string if not anonymized
        """
        if not self.anonymized_path:
            return ''
        
        try:
            rel_path = os.path.relpath(self.anonymized_path, output_folder)
            # Normalize to forward slashes for cross-platform consistency
            return rel_path.replace(os.sep, '/')
        except ValueError:
            # If paths are on different drives (Windows), return basename
            return self.filename
    
    def __repr__(self) -> str:
        """Return detailed string representation.
        
        Returns:
            str: String representation showing filename and status
        """
        return f"DicomFile(filename='{self.filename}', status={self.processing_status}, series_uid='{self.series_uid}')"
    
    def __str__(self) -> str:
        """Return human-readable string representation.
        
        Returns:
            str: Simple string showing filename
        """
        return f"{self.filename} [{self.processing_status}]"
