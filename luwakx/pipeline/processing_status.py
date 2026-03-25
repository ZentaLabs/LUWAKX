"""Processing status enumeration for DICOM file tracking.

This module defines the possible states a DICOM file can be in during the
anonymization pipeline processing.
"""

from enum import Enum, auto


class ProcessingStatus(Enum):
    """Enum representing the processing status of a DICOM file.
    
    Attributes:
        ORIGINAL: File is in its original, unprocessed state
        ORGANIZED: File has been organized into series-based folder structure
        DEFACED: File has undergone visual feature defacing (if applicable)
        ANONYMIZED: File has been fully anonymized
        FAILED: Processing of this file has failed
    """
    
    ORIGINAL = auto()
    ORGANIZED = auto()
    DEFACED = auto()
    ANONYMIZED = auto()
    FAILED = auto()
    
    def __str__(self):
        """Return human-readable string representation."""
        return self.name
    
    def __repr__(self):
        """Return detailed representation."""
        return f"ProcessingStatus.{self.name}"
