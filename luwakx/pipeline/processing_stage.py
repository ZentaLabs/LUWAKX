"""Processing stage enumeration for anonymization pipeline.

This module defines the sequential stages of the DICOM anonymization pipeline,
representing the workflow from input scanning to final cleanup.
"""

from enum import Enum, auto


class ProcessingStage(Enum):
    """Enum representing the processing stages in the anonymization pipeline.
    
    Attributes:
        INPUT_SCANNING: Initial scanning of input DICOM files
        SERIES_ORGANIZATION: Organization of files by SeriesInstanceUID into folder structure
        VISUAL_DEFACING: Removal of recognizable visual features (faces, etc.)
        DICOM_ANONYMIZATION: DICOM tag anonymization using deid recipes
        EXPORT_METADATA: Export of metadata and UID mappings
        CLEANUP: Cleanup of temporary directories and files
    """
    
    INPUT_SCANNING = auto()
    SERIES_ORGANIZATION = auto()
    VISUAL_DEFACING = auto()
    DICOM_ANONYMIZATION = auto()
    EXPORT_METADATA = auto()
    CLEANUP = auto()
    
    def __str__(self):
        """Return human-readable string representation."""
        return self.name.replace('_', ' ').title()
    
    def __repr__(self):
        """Return detailed representation."""
        return f"ProcessingStage.{self.name}"
    
    def get_next_stage(self):
        """Get the next processing stage in the pipeline.
        
        Returns:
            ProcessingStage: The next stage, or None if this is the last stage
        """
        stages = list(ProcessingStage)
        current_index = stages.index(self)
        
        if current_index < len(stages) - 1:
            return stages[current_index + 1]
        return None
    
    def get_previous_stage(self):
        """Get the previous processing stage in the pipeline.
        
        Returns:
            ProcessingStage: The previous stage, or None if this is the first stage
        """
        stages = list(ProcessingStage)
        current_index = stages.index(self)
        
        if current_index > 0:
            return stages[current_index - 1]
        return None
