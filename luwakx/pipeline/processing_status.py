"""Processing status enumeration for DICOM file tracking.

This module defines the possible states a DICOM file can be in during the
anonymization pipeline processing.
"""

from enum import Enum, auto


class ProcessingStatus(Enum):
    """Enum representing the processing status of a DICOM file or series.

    The values are ordered: each state implies all preceding states are complete.
    This ordering is used by the checkpoint database to determine which pipeline
    stages have been completed and what pre-resume cleanup is needed.

    Attributes:
        ORIGINAL:   File/series is in its original, unprocessed state.
        ORGANIZED:  File has been copied into the organised temp folder.
        DEFACED:    Visual feature defacing is complete (or was not required).
        ANONYMIZED: DICOM tag anonymization is complete.
        EXPORTED:   UID mappings, metadata and review flags have been written
                    to the export files. Only series with this status are
                    considered fully done by the checkpoint database.
        FAILED:     Processing has failed for this file/series.
    """
    
    ORIGINAL = auto()
    ORGANIZED = auto()
    DEFACED = auto()
    ANONYMIZED = auto()
    EXPORTED = auto()
    FAILED = auto()

    # ------------------------------------------------------------------
    # Ordering helpers
    # ------------------------------------------------------------------

    def __lt__(self, other: 'ProcessingStatus') -> bool:
        if self.__class__ is not other.__class__:
            return NotImplemented
        # FAILED is treated as outside the normal ordering
        _order = [
            ProcessingStatus.ORIGINAL,
            ProcessingStatus.ORGANIZED,
            ProcessingStatus.DEFACED,
            ProcessingStatus.ANONYMIZED,
            ProcessingStatus.EXPORTED,
        ]
        try:
            return _order.index(self) < _order.index(other)
        except ValueError:
            return False

    def __le__(self, other: 'ProcessingStatus') -> bool:
        return self == other or self.__lt__(other)

    def __gt__(self, other: 'ProcessingStatus') -> bool:
        if self.__class__ is not other.__class__:
            return NotImplemented
        return other.__lt__(self)

    def __ge__(self, other: 'ProcessingStatus') -> bool:
        return self == other or self.__gt__(other)

    def __str__(self):
        """Return human-readable string representation."""
        return self.name

    def __repr__(self):
        """Return detailed representation."""
        return f"ProcessingStatus.{self.name}"
