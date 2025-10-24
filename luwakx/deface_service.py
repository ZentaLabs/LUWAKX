"""Deface service for visual feature processing.

This module provides the DefaceService class which handles removal of
recognizable visual features (faces) from medical images using ML models.

Extracted from anonymize.py in Phase 2 refactoring.
"""

import os
import shutil
import traceback
import importlib.util
from typing import Any, Dict

from dicom_series import DicomSeries
from utils import cleanup_gpu_memory
from luwak_logger import log_project_stacktrace


class DefaceService:
    """Handles visual feature defacing for medical images.
    
    This service manages face detection and pixelation using ML models,
    working with DICOM series to remove identifiable visual features.
    
    Attributes:
        config: Configuration dictionary (read-only)
        logger: Logger instance
        external_mask_paths: List of paths to external mask files
    """
    
    def __init__(self, config: Dict[str, Any], logger):
        """Initialize DefaceService.
        
        Args:
            config: Configuration dictionary
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        
        # Configuration for defacing strategy
        #self.use_external_mask = config.get('use_external_mask', False)
        self.external_mask_paths = config.get('testOptions', {}).get('useExistingMaskDefacer', [])
        if self.external_mask_paths:
            self.external_mask_paths = [os.path.abspath(m) for m in self.external_mask_paths]
        
        # Track series counter for external mask indexing
        self._series_counter = 0
    
    def process_series(self, series: DicomSeries) -> Dict[str, Any]:
        """Process (deface) a single DICOM series.
        
        NOTE: This method trusts the caller's decision that defacing is needed.
        ProcessingPipeline._needs_defacing() makes the business logic decision.
        This service just performs the technical operation.
        
        Args:
            series: DicomSeries to process (assumed to need defacing)
            
        Returns:
            dict: Result dictionary containing:
                - 'nrrd_image_path': Path to image.nrrd (original volume with faces)
                - 'nrrd_defaced_path': Path to image_defaced.nrrd (defaced volume)
                - 'defaced_dicom_files': List of defaced DICOM file paths
        """
        import pydicom
        
        try:
            import SimpleITK
        except ImportError:
            self.logger.error("SimpleITK is required for defacing but not installed")
            raise
        
        self.logger.info(f"DefaceService: Defacing series {series.folder_name}")
        
        # Get organized files for this series
        organized_files = series.get_organized_files()
        if not organized_files:
            self.logger.warning(f"No organized files found for series {series.series_uid}")
            return self._copy_without_defacing(series)
        
        # Create worker-specific temp directory for NRRD processing
        series_temp_dir = os.path.join(series.defaced_base_path, series.folder_name)
        os.makedirs(series_temp_dir, exist_ok=True)
        
        try:
            # Load defacer module
            defacer_path = os.path.join(
                os.path.dirname(__file__), "scripts", "defacing", 
                "image_defacer", "image_anonymization.py"
            )
            spec = importlib.util.spec_from_file_location("image_anonymization", defacer_path)
            defacer = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(defacer)
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_info = f" (line {tb[-1].lineno} in {tb[-1].filename})" if tb else ""
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to load defacer module: {e}")
            return self._copy_without_defacing(series)
        
        # Read DICOM metadata
        try:
            ds = pydicom.dcmread(organized_files[0])
            modality = ds.Modality if 'Modality' in ds else None
            body_part = ds.BodyPartExamined if 'BodyPartExamined' in ds else None
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to read DICOM metadata: {e}")
            return self._copy_without_defacing(series)
        
        self.logger.private(
            f"Defacing series {series.series_uid} with modality {modality} "
            f"and body part {body_part}"
        )
        
        # Load DICOM series as 3D volume
        reader = SimpleITK.ImageSeriesReader()
        try:
            # Sort files properly for 3D reconstruction
            # Use GDCM to get properly sorted file names for this specific series
            # This handles ImagePositionPatient sorting correctly
            series_folder = os.path.dirname(organized_files[0])
            reader.SetFileNames(reader.GetGDCMSeriesFileNames(series_folder, series.series_uid))
            image = reader.Execute()
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to load DICOM series as volume: {e}")
            return self._copy_without_defacing(series)
        
        # Apply face detection/segmentation strategy
        try:
            if self.external_mask_paths:
                # Strategy 1: Use pre-computed external mask
                self.logger.info("Using external mask for face segmentation")
                mask_path = self.external_mask_paths[self._series_counter]
                image_face_segmentation = defacer.prepare_face_mask(image, modality, mask_path)
                self._series_counter += 1
            else:
                # Strategy 2: Run ML face detection
                self.logger.info("Running ML face detection model")
                image_face_segmentation = defacer.prepare_face_mask(image, modality)
                
                # Clean up GPU memory immediately after ML inference
                cleanup_gpu_memory()
                self.logger.debug("GPU memory cleaned up after face detection")
                
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to generate face mask: {e}")
            return self._copy_without_defacing(series)
        
        # Apply pixelation to create defaced volume
        try:
            image_defaced = defacer.pixelate_face(image, image_face_segmentation)
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to pixelate faces: {e}")
            return self._copy_without_defacing(series)
        
        # Save NRRD volumes to temp directory
        nrrd_image_path = os.path.join(series_temp_dir, "image.nrrd")
        nrrd_defaced_path = os.path.join(series_temp_dir, "image_defaced.nrrd")
        
        try:
            SimpleITK.WriteImage(image, nrrd_image_path)
            SimpleITK.WriteImage(image_defaced, nrrd_defaced_path)
            self.logger.debug(f"Saved NRRD volumes to {series_temp_dir}")
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to save NRRD volumes: {e}")
            return self._copy_without_defacing(series)
        
        # Convert defaced volume back to DICOM files
        defaced_array = SimpleITK.GetArrayFromImage(image_defaced)  # Shape: [slices, height, width]
        defaced_dicom_files = []
        
        # Build mapping from organized_path to DicomFile for efficient lookup
        organized_to_dicom_file = {f.organized_path: f for f in series.files if f.organized_path is not None}
        
        for i, original_file_path in enumerate(organized_files):
            try:
                ds = pydicom.dcmread(original_file_path)
                
                # Get rescale parameters
                rescale_slope = getattr(ds, 'RescaleSlope', 1.0)
                rescale_intercept = getattr(ds, 'RescaleIntercept', 0.0)
                
                # Apply inverse scaling to get back to raw values
                raw_pixels = ((defaced_array[i] - rescale_intercept) / rescale_slope).round().astype(ds.pixel_array.dtype)
                ds.PixelData = raw_pixels.tobytes()
                
                # Save defaced DICOM file
                defaced_file_path = os.path.join(series_temp_dir, os.path.basename(original_file_path))
                ds.save_as(defaced_file_path)
                defaced_dicom_files.append(defaced_file_path)
                
                # Update DicomFile object with defaced path using direct mapping
                dicom_file = organized_to_dicom_file.get(original_file_path)
                if dicom_file:
                    dicom_file.set_defaced_path(defaced_file_path)
                
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                log_project_stacktrace(self.logger, e)
                self.logger.error(f"Failed to process DICOM slice {i}: {e}")
                continue
        
        self.logger.info(
            f"Defacing completed for series {series.folder_name}: "
            f"{len(defaced_dicom_files)} files processed"
        )
        
        # Return paths for caller to handle final placement
        return {
            'nrrd_image_path': nrrd_image_path,
            'nrrd_defaced_path': nrrd_defaced_path,
            'defaced_dicom_files': defaced_dicom_files
        }
    
    def _copy_without_defacing(self, series: DicomSeries) -> Dict[str, Any]:
        """Copy files without defacing when defacing fails or is not applicable.
        
        Args:
            series: DicomSeries to copy
            
        Returns:
            dict: Result dictionary with empty NRRD paths
        """
        self.logger.info(f"Copying files without defacing for series {series.folder_name}")
        
        organized_files = series.get_organized_files()
        defaced_folder = os.path.join(series.defaced_base_path, series.folder_name)
        os.makedirs(defaced_folder, exist_ok=True)
        
        # Build mapping from organized_path to DicomFile for efficient lookup
        organized_to_dicom_file = {f.organized_path: f for f in series.files if f.organized_path is not None}
        
        defaced_files = []
        for original_file_path in organized_files:
            try:
                defaced_file_path = os.path.join(defaced_folder, os.path.basename(original_file_path))
                shutil.copy2(original_file_path, defaced_file_path)
                defaced_files.append(defaced_file_path)
                
                # Update DicomFile object using direct mapping
                dicom_file = organized_to_dicom_file.get(original_file_path)
                if dicom_file:
                    dicom_file.set_defaced_path(defaced_file_path)
                        
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                log_project_stacktrace(self.logger, e)
                self.logger.warning(f"Failed to copy file {original_file_path}: {e}")
        
        return {
            'nrrd_image_path': None,
            'nrrd_defaced_path': None,
            'defaced_dicom_files': defaced_files
        }
