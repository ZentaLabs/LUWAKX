"""Deface service for visual feature processing.

This module provides the DefaceService class which handles removal of
recognizable visual features (faces) from medical images using AI models.

"""

import os
import shutil
import traceback
import importlib.util
from typing import Any, Dict, Optional

import numpy as np

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

    def __init__(self, config: Dict[str, Any], logger, deface_mask_db=None):
        """Initialize DefaceService.

        Args:
            config: Configuration dictionary
            logger: Logger instance
            deface_mask_db: Optional DefaceMaskDatabase instance.  When provided
                the service will check for a cached mask before running the ML
                model and will persist newly-computed masks for primary deface
                candidates.
        """
        self.config = config
        self.logger = logger

        # Optional deface-mask database (None when feature is not configured)
        self.deface_mask_db = deface_mask_db

        # Modalities for which mask caching is requested
        # (driven by config: saveDefaceMasks.primary = ["CT", "MR", ...])
        self.best_modalities: list = [
            m.upper()
            for m in config.get('saveDefaceMasks', {}).get('primary', [])
        ]

        # Private mapping folder is used to store persisted mask NRRD files
        self.private_folder: str = config.get('outputPrivateMappingFolder', '')

        # Configuration for defacing strategy
        self.external_mask_paths = config.get('testOptions', {}).get('useExistingMaskDefacer', [])
        if self.external_mask_paths:
            self.external_mask_paths = [os.path.abspath(m) for m in self.external_mask_paths]

        # Physical block size for pixelation (in mm)
        self.physical_block_size_mm = config.get('physicalFacePixelationSizeMm', 8.5)

        # Track series counter for external mask indexing
        self._series_counter = 0

        # Cached defacer module - loaded once on the first series to avoid
        # re-executing module-level moosez initialisation code on every series.
        self._defacer = None

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

        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#41-clean-recognizable-visual-features-defacing----pipeline-stage-3
        """
        import pydicom

        try:
            import SimpleITK
        except ImportError:
            self.logger.error("SimpleITK is required for defacing but not installed")
            raise

        series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
        self.logger.info(f"DefaceService: Defacing series {series_display}")

        # Get organized files for this series
        organized_files = series.get_organized_files()
        if not organized_files:
            self.logger.warning(f"No organized files found for series {series.original_series_uid}")
            return self._copy_without_defacing(series)

        # Create worker-specific temp directory for NRRD processing
        # Note: defaced_base_path already contains the complete UID hierarchy
        series_temp_dir = series.defaced_base_path
        os.makedirs(series_temp_dir, exist_ok=True)

        if self._defacer is None:
            try:
                # Load defacer module once and cache it on the instance to avoid
                # re-executing moosez module-level initialisation on every series.
                defacer_path = os.path.join(
                    os.path.dirname(__file__), "scripts", "defacing",
                    "image_defacer", "image_anonymization.py"
                )
                spec = importlib.util.spec_from_file_location("image_anonymization", defacer_path)
                self._defacer = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(self._defacer)
            except Exception as e:
                log_project_stacktrace(self.logger, e)
                self.logger.error(f"Failed to load defacer module: {e}")
                return self._copy_without_defacing(series)
        defacer = self._defacer

        # Get metadata from series (already loaded during series creation - no file re-reading!)
        modality = series.modality
        self.logger.private(
            f"Defacing series {series.original_series_uid} with modality {modality}"
        )

        # Load DICOM series as 3D volume
        reader = SimpleITK.ImageSeriesReader()
        try:
            # Sort files properly for 3D reconstruction
            # Use GDCM to get properly sorted file names for this specific series
            # This handles ImagePositionPatient sorting correctly
            series_folder = os.path.dirname(organized_files[0])
            gdcm_sorted_files = reader.GetGDCMSeriesFileNames(series_folder, series.original_series_uid)
            reader.SetFileNames(gdcm_sorted_files)
            image = reader.Execute()
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to load DICOM series as volume: {e}")
            return self._copy_without_defacing(series)

        # Apply face detection/segmentation strategy
        #
        # Strategies (highest priority first):
        #   1. Test-time external mask  (testOptions.useExistingMaskDefacer)
        #   2. Primary candidate        -> run ML, save mask to DB
        #   3. All other series         -> run ML normally, no DB interaction
        #
        # DefacePriorityElector determines the best series per (patient, study,
        # FrameOfReferenceUID, modality) group *before* the defacer runs, so
        # the primary is already the optimal candidate - no comparison or
        # overwrite logic is needed when persisting its mask.
        save_mask_after_ml = False

        try:
            if self.external_mask_paths:
                # Strategy 1: Use pre-computed external mask (test / override)
                self.logger.info("Using external mask for face segmentation")
                mask_path = self.external_mask_paths[self._series_counter]
                image_face_segmentation = defacer.prepare_face_mask(image, modality, mask_path)
                self._series_counter += 1

            elif series.is_primary_deface_candidate:
                # Strategy 2: Primary candidate - run ML and mark for DB persistence.
                # No DB lookup needed: by definition no better mask exists yet.
                self.logger.info(
                    f"Series is primary deface candidate for modality={modality}; "
                    f"running ML defacing."
                )
                image_face_segmentation = defacer.prepare_face_mask(image, modality)
                cleanup_gpu_memory()
                self.logger.debug("GPU memory cleaned up after face detection")
                save_mask_after_ml = True

            else:
                # Strategy 3: Non-primary or untracked modality.
                # Run ML defacing normally; do not persist the mask.
                image_face_segmentation = defacer.prepare_face_mask(image, modality)
                cleanup_gpu_memory()
                self.logger.debug("GPU memory cleaned up after face detection")

        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to generate face mask: {e}")
            return self._copy_without_defacing(series)

        # Apply pixelation to create defaced volume
        try:
            image_defaced = defacer.pixelate_face(image, image_face_segmentation, target_block_size_mm=self.physical_block_size_mm)
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to pixelate faces: {e}")
            return self._copy_without_defacing(series)

        # Save NRRD volumes to temp directory
        nrrd_image_path = os.path.join(series_temp_dir, "image.nrrd")
        nrrd_defaced_path = os.path.join(series_temp_dir, "image_defaced.nrrd")

        nrrd_mask_path = os.path.join(series_temp_dir, "mask.nrrd")

        try:
            SimpleITK.WriteImage(image, nrrd_image_path)
            SimpleITK.WriteImage(image_defaced, nrrd_defaced_path)
            SimpleITK.WriteImage(image_face_segmentation, nrrd_mask_path, useCompression=True)
            self.logger.debug(f"Saved NRRD volumes to {series_temp_dir}")
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to save NRRD volumes: {e}")

        # Persist the primary mask to the database (if elected as primary candidate)
        if save_mask_after_ml:
            try:
                self._persist_mask_to_db(series, image_face_segmentation, image)
            except Exception as e:
                log_project_stacktrace(self.logger, e)
                self.logger.warning(f"Failed to persist deface mask to database: {e}")

        # Convert defaced volume back to DICOM files.
        # We iterate over all files (any order is fine) and extract each slice from the
        # defaced 3D volume using the slice's own DICOM spatial metadata
        # (ImagePositionPatient / ImageOrientationPatient / PixelSpacing).
        # This approach guarantees that the pixel data written to each file
        # matches its spatial location.
        defaced_dicom_files = []

        # Build mapping from organized_path to DicomFile for efficient lookup
        organized_to_dicom_file = {f.organized_path: f for f in series.files if f.organized_path is not None}

        # Log whether axis-aligned or general orientation extraction will be used (check once for the series)
        _first_ds = pydicom.dcmread(gdcm_sorted_files[0])
        _iop_first = [float(x) for x in _first_ds.ImageOrientationPatient]
        del _first_ds  # full pydicom Dataset no longer needed - free it before the slice loop
        _axis_aligned = self._is_volume_axis_aligned(_iop_first)
        if _axis_aligned:
            self.logger.info(f"Volume has axis-aligned with the world (LPS) coordinate axes")
        else:
            self.logger.warning(f"Volume has axis not aligned with the world (LPS) coordinate axes, check final defacing result, this option is not tested")

        try:
            for original_file_path in gdcm_sorted_files:
                ds = pydicom.dcmread(original_file_path)

                # Extract the defaced 2D slice that corresponds to this DICOM file's
                # physical position/orientation - independent of file order.
                ipp = [float(x) for x in ds.ImagePositionPatient]
                iop = [float(x) for x in ds.ImageOrientationPatient]
                pixel_spacing = [float(x) for x in ds.PixelSpacing]
                slice_2d = self._extract_slice_from_volume(
                    image_defaced, ipp, iop, pixel_spacing, ds.Rows, ds.Columns
                )

                # Get rescale parameters
                rescale_slope = getattr(ds, 'RescaleSlope', 1.0)
                rescale_intercept = getattr(ds, 'RescaleIntercept', 0.0)

                # Apply inverse scaling to get back to raw stored values
                raw_pixels = ((slice_2d - rescale_intercept) / rescale_slope).round().astype(ds.pixel_array.dtype)
                ds.PixelData = raw_pixels.tobytes()

                # If the original file used a compressed transfer syntax, the raw
                # pixel bytes we just assigned are NOT encapsulated, so pydicom would
                # raise a ValueError on save.  Switch to Explicit VR Little Endian
                # (uncompressed) so the file can be written correctly.
                if hasattr(ds, 'file_meta') and ds.file_meta.TransferSyntaxUID.is_compressed:
                    ds.file_meta.TransferSyntaxUID = pydicom.uid.ExplicitVRLittleEndian
                    ds.is_implicit_VR = False
                    ds.is_little_endian = True

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
            self.logger.error(f"Failed to convert defaced volume to DICOM files: {e}")
            self.logger.error("Defacing failed - falling back to undefaced files")
            return self._copy_without_defacing(series)
        finally:
            # Explicitly release large 3D volumes now that the slice loop is done.
            # NRRD files are on disk; image_defaced was needed only for per-slice
            # extraction above.  Releasing here (rather than waiting for function-scope
            # GC) lets the C++/ITK allocator reclaim the memory before the next series.
            import gc as _gc
            try:
                del image_defaced
            except NameError:
                pass
            try:
                del image_face_segmentation
            except NameError:
                pass
            try:
                del reader
            except NameError:
                pass
            try:
                del image
            except NameError:
                pass
            _gc.collect()

        series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
        self.logger.info(
            f"Defacing completed for series {series_display}: "
            f"{len(defaced_dicom_files)} files processed"
        )

        # Mark defacing as successful
        series.defacing_succeeded = True

        # Return paths for caller to handle final placement
        return {
            'nrrd_image_path': nrrd_image_path,
            'nrrd_defaced_path': nrrd_defaced_path,
            'nrrd_mask_path': nrrd_mask_path,
            'defaced_dicom_files': defaced_dicom_files
        }

    @staticmethod
    def _is_volume_axis_aligned(iop) -> bool:
        """Check if the DICOM slice is aligned with the world (LPS) coordinate axes.

        Axis-aligned means the row and column direction cosines are each parallel
        to one of the standard basis vectors [1,0,0], [0,1,0], [0,0,1] (LPS axes).
        This covers standard axial, coronal and sagittal acquisitions.

        Args:
            iop: ImageOrientationPatient (list of 6 floats)

        Returns:
            True if axis-aligned, False otherwise.
        """
        axes = np.eye(3)  # [[1,0,0], [0,1,0], [0,0,1]]
        row_cosines = np.array(iop[:3], dtype=float)
        col_cosines = np.array(iop[3:], dtype=float)
        row_cosines_aligned = any(np.allclose(np.abs(row_cosines), axis, atol=1e-5) for axis in axes)
        col_cosines_aligned = any(np.allclose(np.abs(col_cosines), axis, atol=1e-5) for axis in axes)

        return row_cosines_aligned and col_cosines_aligned


    @staticmethod
    def _extract_slice_from_volume(volume, ipp, iop, pixel_spacing, rows: int, cols: int) -> np.ndarray:
        """Extract a 2D slice from a 3D SimpleITK volume at the physical location of a DICOM slice.

        Uses ExtractImageFilter when the volume is axis-aligned (fast, lossless) and
        ResampleImageFilter for arbitrary orientation acquisitions.

        For the axis-aligned path, the slice index is computed directly.
        The 3rd column of the direction matrix gives the slice axis direction
        in world space. Since the volume is axis-aligned, this direction is parallel to
        exactly one world axis k. The slice index is then:

            k              = argmax(|z_axis_dir|)   # which world axis (0=x,1=y,2=z)
            slice_step     = spacing[2] * z_axis_dir[k]  # signed mm per slice
            slice_index    = round((ipp[k] - origin[k]) / slice_step)

        Args:
            volume: SimpleITK.Image (3D) - the defaced volume
            ipp: ImagePositionPatient (list/tuple of 3 floats)
            iop: ImageOrientationPatient (list/tuple of 6 floats)
            pixel_spacing: PixelSpacing (list/tuple of 2 floats, [row_spacing, col_spacing])
            rows: DICOM Rows
            cols: DICOM Columns

        Returns:
            2D numpy array with the extracted pixel values.
        """
        import SimpleITK

        ipp_arr = np.array(ipp, dtype=float)
        row_cosines = np.array(iop[:3], dtype=float)
        col_cosines = np.array(iop[3:], dtype=float)

        if DefaceService._is_volume_axis_aligned(iop):
            # --- Axis-aligned volume path ---
            # For axis-aligned volumes the slice direction (3rd column of vol_dir) is
            # parallel to exactly one world axis k. The slice index is computed directly
            # as the offset along that axis divided by the signed physical step per slice.
            vol_dir = np.array(volume.GetDirection()).reshape(3, 3)
            vol_spacing = np.array(volume.GetSpacing())
            vol_origin = np.array(volume.GetOrigin())

            z_axis_dir = vol_dir[:, 2]                          # slice direction in world space
            k = int(np.argmax(np.abs(z_axis_dir)))              # dominant world axis (0=x,1=y,2=z)
            slice_step = vol_spacing[2] * z_axis_dir[k]         # signed mm per slice along axis k
            slice_index = int(round((ipp_arr[k] - vol_origin[k]) / slice_step))
            # Clamp to valid range
            slice_index = max(0, min(slice_index, volume.GetSize()[2] - 1))

            extractor = SimpleITK.ExtractImageFilter()
            size = list(volume.GetSize())
            size[2] = 0  # collapse z -> 2D output
            extractor.SetSize(size)
            extractor.SetIndex([0, 0, slice_index])
            slice_2d = extractor.Execute(volume)
            return SimpleITK.GetArrayFromImage(slice_2d)
        else:
            # --- General path: arbitrary orientation ---
            # TODO: Test this path with appropriate data, then remove the warning logged if validated
            # Build a 2D reference image with the exact geometry of the DICOM slice
            # and resample the 3D volume onto it. SimpleITK's ResampleImageFilter
            # handles the full 3D->2D transform internally.
            row_spacing = float(pixel_spacing[0])
            col_spacing = float(pixel_spacing[1])

            # SimpleITK 2D direction is the upper-left 2x2 sub-matrix (row-major)
            direction_2d = (
                row_cosines[0], row_cosines[1],
                col_cosines[0], col_cosines[1]
            )

            slice_ref = SimpleITK.Image([cols, rows], volume.GetPixelID())
            slice_ref.SetSpacing([col_spacing, row_spacing])
            slice_ref.SetOrigin(ipp_arr.tolist())
            slice_ref.SetDirection(direction_2d)

            resampler = SimpleITK.ResampleImageFilter()
            resampler.SetReferenceImage(slice_ref)
            resampler.SetInterpolator(SimpleITK.sitkLinear)
            resampler.SetDefaultPixelValue(0)
            extracted = resampler.Execute(volume)
            return SimpleITK.GetArrayFromImage(extracted)

    def _get_cached_mask_path(self, series: DicomSeries) -> Optional[str]:
        """Return the path of a cached mask for this series, or ``None``.

        Only called for non-primary series.  Because the factory guarantees
        that the primary candidate for each (patient, FrameOfReferenceUID,
        modality) group is processed before the others, a mask is already in
        the database when this method is called for any non-primary series in
        the same group.

        For modalities not listed in ``saveDefaceMasks.primary``, or when no DB is
        configured, returns ``None`` immediately so the caller falls through to
        plain ML processing.

        Args:
            series: The non-primary series about to be defaced.

        Returns:
            Absolute path to the cached NRRD mask file, or ``None``.
        """
        if self.deface_mask_db is None:
            return None

        modality = (series.modality or '').upper()
        if modality not in self.best_modalities:
            return None

        if not series.frame_of_reference_uid:
            return None

        cached = self.deface_mask_db.get_primary_mask(
            series.original_patient_id,
            series.original_patient_name,
            series.original_patient_birthdate,
            series.original_study_uid,
            series.frame_of_reference_uid,
            modality,
        )

        if not cached:
            return None

        # mask_path in the DB is relative to private_folder - reconstruct absolute.
        abs_mask_path = os.path.join(self.private_folder, cached['mask_path'])
        if not os.path.exists(abs_mask_path):
            self.logger.warning(f"Cached deface mask file not found: {abs_mask_path}")
            return None
        return abs_mask_path

    def _persist_mask_to_db(
        self,
        series: DicomSeries,
        mask_image,   # SimpleITK.Image
        source_image,  # SimpleITK.Image
    ) -> None:
        """Save ``mask_image`` as an NRRD file and upsert the database record.

        The mask is stored alongside ``image.nrrd`` in the private mapping
        folder, mirroring the series output path structure::

            <outputPrivateMappingFolder>/<rel_series_path>/deface_mask_<modality>.nrrd

        where ``rel_series_path`` is the path of ``series.output_base_path``
        relative to ``outputDeidentifiedFolder``.  This places the mask in the
        same directory as ``image.nrrd`` (moved there by
        ``ProcessingPipeline._export_nrrd_files``).

        The directory is created if it does not exist.  After writing the file
        the database entry for the (patient, FrameOfReferenceUID, modality)
        combination is inserted or replaced.

        Args:
            series: The series that produced the mask.
            mask_image: SimpleITK mask image (binary label map).
            source_image: The original (pre-defacing) SimpleITK image.  Its
                spacing / origin / direction are stored in the DB so the mask
                can later be resampled onto other series.
        """
        import SimpleITK

        modality = (series.modality or 'UNKNOWN').upper()

        if self.deface_mask_db is None:
            return

        if not self.private_folder:
            self.logger.warning("outputPrivateMappingFolder is not set; cannot persist mask")
            return

        if not series.anonymized_patient_id:
            self.logger.warning("anonymized_patient_id not set on series; cannot persist mask")
            return

        # Mirror the same directory as image.nrrd by computing the series path
        # relative to outputDeidentifiedFolder, then rooting it under the
        # private mapping folder.  This is identical to what
        # ProcessingPipeline._export_nrrd_files does for image.nrrd.
        deidentified_folder = self.config.get('outputDeidentifiedFolder', '')
        if series.output_base_path and deidentified_folder:
            rel_path = os.path.relpath(series.output_base_path, deidentified_folder)
            mask_dir = os.path.join(self.private_folder, rel_path)
        else:
            # Fallback: store under patient folder in private mapping
            self.logger.warning(
                "output_base_path or outputDeidentifiedFolder not available; "
                "falling back to patient-level mask directory"
            )
            mask_dir = os.path.join(self.private_folder, series.anonymized_patient_id)
        os.makedirs(mask_dir, exist_ok=True)

        mask_filename = f"deface_mask_{modality}.nrrd"
        mask_path = os.path.join(mask_dir, mask_filename)

        SimpleITK.WriteImage(mask_image, mask_path)
        self.logger.info(f"Saved deface mask: {mask_path}")

        # Store the path relative to private_folder so the DB is portable
        # (moving the private mapping folder does not break cached entries).
        mask_path_db = os.path.relpath(mask_path, self.private_folder)

        # Collect image geometry for future resampling
        spacing   = list(source_image.GetSpacing())
        origin    = list(source_image.GetOrigin())
        direction = list(source_image.GetDirection())
        for_uid   = series.frame_of_reference_uid or ''

        self.deface_mask_db.upsert_mask(
            patient_id            = series.original_patient_id,
            patient_name          = series.original_patient_name,
            birthdate             = series.original_patient_birthdate,
            study_instance_uid    = series.original_study_uid,
            frame_of_reference_uid= for_uid,
            modality              = modality,
            mask_path             = mask_path_db,
            # spatial_volume_cm3 / min_voxel_size_mm live on DefacePrioritySeries;
            # use getattr so the signature stays DicomSeries without coupling.
            spatial_volume_cm3    = getattr(series, 'spatial_volume_cm3', None) or 0.0,
            min_voxel_size_mm     = getattr(series, 'min_voxel_size_mm',  None) or 0.0,
            spacing               = spacing,
            origin                = origin,
            direction             = direction,
            anonymized_patient_id = series.anonymized_patient_id,
            anonymized_study_uid  = series.anonymized_study_uid,
        )

    def _copy_without_defacing(self, series: DicomSeries) -> Dict[str, Any]:
        """Copy files without defacing when defacing fails or is not applicable.

        Args:
            series: DicomSeries to copy

        Returns:
            dict: Result dictionary with empty NRRD paths
        """
        series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
        self.logger.info(f"Copying files without defacing for series {series_display}")

        organized_files = series.get_organized_files()
        # Note: defaced_base_path already contains the complete UID hierarchy
        defaced_folder = series.defaced_base_path
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
