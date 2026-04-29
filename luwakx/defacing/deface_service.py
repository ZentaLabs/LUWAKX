"""Deface service for visual feature processing.

This module provides the DefaceService class which handles removal of
recognizable visual features (faces) from medical images using AI models.

"""

import gc
import os
import shutil
import traceback
import importlib.util
from typing import Any, Dict, Optional, Tuple

import numpy as np

from ..dicom.dicom_series import DicomSeries
from ..utils import cleanup_gpu_memory
from ..logging.luwak_logger import log_project_stacktrace


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

        # When saveDefaceMasks is true every series that runs ML inference gets its
        # mask saved to the private folder and the DB persists after the run,
        # enabling full re-run cache hits.  When false (default) only primary CT
        # candidates (those serving a paired PET) persist their mask - just long
        # enough to project it onto the PET within the same run.
        self.save_all_masks: bool = bool(config.get('saveDefaceMasks', False))

        # Private mapping folder is used to store persisted mask NRRD files
        self.private_folder: str = config.get('outputPrivateMappingFolder', '')

        # Config directory used to format log paths as relative paths
        self._config_dir: str = config.get('configDir', '')

        # Configuration for defacing strategy
        self.external_mask_paths = config.get('testOptions', {}).get('useExistingMaskDefacer', [])
        if self.external_mask_paths:
            self.external_mask_paths = [os.path.abspath(m) for m in self.external_mask_paths]

        # Physical block size for pixelation (in mm)
        self.physical_block_size_mm = config.get('physicalFacePixelationSizeMm', 8.5)

        # Dilation margin applied to the face mask before pixelation (in mm)
        self.face_dilation_margin_mm = config.get('faceDilationMarginMm', 15.0)

        # Track series counter for external mask indexing
        self._series_counter = 0

        # Cached defacer module - loaded once on the first series to avoid
        # re-executing module-level moosez initialisation code on every series.
        self._defacer = None

    def _rel_path(self, path: str) -> str:
        """Return *path* relative to the config file directory for log messages."""
        if self._config_dir and path:
            try:
                return os.path.relpath(path, self._config_dir)
            except ValueError:
                pass  # On Windows, relpath raises ValueError across drives
        return path

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
        https://github.com/ZentaLabs/LUWAKX/blob/conformance-document-creation/docs/deidentification_conformance.md#41-clean-recognizable-visual-features-defacing----pipeline-stage-3
        """
        import pydicom

        try:
            import SimpleITK
        except ImportError:
            self.logger.error("SimpleITK is required for defacing but not installed")
            raise

        series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
        self.logger.info(f"DefaceService: Defacing {series_display}")

        # Get organized files for this series
        organized_files = series.get_organized_files()
        if not organized_files:
            self.logger.warning(f"No organized files found for series {series.anonymized_series_uid}")
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
                    os.path.dirname(os.path.dirname(__file__)), "scripts", "defacing",
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

        # Initialise to None so the finally block can release them unconditionally,
        # regardless of which early-return path was taken.
        image = image_face_segmentation = image_defaced = None

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

        # reader is no longer needed - image holds the decoded volume
        del reader

        # Apply face detection/segmentation strategy
        #
        # Strategies (highest priority first):
        #   1. Test-time external mask  (testOptions.useExistingMaskDefacer)
        #   2. PET paired with a CT     -> project cached CT mask, no ML
        #   3. All other series:
        #      3a. Cached mask in DB    -> reuse from a previous run, no ML
        #      3b. ML inference         -> run model; save if primary CT or mode='all'
        save_mask_after_ml = False

        try:
            if self.external_mask_paths:
                # Strategy 1: Use pre-computed external mask (test / override)
                self.logger.info("Using external mask for face segmentation")
                mask_path = self.external_mask_paths[self._series_counter]
                image_face_segmentation = defacer.prepare_face_mask(image, modality, mask_path, dilation_margin_mm=self.face_dilation_margin_mm)
                self._series_counter += 1

            elif series.primary_ct_series is not None:
                # Strategy 2: PET paired with a CT primary.
                # Retrieve the CT face mask from the database, resample it onto this
                # series' geometry (same FrameOfReferenceUID guarantees spatial
                # co-registration), and apply pixelation directly - no ML inference.
                ct_mask_path = self._get_ct_mask_for_pet(series)
                if ct_mask_path is None:
                    self.logger.warning(
                        f"CT face mask not yet available for secondary series "
                        f"{series.anonymized_series_uid!r} "
                        f"(primary CT: {series.primary_ct_series.anonymized_series_uid!r}); "
                        f"falling back to copy without defacing."
                    )
                    return self._copy_without_defacing(series)

                self.logger.info(
                    f"Projecting CT face mask onto secondary series "
                    f"(modality={modality}): {self._rel_path(ct_mask_path)}"
                )
                # SimpleITK reads compressed NRRD automatically.
                ct_mask_image = SimpleITK.ReadImage(ct_mask_path)

                # Resample CT mask onto target series geometry using nearest-neighbour
                # interpolation (mask is binary - continuous interpolation would blur edges).
                resampler = SimpleITK.ResampleImageFilter()
                resampler.SetReferenceImage(image)
                resampler.SetInterpolator(SimpleITK.sitkNearestNeighbor)
                resampler.SetDefaultPixelValue(0)
                image_face_segmentation = resampler.Execute(ct_mask_image)

                if self.save_all_masks:
                    try:
                        self._persist_mask_to_db(series, image_face_segmentation, image)
                    except Exception as e:
                        log_project_stacktrace(self.logger, e)
                        self.logger.warning(f"Failed to persist resampled PET face mask to database: {e}")

            else:
                # Strategy 3: CT-only or standalone modality.
                # 3a: Check DB cache - reuse a mask saved during a *previous run of
                #     this exact series*.  Cache hit is only valid when the stored
                #     ct_series_instance_uid matches this series' UID, guaranteeing
                #     identical geometry (no resampling needed).  Any other cached
                #     entry (different source series) is ignored and
                #     ML inference runs fresh.
                cached_mask_path = self._get_cached_mask_path(series)
                if cached_mask_path is not None:
                    # Exact per-series hit: geometry is guaranteed identical.
                    self.logger.info(
                        f"Reusing cached face mask for series "
                        f"{series.anonymized_series_uid!r}: {self._rel_path(cached_mask_path)}"
                    )
                    image_face_segmentation = SimpleITK.ReadImage(cached_mask_path)

                if cached_mask_path is None:
                    # 3b: Run ML inference.
                    self.logger.info(
                        f"Running ML defacing for series {series.anonymized_series_uid!r} "
                        f"(modality={modality})"
                    )
                    image_face_segmentation = defacer.prepare_face_mask(image, modality, dilation_margin_mm=self.face_dilation_margin_mm)
                    cleanup_gpu_memory()
                    self.logger.debug("GPU memory cleaned up after face detection")
                    # Persist mask if this CT serves a paired PET, or saveDefaceMasks=true.
                    if series.is_primary_deface_candidate or self.save_all_masks:
                        save_mask_after_ml = True

        except RuntimeError as e:
            # RuntimeError is raised by prepare_face_mask when the ML model
            # returns no segmentation or an all-zero mask.  This is expected
            # for series that do not contain a face/head region (e.g. chest CT).
            # Copy without defacing and flag for manual review.
            self.logger.warning(
                f"No face detected in series {series.anonymized_series_uid!r} "
                f"({e}). Series copied without defacing please verify manually "
                f"that it contains no facial features."
            )
            return self._copy_without_defacing(series)
        except Exception as e:
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
            SimpleITK.WriteImage(image, nrrd_image_path, useCompression=True)
            SimpleITK.WriteImage(image_defaced, nrrd_defaced_path, useCompression=True)
            self.logger.debug(f"Saved NRRD volumes to {self._rel_path(series_temp_dir)}")
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
            for file_idx, original_file_path in enumerate(gdcm_sorted_files):
                ds = pydicom.dcmread(original_file_path)

                # Extract the defaced 2D slice that corresponds to this DICOM file's
                # physical position/orientation.
                # We pass file_idx as the explicit z-index because ITK reads slices in
                # exactly the same order as gdcm_sorted_files.  Using the loop index
                # avoids index-computation errors that occur with non-uniform slice
                # spacing (ITK warning "Non uniform sampling or missing slices"), where
                # the physical-position-to-index formula using vol_spacing[2] (the
                # average spacing) maps some slices to wrong indices.
                ipp = [float(x) for x in ds.ImagePositionPatient]
                iop = [float(x) for x in ds.ImageOrientationPatient]
                pixel_spacing = [float(x) for x in ds.PixelSpacing]
                slice_2d = self._extract_slice_from_volume(
                    image_defaced, ipp, iop, pixel_spacing, ds.Rows, ds.Columns,
                    slice_z_index=file_idx,
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
                    # OB is the correct VR for encapsulated/compressed pixel data, but
                    # once we decompress and switch to Explicit VR Little Endian the DICOM
                    # standard (PS3.5 sec 8.2) requires OW when BitsAllocated > 8.  pydicom
                    # does not update the DataElement VR automatically, so we do it here.
                    pixel_data_tag = pydicom.tag.Tag(0x7FE0, 0x0010)
                    if pixel_data_tag in ds and getattr(ds, 'BitsAllocated', 0) > 8:
                        ds[pixel_data_tag].VR = 'OW'

                # Save defaced DICOM file
                defaced_file_path = os.path.join(series_temp_dir, os.path.basename(original_file_path))
                ds.save_as(defaced_file_path)
                defaced_dicom_files.append(defaced_file_path)

                # Update DicomFile object with defaced path using direct mapping
                dicom_file = organized_to_dicom_file.get(original_file_path)
                if dicom_file:
                    dicom_file.set_defaced_path(defaced_file_path)

            # image_defaced is no longer needed: the slice loop has finished writing
            # all DICOM files and the verify step reads from disk, not from this object.
            del image_defaced
            gc.collect()

            # Optionally verify that only face voxels were modified, reading the
            # written DICOM files back from disk (mirrors the test_defacer_service check)
            if self.config.get('verifyDefacingIntegrity', False):
                self._verify_non_face_pixels_unchanged(
                    image, series_temp_dir, series.original_series_uid,
                    image_face_segmentation, series
                )

            # image and image_face_segmentation are no longer needed after verify.
            del image
            del image_face_segmentation
            gc.collect()

        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            log_project_stacktrace(self.logger, e)
            self.logger.error(f"Failed to convert defaced volume to DICOM files: {e}")
            self.logger.error("Defacing failed - falling back to undefaced files")
            return self._copy_without_defacing(series)
        finally:
            # Release any large ITK volumes that were not yet freed on the
            # happy path (e.g. when an exception cut the try block short).
            # Assigning None is safe regardless of whether the name was already
            # del-ed inside the try block or was never assigned at all, because
            # all three were initialised to None before the try.
            image = image_defaced = image_face_segmentation = None
            gc.collect()

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
    def _extract_slice_from_volume(volume, ipp, iop, pixel_spacing, rows: int, cols: int, slice_z_index: int) -> np.ndarray:
        """Extract a 2D slice from a 3D SimpleITK volume by its loop index.

        The correct and universal approach is to treat ``slice_z_index`` (the
        loop index over ``gdcm_sorted_files``) as the ITK z-index, because ITK
        always builds the 3D volume by stacking ``gdcm_sorted_files[i]`` at
        internal z-index ``i`` - regardless of whether the acquisition is
        axis-aligned, oblique/tilted, or has non-uniform slice spacing.

        For tilted/oblique series specifically: ITK still stacks slices along its
        internal z-axis in file order regardless of the physical orientation. The
        volume's direction matrix (3rd column) records the oblique slice normal,
        but the internal storage index is invariant to this. ``ExtractImageFilter``
        at z=``slice_z_index`` performs a direct memory copy with no arithmetic,
        which is exact for any orientation.

        Args:
            volume:         SimpleITK.Image (3D) - the defaced volume built by ITK
                            from ``gdcm_sorted_files``.
            ipp:            ImagePositionPatient (not used for extraction; kept for
                            API symmetry with the DICOM metadata).
            iop:            ImageOrientationPatient (not used for extraction; kept
                            for API symmetry).
            pixel_spacing:  PixelSpacing (not used; kept for API symmetry).
            rows:           DICOM Rows (not used; kept for API symmetry).
            cols:           DICOM Columns (not used; kept for API symmetry).
            slice_z_index:  The 0-based index of the source file in the
                            ``gdcm_sorted_files`` list, equal to the ITK z-index.

        Returns:
            2D numpy array with the exact pixel values at that z-slice.
        """
        import SimpleITK

        # Clamp defensively in case of edge conditions (e.g. a gap slice).
        slice_index = max(0, min(slice_z_index, volume.GetSize()[2] - 1))

        extractor = SimpleITK.ExtractImageFilter()
        size = list(volume.GetSize())
        size[2] = 0  # collapse z dimension -> 2D output
        extractor.SetSize(size)
        extractor.SetIndex([0, 0, slice_index])
        return SimpleITK.GetArrayFromImage(extractor.Execute(volume))

    def _get_ct_mask_for_pet(self, series: DicomSeries) -> Optional[str]:
        """Return the absolute path to the cached CT face mask for a secondary series.

        Queries ``deface_series_pairing`` for the row whose ``pet_series_uid``
        matches this series.  The row is written by
        :class:`DefacePriorityElector` before processing starts, and
        ``mask_path`` is filled in by :meth:`_persist_mask_to_db` once the CT
        mask has been computed.

        Args:
            series: The secondary series (e.g. PET) whose CT primary mask is needed.

        Returns:
            Absolute path to the NRRD mask file, or ``None`` when the pairing
            row is absent, mask_path is still NULL, or the file no longer exists.
        """
        if self.deface_mask_db is None:
            return None

        if series.primary_ct_series is None:
            return None

        pairing = self.deface_mask_db.get_pairing(
            study_instance_uid     = series.original_study_uid,
            frame_of_reference_uid = series.frame_of_reference_uid or '',
            pet_series_uid         = series.original_series_uid,
        )

        if not pairing or not pairing.get('mask_path'):
            return None

        abs_mask_path = os.path.join(self.private_folder, pairing['mask_path'])
        if not os.path.exists(abs_mask_path):
            self.logger.warning(
                f"Cached CT face mask file not found on disk: {abs_mask_path}"
            )
            return None

        return abs_mask_path

    def _get_cached_mask_path(self, series: DicomSeries) -> Optional[str]:
        """Return the absolute path of a previously-saved mask for this series, or ``None``.

        Looks up ``deface_mask_cache`` by
        (cache_key, modality, ct_series_instance_uid) - an exact per-series match.
        Returns ``None`` when no DB is configured, no matching entry exists, or
        the file is missing on disk.

        Args:
            series: The series about to be defaced.

        Returns:
            Absolute path string, or ``None``.
        """
        if self.deface_mask_db is None:
            return None

        modality = (series.modality or '').upper()
        cached = self.deface_mask_db.get_primary_mask(
            series.original_patient_id,
            series.original_patient_name,
            series.original_patient_birthdate,
            series.original_study_uid,
            series.frame_of_reference_uid or '',
            modality,
            ct_series_instance_uid=series.original_series_uid,
        )

        if not cached:
            return None

        # mask_path in the DB is relative to private_folder - reconstruct absolute.
        abs_mask_path = os.path.join(self.private_folder, cached['mask_path'])
        if not os.path.exists(abs_mask_path):
            self.logger.warning(f"Cached deface mask file not found on disk: {self._rel_path(abs_mask_path)}")
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

        SimpleITK.WriteImage(mask_image, mask_path, useCompression=True)

        # Store the path relative to private_folder so the DB is portable
        # (moving the private mapping folder does not break cached entries).
        mask_path_db = os.path.relpath(mask_path, self.private_folder)
        self.logger.info(f"Saved deface mask: {mask_path_db}")

        # Collect image geometry for future resampling
        spacing   = list(source_image.GetSpacing())
        origin    = list(source_image.GetOrigin())
        direction = list(source_image.GetDirection())
        for_uid   = series.frame_of_reference_uid or ''

        # Write one row to deface_mask_cache (keyed on patient/study/FOR/modality).
        self.deface_mask_db.upsert_mask(
            patient_id             = series.original_patient_id,
            patient_name           = series.original_patient_name,
            birthdate              = series.original_patient_birthdate,
            study_instance_uid     = series.original_study_uid,
            frame_of_reference_uid = for_uid,
            modality               = modality,
            mask_path              = mask_path_db,
            spacing                = spacing,
            origin                 = origin,
            direction              = direction,
            anonymized_patient_id  = series.anonymized_patient_id,
            anonymized_study_uid   = series.anonymized_study_uid,
            ct_series_instance_uid = series.original_series_uid,
        )

        # Update deface_series_pairing rows for every PET paired with this CT.
        # The pairing rows were written by DefacePriorityElector before
        # processing started; we now fill in mask_path + mask_written_at.
        pairings = self.deface_mask_db.get_pairings_for_ct(
            study_instance_uid     = series.original_study_uid,
            frame_of_reference_uid = for_uid,
            ct_series_uid          = series.original_series_uid,
        )
        for pairing in pairings:
            self.deface_mask_db.update_pairing_mask_path(
                study_instance_uid     = series.original_study_uid,
                frame_of_reference_uid = for_uid,
                pet_series_uid         = pairing['pet_series_uid'],
                mask_path              = mask_path_db,
            )
        if pairings:
            self.logger.private(
                f"Updated mask_path in {len(pairings)} pairing row(s) for "
                f"CT {series.original_series_uid!r}"
            )

    def _verify_non_face_pixels_unchanged(
        self,
        original_image,
        defaced_dicom_dir: str,
        original_series_uid: str,
        face_mask,
        series: DicomSeries,
    ) -> bool:
        """Verify that defacing only modified face voxels.

        Reads the defaced DICOM series back from disk and compares it against
        the original in-memory volume.  The incoming ``face_mask`` is already
        dilated by ``dilation_margin_mm`` (see ``prepare_face_mask``).  This
        method applies an additional dilation by the pixelation block size to
        account for block-boundary effects, then checks that every voxel
        outside the dilated mask is unchanged.
        Results are logged at INFO level on success and WARNING level on failure.

        Activated by setting ``verifyDefacingIntegrity: true`` in the config.

        Args:
            original_image:     SimpleITK.Image before defacing (in-memory).
            defaced_dicom_dir:  Directory containing the written defaced DICOM files.
            original_series_uid: SeriesInstanceUID used by the GDCM reader to sort files.
            face_mask:          SimpleITK binary label image (face region = 1).
            series:             DicomSeries being processed (used for log messages).

        Returns:
            True if all non-face voxels are unchanged, False otherwise.
        """
        import SimpleITK

        series_display = (
            f"series:{series.anonymized_series_uid}, "
            f"study:{series.anonymized_study_uid}"
        )
        try:
            original_arr = SimpleITK.GetArrayFromImage(original_image)

            # Read the written DICOM files back from disk (mirrors the test check).
            # Pass original_series_uid so GDCM sorts the files the same way as the
            # original read, ensuring slice order matches original_arr exactly.
            defaced_reader = SimpleITK.ImageSeriesReader()
            defaced_files = defaced_reader.GetGDCMSeriesFileNames(defaced_dicom_dir, original_series_uid)
            defaced_reader.SetFileNames(defaced_files)
            defaced_arr = SimpleITK.GetArrayFromImage(defaced_reader.Execute())

            face_margin_mm  = self.face_dilation_margin_mm
            block_size_mm   = self.physical_block_size_mm
            min_spacing     = min(original_image.GetSpacing())
            dilation_radius = int(np.ceil(block_size_mm / min_spacing))

            dilate_filter = SimpleITK.BinaryDilateImageFilter()
            dilate_filter.SetKernelRadius(dilation_radius)
            dilate_filter.SetForegroundValue(1)
            face_mask_dilated = dilate_filter.Execute(
                SimpleITK.Cast(face_mask, SimpleITK.sitkUInt8)
            )
            face_mask_arr = SimpleITK.GetArrayFromImage(face_mask_dilated).astype(bool)

            if original_arr.shape != defaced_arr.shape:
                self.logger.warning(
                    f"DefacingIntegrityCheck: volume shapes differ "
                    f"(original={original_arr.shape}, defaced={defaced_arr.shape}) "
                    f"for {series_display} - cannot verify integrity"
                )
                return False

            if original_arr.shape != face_mask_arr.shape:
                self.logger.warning(
                    f"DefacingIntegrityCheck: face mask shape {face_mask_arr.shape} "
                    f"does not match volume shape {original_arr.shape} "
                    f"for {series_display} - cannot verify integrity"
                )
                return False

            nonface_mask = ~face_mask_arr
            unchanged    = np.isclose(
                original_arr[nonface_mask], defaced_arr[nonface_mask], atol=1e-4
            )
            n_changed = int(np.sum(~unchanged))

            if n_changed > 0:
                self.logger.warning(
                    f"DefacingIntegrityCheck FAILED for {series_display}: "
                    f"{n_changed} non-face voxels were modified outside the dilated "
                    f"face mask (face_margin={face_margin_mm}mm + "
                    f"block={block_size_mm}mm, "
                    f"block_dilation_radius={dilation_radius} voxels, "
                    f"spacing={min_spacing:.2f}mm)"
                )
                return False

            self.logger.info(
                f"DefacingIntegrityCheck passed for {series_display}: "
                f"all non-face voxels unchanged "
                f"(face_margin={face_margin_mm}mm + "
                f"block={block_size_mm}mm, "
                f"block_dilation_radius={dilation_radius} voxels, "
                f"spacing={min_spacing:.2f}mm)"
            )
            return True

        except Exception as e:
            log_project_stacktrace(self.logger, e)
            self.logger.warning(
                f"DefacingIntegrityCheck raised an exception for {series_display}: {e}"
            )
            return False

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
