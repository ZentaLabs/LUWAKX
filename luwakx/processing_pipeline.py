"""Processing pipeline for orchestrating DICOM anonymization workflow.

This module provides the ProcessingPipeline class which manages the entire
anonymization workflow, coordinating between multiple DicomSeries objects
and tracking progress through processing stages.
"""

import os
import shutil
from typing import Any, Dict, List, Optional
from dicom_series import DicomSeries
from processing_stage import ProcessingStage
from processing_status import ProcessingStatus
from utils import cleanup_lm_studio_workers


class ProcessingPipeline:
    """Orchestrates the DICOM anonymization pipeline workflow.
    
    This class manages the collection of DicomSeries objects and coordinates
    their processing through multiple stages (organization, defacing, anonymization).
    It replaces the complex file_mappings dictionary structure with clean OOP design.
    
    Attributes:
        series_collection: Dictionary mapping series_uid to DicomSeries objects
        current_stage: Current processing stage in the pipeline
        output_directory: Main output directory for anonymized files
        organized_temp_dir: Temporary directory for organized structure
        defaced_temp_dir: Temporary directory for defaced files
        config: Configuration dictionary
        logger: Logger instance (optional)
        
    See conformance documentation:
    - Pipeline Architecture: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#32-pipeline-architecture
    - Core Classes: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#922-core-classes-and-relationships
    """
    
    def __init__(self, series_subset: List[DicomSeries], output_directory: str,
                 config: Dict[str, Any], logger=None, worker_id: int = 0,
                 llm_cache=None, patient_uid_db=None, recipe=None,
                 deface_mask_db=None):
        """Initialize a ProcessingPipeline instance.
        
        Args:
            series_subset: List of DicomSeries to process (subset of all series)
            output_directory: Main output directory for processed files
            config: Configuration dictionary
            logger: Logger instance for logging (optional)
            worker_id: Unique identifier for this worker instance (default: 0)
            llm_cache: Shared LLM cache instance (thread-safe, read-only for workers)
            patient_uid_db: Shared patient UID database instance (thread-safe)
            recipe: DeidRecipe instance for anonymization (shared across all workers)
            deface_mask_db: Shared DefaceMaskDatabase instance (thread-safe, optional)
        """
        self.worker_id = worker_id
        self.series_collection: Dict[str, DicomSeries] = {}
        self.current_stage = ProcessingStage.INPUT_SCANNING
        
        # Directory paths - isolated per worker
        self.output_directory = output_directory
        self.organized_temp_dir = os.path.join(
            output_directory, f"worker_{worker_id}", "temp_organized_input"
        )
        self.defaced_temp_dir = os.path.join(
            output_directory, f"worker_{worker_id}", "temp_defaced_organized"
        )
        
        # Configuration and logging
        self.config = config
        self.logger = logger
        
        # Shared LLM cache (thread-safe, read-only for workers)
        self.llm_cache = llm_cache
        
        # Shared patient UID database (thread-safe)
        self.patient_uid_db = patient_uid_db
        
        # Shared recipe (read-only for workers)
        self.recipe = recipe

        # Shared deface mask database (thread-safe, optional)
        self.deface_mask_db = deface_mask_db
        
        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        
        # Add series subset to collection
        for series in series_subset:
            self.add_series(series)
        
        # Initialize service instances (isolated per worker)
        # These will be imported and initialized when needed to avoid circular imports
        self._processor = None
        self._deface_service = None
        self._exporter = None
        
        # Direct export to final files (for single-worker sequential processing)
        # Files are written incrementally after each series for immediate availability
        private_folder = config.get('outputPrivateMappingFolder')
        
        self.uid_mappings_file = os.path.join(private_folder, 'uid_mappings.csv')
        self.metadata_file = os.path.join(private_folder, 'metadata.parquet')
    
    @property
    def processor(self):
        """Lazy-load DicomProcessor instance with shared LLM cache and patient UID DB."""
        if self._processor is None:
            from dicom_processor import DicomProcessor
            self._processor = DicomProcessor(
                self.config, 
                self.logger, 
                llm_cache=self.llm_cache,
                patient_uid_db=self.patient_uid_db
            )
        return self._processor
    
    @property
    def deface_service(self):
        """Lazy-load DefaceService instance."""
        if self._deface_service is None:
            from deface_service import DefaceService
            self._deface_service = DefaceService(
                self.config, self.logger, deface_mask_db=self.deface_mask_db
            )
        return self._deface_service
    
    @property
    def exporter(self):
        """Lazy-load MetadataExporter instance."""
        if self._exporter is None:
            from metadata_exporter import MetadataExporter
            self._exporter = MetadataExporter(self.config, self.logger)
        return self._exporter
    
    def add_series(self, series: DicomSeries) -> None:
        """Add a DicomSeries to the pipeline.
        
        Args:
            series: DicomSeries instance to add
            
        Raises:
            ValueError: If series with same UID already exists
        """
        if series.original_series_uid in self.series_collection:
            raise ValueError(f"Series with UID '{series.original_series_uid}' already exists in pipeline")
        
        self.series_collection[series.original_series_uid] = series
        
        # Update base paths for the series (organized and defaced use UID hierarchy)
        series.update_base_paths(
            organized=self.organized_temp_dir,
            defaced=self.defaced_temp_dir
        )
        
        # Note: output_base_path is already set by DicomSeriesFactory during creation
        # with proper collision detection. No need to rebuild it here.
    
    def get_series(self, series_uid: str) -> Optional[DicomSeries]:
        """Get a DicomSeries by its UID.
        
        Args:
            series_uid: SeriesInstanceUID to retrieve
            
        Returns:
            DicomSeries: The series, or None if not found
        """
        return self.series_collection.get(series_uid)
    
    def get_all_series(self) -> List[DicomSeries]:
        """Get list of all DicomSeries in the pipeline.
        
        Returns:
            List[DicomSeries]: All series in the pipeline
        """
        return list(self.series_collection.values())
    
    def run_full_pipeline(self) -> None:
        """Process each series completely through all stages (series-by-series).
        
        This method implements series-by-series processing where each series
        goes through all stages (organize, deface, anonymize) before moving
        to the next series. This approach:
        - Reduces memory footprint
        - Enables better progress tracking
        - Facilitates parallelization
        - Allows independent error handling per series
        
        See conformance documentation:
        https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#32-pipeline-architecture
        """
        if self.logger:
            self.logger.info(
                f"Worker {self.worker_id}: Processing {len(self.series_collection)} series"
            )
        
        completed = 0
        failed = 0
        
        for series_uid, series in self.series_collection.items():
            try:
                if self.logger:
                    # Use output_base_path basename for logging (contains UID hierarchy)
                    series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
                    self.logger.info(
                        f"Worker {self.worker_id}: Processing series "
                        f"{completed + 1}/{len(self.series_collection)}: {series_display}"
                    )
                
                # Process this series through ALL stages
                self._process_single_series(series)
                
                completed += 1
                if self.logger:
                    self.logger.info(f"✓ Completed: {series_display}")
                
            except Exception as e:
                series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
                failed += 1
                series.processing_status = ProcessingStatus.FAILED
                if self.logger:
                    self.logger.error(f"✗ Failed: {series_display}: {e}")
                # Continue with next series
        
        if self.logger:
            self.logger.info(
                f"Worker {self.worker_id} finished: {completed} completed, {failed} failed"
            )
            self.logger.info(f"Worker {self.worker_id}: All results exported incrementally")
        
        # Mark export stage complete (exports happened incrementally during processing)
        self.current_stage = ProcessingStage.EXPORT_METADATA
        
        # Cleanup temp directories
        self.cleanup()
    
    def _process_single_series(self, series: DicomSeries) -> None:
        """Process a single series through all stages.
        
        Args:
            series: DicomSeries to process
        """
        # Stage 1: Organize
        self._organize_series(series)
        
        # Stage 2: Deface (if needed)
        if self._needs_defacing(series):
            self._deface_series(series)
        
        # Stage 3: Anonymize
        self._anonymize_series(series)
        
        # Stage 4: Export results incrementally (streaming mode)
        # This writes results to worker-specific temp files immediately,
        # keeping memory usage constant regardless of dataset size
        self._export_series_results_incremental(series)
        
        # Stage 5: Clear series data from memory after export completes
        # This frees current_file_mappings and self.series reference
        self.processor.clear_series_data(series)
        
        # Clean up LM Studio worker processes after series is complete
        # The LLM inference (recipe generation) creates worker processes that
        # accumulate GPU memory. Clean them up after each series to prevent OOM.
        killed_count = cleanup_lm_studio_workers()
        if killed_count > 0 and self.logger:
            self.logger.debug(f"Cleaned up {killed_count} LM Studio worker process(es)")

    
    def _organize_series(self, series: DicomSeries) -> None:
        """Organize files for a single series.
        
        Args:
            series: DicomSeries to organize
        
        Note:
            organized_base_path is already set by add_series() -> update_base_paths()
            output_base_path is already set by DicomSeriesFactory with collision detection
        """
        # Create organized directory structure (path already set in add_series)
        os.makedirs(series.organized_base_path, exist_ok=True)
        
        # Create final output directory structure (path already set by DicomSeriesFactory)
        os.makedirs(series.output_base_path, exist_ok=True)
        
        # Copy files and update paths
        for dicom_file in series.files:
            organized_path = os.path.join(series.organized_base_path, dicom_file.filename)
            
            try:
                shutil.copy2(dicom_file.original_path, organized_path)
                dicom_file.set_organized_path(organized_path)
            except Exception as e:
                if self.logger:
                    self.logger.warning(
                        f"Could not copy file {dicom_file.original_path}: {e}"
                    )
                dicom_file.update_status(ProcessingStatus.FAILED)
                raise
        
        series.processing_status = ProcessingStatus.ORGANIZED
    
    def _deface_series(self, series: DicomSeries) -> None:
        """Deface a single series.
        
        Args:
            series: DicomSeries to deface
        
        Note:
            defaced_base_path is already set by add_series() -> update_base_paths()
        """
        if self.logger:
            series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
            self.logger.info(f"Defacing {series_display} at {series.defaced_base_path}")
        
        # Create defaced directory structure (path already set in add_series)
        os.makedirs(series.defaced_base_path, exist_ok=True)
        
        # Call deface service
        deface_result = self.deface_service.process_series(series)
        
        # Move NRRD files immediately to final destinations (before cleanup)
        if isinstance(deface_result, dict):
            self._export_nrrd_files(series, deface_result)
        
        series.processing_status = ProcessingStatus.DEFACED
    
    def _export_nrrd_files(self, series: DicomSeries, deface_result: Dict[str, Any]) -> None:
        """Export NRRD files immediately after defacing to prevent cleanup deletion.
        
        This method moves NRRD files from temp locations to their final destinations
        right after defacing completes, before temp directories are cleaned up.
        
        Args:
            series: DicomSeries that was defaced
            deface_result: Result dictionary from DefaceService containing NRRD paths
        """
        nrrd_image_src = deface_result.get('nrrd_image_path')
        nrrd_defaced_src = deface_result.get('nrrd_defaced_path')
        
        if not nrrd_image_src or not nrrd_defaced_src:
            return
        
        if not os.path.exists(nrrd_image_src) or not os.path.exists(nrrd_defaced_src):
            if self.logger:
                series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
                self.logger.warning(
                    f"NRRD files not found for series {series_display}: "
                    f"image={nrrd_image_src}, defaced={nrrd_defaced_src}"
                )
            return
        
        try:
            # Calculate relative path for structure mirroring
            rel_path = os.path.relpath(series.output_base_path, self.output_directory)
            
            # Destination: image.nrrd → private folder with same structure
            private_folder = self.config.get('outputPrivateMappingFolder', '')
            nrrd_image_dst = os.path.join(private_folder, rel_path, "image.nrrd")
            os.makedirs(os.path.dirname(nrrd_image_dst), exist_ok=True)
            
            # Destination: image_defaced.nrrd → public output
            nrrd_defaced_dst = os.path.join(series.output_base_path, "image_defaced.nrrd")
            os.makedirs(os.path.dirname(nrrd_defaced_dst), exist_ok=True)
            
            # Move files
            shutil.move(nrrd_image_src, nrrd_image_dst)
            shutil.move(nrrd_defaced_src, nrrd_defaced_dst)
            
            # Store final paths in metadata for reference
            series.metadata['nrrd_image_path'] = nrrd_image_dst
            series.metadata['nrrd_defaced_path'] = nrrd_defaced_dst
            
            if self.logger:
                self.logger.info(f"Moved NRRD files for series {series.anonymized_series_uid}")
                self.logger.private(f"  image.nrrd → {nrrd_image_dst}")
                self.logger.private(f"  image_defaced.nrrd → {nrrd_defaced_dst}")
        except Exception as e:
            if self.logger:
                series_display = os.path.basename(series.output_base_path) if series.output_base_path else series.original_series_uid
                self.logger.warning(
                    f"Failed to move NRRD files for series {series.anonymized_series_uid} in subfolder {series_display}: {e}"
                )
    
    def _anonymize_series(self, series: DicomSeries) -> None:
        """Anonymize a single series using DicomProcessor.
        
        Args:
            series: DicomSeries to anonymize
        
        Note:
            output_base_path is already set by DicomSeriesFactory with collision detection.
            No need to rebuild it here.
        """
        if self.logger:
            series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
            self.logger.debug(f"Anonymizing {series_display}")
        
        # Call processor with recipe (output_base_path already set by DicomSeriesFactory)
        self.processor.process_series(series, self.recipe)
        
        series.processing_status = ProcessingStatus.ANONYMIZED
    
    def _needs_defacing(self, series: DicomSeries) -> bool:
        """Check if series needs defacing based on modality and config.
        
        Args:
            series: DicomSeries to check
            
        Returns:
            bool: True if defacing is needed
        """
        # Check if defacing is enabled in config
        if 'clean_recognizable_visual_features' not in self.config.get('recipes', []):
            return False
        
        # Check if modality is CT (defacing currently only for CT)
        if series.modality and series.modality.upper() == "CT":
            return True
        
        series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
        self.logger.info(f"Skipping defacing for non-CT modality: {series_display}")
        return False
        
    def process_defacing(self) -> None:
        """Process visual defacing for applicable series.
        
        Note: This method sets up paths for defacing. The actual defacing
        is performed by the anonymizer's clean_recognizable_visual_features method.
        """
        if self.logger:
            self.logger.info("Setting up for visual defacing stage")
        
        # Create defaced temp directory
        os.makedirs(self.defaced_temp_dir, exist_ok=True)
        
        # Calculate defaced paths for all files
        for series in self.series_collection.values():
            series_folder = series.defaced_base_path
            
            for dicom_file in series.files:
                defaced_path = os.path.join(series_folder, dicom_file.filename)
                dicom_file.defaced_path = defaced_path  # Pre-calculate path
        
        self.current_stage = ProcessingStage.VISUAL_DEFACING
    
    def process_anonymization(self) -> None:
        """Process DICOM anonymization for all series.
        
        Note: This method sets up paths for anonymization. The actual anonymization
        is performed by the anonymizer's deid-based processing.
        """
        if self.logger:
            self.logger.info("Setting up for DICOM anonymization stage")
        
        # Calculate anonymized output paths for all files
        for series in self.series_collection.values():
            series_folder = series.output_base_path
            
            for dicom_file in series.files:
                output_path = os.path.join(series_folder, dicom_file.filename)
                dicom_file.anonymized_path = output_path  # Pre-calculate path
        
        self.current_stage = ProcessingStage.DICOM_ANONYMIZATION
    
    def advance_to_stage(self, stage: ProcessingStage) -> None:
        """Manually advance the pipeline to a specific stage.
        
        Args:
            stage: ProcessingStage to advance to
        """
        self.current_stage = stage
        
        if self.logger:
            self.logger.debug(f"Advanced pipeline to stage: {stage}")
    
    def get_files_for_current_stage(self) -> List[str]:
        """Get list of file paths for the current processing stage.
        
        Returns:
            List[str]: File paths appropriate for current stage
        """
        all_files = []
        
        for series in self.series_collection.values():
            for dicom_file in series.files:
                if self.current_stage == ProcessingStage.INPUT_SCANNING:
                    all_files.append(dicom_file.original_path)
                elif self.current_stage == ProcessingStage.SERIES_ORGANIZATION:
                    if dicom_file.organized_path:
                        all_files.append(dicom_file.organized_path)
                elif self.current_stage == ProcessingStage.VISUAL_DEFACING:
                    if dicom_file.defaced_path:
                        all_files.append(dicom_file.defaced_path)
                elif self.current_stage == ProcessingStage.DICOM_ANONYMIZATION:
                    if dicom_file.anonymized_path:
                        all_files.append(dicom_file.anonymized_path)
        
        return all_files
    
    def update_file_paths_for_stage(self, stage: ProcessingStage) -> None:
        """Update file paths for a specific processing stage.
        
        Args:
            stage: ProcessingStage to update paths for
        """
        for series in self.series_collection.values():
            if stage == ProcessingStage.SERIES_ORGANIZATION:
                series.calculate_file_paths_for_stage(
                    series.organized_base_path, 'set_organized_path'
                )
            elif stage == ProcessingStage.VISUAL_DEFACING:
                series.calculate_file_paths_for_stage(
                    series.defaced_base_path, 'set_defaced_path'
                )
            elif stage == ProcessingStage.DICOM_ANONYMIZATION:
                series.calculate_file_paths_for_stage(
                    series.output_base_path, 'set_anonymized_path'
                )
    
    def cleanup(self) -> None:
        """Clean up temporary directories created during processing.
        
        
        If the config option 'keepTempFiles' is set to True, temporary directories
        (temp_organized_input, temp_defaced_organized) are retained after processing
        to allow step-by-step validation of the deidentification pipeline.
        """
        if self.config.get('keepTempFiles', False):
            if self.logger:
                self.logger.info(
                    f"Worker {self.worker_id}: Skipping cleanup of temporary directories "
                    f"(keepTempFiles=True). Temp dirs retained for validation:\n"
                    f"  organized: {self.organized_temp_dir}\n"
                    f"  defaced:   {self.defaced_temp_dir}"
                )
            self.current_stage = ProcessingStage.CLEANUP
            return

        if self.logger:
            self.logger.info(f"Worker {self.worker_id}: Cleaning up temporary directories")
        
        # Remove organized temp directory
        if os.path.exists(self.organized_temp_dir):
            try:
                shutil.rmtree(self.organized_temp_dir)
                if self.logger:
                    self.logger.debug(f"Removed: {self.organized_temp_dir}")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Could not remove {self.organized_temp_dir}: {e}")
        
        # Remove defaced temp directory
        if os.path.exists(self.defaced_temp_dir):
            try:
                shutil.rmtree(self.defaced_temp_dir)
                if self.logger:
                    self.logger.debug(f"Removed: {self.defaced_temp_dir}")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Could not remove {self.defaced_temp_dir}: {e}")
        
        # Remove parent worker directory if empty
        worker_dir = os.path.join(self.output_directory, f"worker_{self.worker_id}")
        if os.path.exists(worker_dir):
            try:
                # Only remove if directory is empty
                if not os.listdir(worker_dir):
                    os.rmdir(worker_dir)
                    if self.logger:
                        self.logger.debug(f"Removed empty worker directory: {worker_dir}")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Could not remove worker directory {worker_dir}: {e}")
        
        self.current_stage = ProcessingStage.CLEANUP
    
    # ============================================================================
    # Streaming Export Methods (Memory-Efficient Incremental Export)
    # ============================================================================
    
    def _export_series_results_incremental(self, series: DicomSeries) -> None:
        """Export results for one series immediately to final files (streaming mode).
        
        This method writes series results directly to final CSV and Parquet files
        immediately after processing, keeping memory usage constant regardless of
        dataset size. Files are available for inspection during processing.
        
        Note: Currently designed for single-worker sequential processing.
        For parallel processing, file locking mechanisms would be required.
        
        Delegates to MetadataExporter for consistent export logic.
        
        Args:
            series: DicomSeries that was just processed
        """
        if self.logger:
            series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
            self.logger.debug(
                f"Worker {self.worker_id}: Exporting results for series {series_display}"
            )
        
        # Get series-specific UID mappings from processor (file-based structure)
        series_mappings = self.processor.get_series_uid_mappings(series)
        
        # Get input and output folders from config for relative path calculation
        input_folder = self.config.get('inputFolder', '')
        output_folder = self.config.get('outputDeidentifiedFolder', self.output_directory)
        
        # Append directly to final CSV file using MetadataExporter
        # Pass series object to use DicomFile relative path methods
        self.exporter.append_series_uid_mappings(
            self.uid_mappings_file,
            series,
            series_mappings,
            input_folder,
            output_folder
        )
        
        # Extract and append metadata from first file of series
        if series.files:
            first_file = series.files[0]
            first_anonymized_path = os.path.join(series.output_base_path, first_file.filename)
            
            # Extract metadata using MetadataExporter
            metadata_dict = self.exporter.extract_dicom_metadata(
                dicom_file=first_file.original_path,
                anonymized_file_path=first_anonymized_path,
                output_folder=self.output_directory,
                private_map_folder=self.config.get('outputPrivateMappingFolder', '')
            )
            
            if metadata_dict:
                # Append directly to final Parquet file (one row per series)
                self.exporter.append_series_metadata(
                    self.metadata_file,
                    [metadata_dict]  # Wrap in list since append expects a list
                )
        
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of processing status for all series.
        
        Returns:
            Dict[str, Any]: Summary statistics and status information
        """
        total_files = sum(s.get_file_count() for s in self.series_collection.values())
        total_series = len(self.series_collection)
        
        # Count files by status
        status_counts = {status: 0 for status in ProcessingStatus}
        for series in self.series_collection.values():
            for dicom_file in series.files:
                status_counts[dicom_file.processing_status] += 1
        
        return {
            'current_stage': str(self.current_stage),
            'total_series': total_series,
            'total_files': total_files,
            'status_breakdown': {str(k): v for k, v in status_counts.items()},
            'output_directory': self.output_directory
        }
