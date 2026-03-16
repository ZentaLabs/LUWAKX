"""Pipeline coordinator for managing multiple processing pipeline instances.

This module provides the PipelineCoordinator class which manages multiple
ProcessingPipeline instances, enabling parallel processing of DICOM series
by distributing series across multiple workers.
"""

import os
from typing import Any, Dict, List
from processing_pipeline import ProcessingPipeline
from dicom_series import DicomSeries
from dicom_series_factory import DicomSeriesFactory
from deface_priority_elector import DefacePriorityElector


class PipelineCoordinator:
    """Coordinates multiple ProcessingPipeline instances for parallel processing.
    
    This class manages the distribution of DICOM series across multiple pipeline
    workers, handles result aggregation, and provides both sequential and parallel
    execution strategies.
    
    Attributes:
        all_series: Complete list of all DicomSeries to process
        pipelines: List of ProcessingPipeline worker instances
        config: Configuration dictionary (shared read-only)
        logger: Logger instance (shared thread-safe)
        output_directory: Main output directory for all processed files
        num_workers: Number of pipeline workers to create
        
    See conformance documentation:
    - Pipeline Architecture: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#32-pipeline-architecture
    - Core Classes: https://github.com/ZentaLabs/luwak/blob/conformance-document-creation/docs/deidentification_conformance.md#922-core-classes-and-relationships
    """
    
    def __init__(self, all_series: List[DicomSeries], output_directory: str,
                 config: Dict[str, Any], logger, num_workers: int = 1, 
                 llm_cache=None, patient_uid_db=None, recipe=None,
                 deface_mask_db=None):
        """Initialize PipelineCoordinator.
        
        Args:
            all_series: List of all DicomSeries to process
            output_directory: Main output directory for processed files
            config: Configuration dictionary
            logger: Logger instance
            num_workers: Number of pipeline workers (default: 1)
            llm_cache: Shared LLM cache instance (thread-safe, read-only for workers)
            patient_uid_db: Shared patient UID database instance (thread-safe)
            recipe: DeidRecipe instance for anonymization (shared across all workers)
            deface_mask_db: Shared DefaceMaskDatabase instance (thread-safe, optional)
        """
        self.all_series = all_series
        self.output_directory = output_directory
        self.config = config
        self.logger = logger
        self.num_workers = max(1, min(num_workers, len(all_series)))  # Cap at series count
        self.llm_cache = llm_cache          # Shared across all workers
        self.patient_uid_db = patient_uid_db  # Shared across all workers
        self.recipe = recipe                  # Shared across all workers
        self.deface_mask_db = deface_mask_db  # Shared across all workers
        
        self.pipelines: List[ProcessingPipeline] = []
        
        # Create pipelines with partitioned series
        self._create_pipelines()
        
        self.logger.info(f"Coordinator created with {self.num_workers} workers for {len(all_series)} series")
    
    def _create_pipelines(self) -> None:
        """Create pipeline instances with partitioned series subsets."""
        # Partition series across workers
        partitions = self._partition_series(self.num_workers)
        
        # Create a pipeline for each partition
        for worker_id, series_subset in enumerate(partitions):
            if not series_subset:  # Skip empty partitions
                continue
            
            pipeline = ProcessingPipeline(
                series_subset=series_subset,
                output_directory=self.output_directory,
                config=self.config,
                logger=self.logger,
                worker_id=worker_id,
                llm_cache=self.llm_cache,
                patient_uid_db=self.patient_uid_db,
                recipe=self.recipe,
                deface_mask_db=self.deface_mask_db,
            )
            
            self.pipelines.append(pipeline)
            
            self.logger.debug(f"Created worker {worker_id} with {len(series_subset)} series")
    
    def _partition_series(self, num_partitions: int) -> List[List[DicomSeries]]:
        """Partition series into balanced groups by file count.
        
        Uses a greedy algorithm to distribute series across partitions,
        minimizing workload imbalance by assigning each series to the
        partition with the smallest current total file count.
        
        Args:
            num_partitions: Number of partitions to create
            
        Returns:
            List of series lists, one per partition
        """
        # Sort series by file count (descending) for better load balancing
        sorted_series = sorted(
            self.all_series,
            key=lambda s: s.get_file_count(),
            reverse=True
        )
        
        # Initialize partitions and their sizes
        partitions: List[List[DicomSeries]] = [[] for _ in range(num_partitions)]
        partition_sizes = [0] * num_partitions
        
        # Greedy assignment: assign each series to least-loaded partition
        for series in sorted_series:
            # Find partition with smallest total file count
            min_idx = partition_sizes.index(min(partition_sizes))
            
            # Assign series to this partition
            partitions[min_idx].append(series)
            partition_sizes[min_idx] += series.get_file_count()
        
        # Log partition statistics
        for i, (partition, size) in enumerate(zip(partitions, partition_sizes)):
            if partition:
                self.logger.debug(
                    f"Partition {i}: {len(partition)} series, {size} files"
                )
        
        return partitions
    
    def run_all_pipelines_sequential(self) -> None:
        """Run all pipelines sequentially (one after another).
        
        This method processes each pipeline worker sequentially, which is
        useful for testing and debugging before enabling parallelization.
        """
        self.logger.info(f"Starting sequential execution of {len(self.pipelines)} pipelines")
        
        for idx, pipeline in enumerate(self.pipelines):
            self.logger.info(f"Running pipeline {idx + 1}/{len(self.pipelines)}")
            
            try:
                pipeline.run_full_pipeline()
                self.logger.info(f"Pipeline {idx + 1} completed successfully")
            except Exception as e:
                self.logger.error(f"Pipeline {idx + 1} failed: {e}")
                # Continue with other pipelines
        
        self.logger.info("All pipelines completed")
        
    def aggregate_results(self) -> Dict[str, Any]:
        """Aggregate results from all pipeline workers.
        
        Returns:
            Dictionary containing aggregated statistics and results
        """
        total_series = sum(len(p.series_collection) for p in self.pipelines)
        total_files = sum(
            sum(s.get_file_count() for s in p.series_collection.values())
            for p in self.pipelines
        )
        
        # Count by status
        from processing_status import ProcessingStatus
        status_counts = {status: 0 for status in ProcessingStatus}
        
        for pipeline in self.pipelines:
            for series in pipeline.series_collection.values():
                status_counts[series.processing_status] += 1
        
        return {
            'num_workers': len(self.pipelines),
            'total_series': total_series,
            'total_files': total_files,
            'status_breakdown': {str(k): v for k, v in status_counts.items()},
            'output_directory': self.output_directory
        }
    
    def finalize_exports(self, private_folder: str) -> None:
        """Finalize exports after processing completes.
        
        With direct file writing (single-worker mode), exports are already
        complete and no concatenation is needed. This method is kept for
        compatibility and logs completion.
        
        Args:
            private_folder: Path to private mapping folder containing exports
        """
        self.logger.info("Export finalization: Files already written directly during processing")
        
        # Verify final files exist
        uid_mappings_path = os.path.join(private_folder, 'uid_mappings.csv')
        metadata_path = os.path.join(private_folder, 'metadata.parquet')
        
        if os.path.exists(uid_mappings_path):
            self.logger.info(f"(SUCCESS) UID mappings available: {uid_mappings_path}")
        else:
            self.logger.warning(f"(ERROR) UID mappings file not found: {uid_mappings_path}")
        
        if os.path.exists(metadata_path):
            self.logger.info(f"(SUCCESS) Metadata available: {metadata_path}")
        else:
            self.logger.warning(f"(ERROR) Metadata file not found: {metadata_path}")
    
    def _concatenate_csv_files(self, input_files: List[str], output_file: str) -> None:
        """Concatenate multiple CSV files into one (streaming).
        
        Args:
            input_files: List of CSV file paths to concatenate
            output_file: Path to output consolidated CSV file
        """
        if not input_files:
            return
        
        self.logger.debug(f"Concatenating {len(input_files)} CSV files...")
        
        with open(output_file, 'w') as outfile:
            for i, fname in enumerate(input_files):
                with open(fname, 'r') as infile:
                    if i == 0:
                        # Include header from first file
                        outfile.write(infile.read())
                    else:
                        # Skip header line from subsequent files
                        next(infile)  # skip header
                        outfile.write(infile.read())
    
    def _concatenate_parquet_files(self, input_files: List[str], output_file: str) -> None:
        """Concatenate multiple Parquet files into one (streaming).
        
        Uses pyarrow for efficient concatenation without loading all data into memory.
        
        Args:
            input_files: List of Parquet file paths to concatenate
            output_file: Path to output consolidated Parquet file
        """
        if not input_files:
            return
        
        self.logger.debug(f"Concatenating {len(input_files)} Parquet files...")
        
        try:
            import pyarrow.parquet as pq
            import pyarrow as pa
            
            # Read all tables
            tables = []
            for fname in input_files:
                table = pq.read_table(fname)
                tables.append(table)
            
            # Concatenate tables
            combined_table = pa.concat_tables(tables)
            
            # Write to output file
            pq.write_table(
                combined_table,
                output_file,
                compression='snappy'
            )
            
        except Exception as e:
            self.logger.error(f"Failed to concatenate Parquet files: {e}")
            # Fallback: copy first file if concatenation fails
            if input_files:
                import shutil
                shutil.copy2(input_files[0], output_file)
                self.logger.warning(f"Copied first Parquet file as fallback: {input_files[0]}")
    
    @classmethod
    def create_from_dicom_files(cls, dicom_files, output_directory: str,
                               config: Dict[str, Any], logger,
                               num_workers: int = 1, llm_cache=None, 
                               patient_uid_db=None, recipe=None,
                               deface_mask_db=None) -> 'PipelineCoordinator':
        """Factory method to create coordinator from DICOM file list or input folder.
        
        Uses DicomSeriesFactory to create DicomSeries objects, then initializes
        the coordinator with multiple pipeline workers.
        
        Args:
            dicom_files: Either a list of DICOM file paths, a single file path (str),
                        or a directory path to scan for DICOM files
            output_directory: Output directory for processed files
            config: Configuration dictionary
            logger: Logger instance
            num_workers: Number of pipeline workers (default: 1)
            llm_cache: Shared LLM cache instance (thread-safe)
            patient_uid_db: Patient UID database for anonymization
            recipe: DeidRecipe instance for anonymization (shared across workers)
            deface_mask_db: Shared DefaceMaskDatabase instance (thread-safe, optional)
            
        Returns:
            PipelineCoordinator: Initialized coordinator ready to run
        """
        # Create factory and generate series from files
        factory = DicomSeriesFactory(
            patient_uid_db=patient_uid_db,
            config=config,
            logger=logger,
            output_directory=output_directory
        )
        
        # Factory handles file discovery, reading, grouping, and series creation
        all_series = factory.create_series_from_files(dicom_files)

        # Elect primary deface candidates and reorder so each primary precedes
        # its group members - required for PET/CT mask projection to work correctly.
        # The elector always runs when the deface recipe is active; CT is the
        # hardcoded primary modality (no user config required).
        if 'clean_recognizable_visual_features' in config.get('recipes', []):
            elector = DefacePriorityElector(
                best_modalities=['CT'],
                logger=logger,
                deface_mask_db=deface_mask_db,
            )
            all_series = elector.elect_and_sort(all_series)

        # Create and return coordinator with created series
        return cls(all_series, output_directory, config, logger, num_workers, 
                  llm_cache, patient_uid_db, recipe, deface_mask_db)
