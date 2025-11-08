"""Pipeline coordinator for managing multiple processing pipeline instances.

This module provides the PipelineCoordinator class which manages multiple
ProcessingPipeline instances, enabling parallel processing of DICOM series
by distributing series across multiple workers.
"""

import os
from typing import Any, Dict, List
from processing_pipeline import ProcessingPipeline
from dicom_series import DicomSeries
from dicom_file import DicomFile


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
    """
    
    def __init__(self, all_series: List[DicomSeries], output_directory: str,
                 config: Dict[str, Any], logger, num_workers: int = 1, 
                 llm_cache=None, patient_uid_db=None, recipe=None):
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
        """
        self.all_series = all_series
        self.output_directory = output_directory
        self.config = config
        self.logger = logger
        self.num_workers = max(1, min(num_workers, len(all_series)))  # Cap at series count
        self.llm_cache = llm_cache  # Shared across all workers
        self.patient_uid_db = patient_uid_db  # Shared across all workers
        self.recipe = recipe  # Shared across all workers
        
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
                llm_cache=self.llm_cache,  # Pass shared LLM cache to all workers
                patient_uid_db=self.patient_uid_db,  # Pass shared patient UID DB to all workers
                recipe=self.recipe  # Pass shared recipe to all workers
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
        """Concatenate all worker export files into final consolidated files.
        
        This method implements streaming concatenation of worker-specific temp files
        into final CSV and Parquet files, enabling memory-efficient processing of
        large datasets.
        
        Args:
            private_folder: Path to private mapping folder containing temp exports
        """
        import glob
        import shutil
        
        self.logger.info("Finalizing exports: Concatenating worker files...")
        
        temp_dir = os.path.join(private_folder, '.temp_worker_exports')
        
        if not os.path.exists(temp_dir):
            self.logger.warning(f"Temp export directory not found: {temp_dir}")
            return
        
        # Concatenate UID mappings (CSV)
        uid_mapping_files = sorted(glob.glob(f'{temp_dir}/worker_*_uid_mappings.csv'))
        if uid_mapping_files:
            final_mappings = os.path.join(private_folder, 'uid_mappings.csv')
            self._concatenate_csv_files(uid_mapping_files, final_mappings)
            self.logger.info(f"Created consolidated UID mappings: {final_mappings}")
        
        # Concatenate metadata (Parquet)
        metadata_files = sorted(glob.glob(f'{temp_dir}/worker_*_metadata.parquet'))
        if metadata_files:
            final_metadata = os.path.join(private_folder, 'metadata.parquet')
            self._concatenate_parquet_files(metadata_files, final_metadata)
            self.logger.info(f"Created consolidated metadata: {final_metadata}")
        
        # Clean up temp directory
        try:
            shutil.rmtree(temp_dir)
            self.logger.debug(f"Removed temp export directory: {temp_dir}")
        except Exception as e:
            self.logger.warning(f"Could not remove temp directory {temp_dir}: {e}")
    
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
                               patient_uid_db=None, recipe=None) -> 'PipelineCoordinator':
        """Factory method to create coordinator from DICOM file list or input folder.
        
        This method scans DICOM files (or discovers them from a folder),
        groups them by SeriesInstanceUID, creates DicomSeries objects,
        and initializes the coordinator with multiple pipeline workers.
        
        Args:
            dicom_files: Either a list of DICOM file paths, a single file path (str),
                        or a directory path to scan for DICOM files
            output_directory: Output directory for processed files
            config: Configuration dictionary
            logger: Logger instance
            num_workers: Number of pipeline workers (default: 1)
            llm_cache: Shared LLM cache instance (thread-safe)
            recipe: DeidRecipe instance for anonymization (shared across workers)
            
        Returns:
            PipelineCoordinator: Initialized coordinator ready to run
        """
        import pydicom
        
        # Handle different input types: folder path, file path, or file list
        if isinstance(dicom_files, str):
            input_path = dicom_files
            logger.info(f"Discovering DICOM files from: {input_path}")
            
            if os.path.isfile(input_path):
                # Single file
                dicom_files = [input_path]
                logger.info("Processing single DICOM file")
            elif os.path.isdir(input_path):
                # Directory - walk and collect all files
                dicom_files = []
                for root, dirs, files in os.walk(input_path):
                    for file in files:
                        dicom_files.append(os.path.join(root, file))
                logger.info(f"Found {len(dicom_files)} files in directory")
            else:
                logger.error(f"Input path does not exist: {input_path}")
                dicom_files = []
        
        logger.info(f"Creating coordinator from {len(dicom_files)} DICOM files")
        
        # Group files by SeriesInstanceUID
        series_groups: Dict[str, List[str]] = {}
        series_metadata: Dict[str, Dict[str, Any]] = {}
        
        for file_path in dicom_files:
            try:
                ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                series_uid = getattr(ds, 'SeriesInstanceUID', 'unknown_series')
                
                if series_uid not in series_groups:
                    series_groups[series_uid] = []
                    
                    # Extract metadata for folder naming
                    series_desc = getattr(ds, 'SeriesDescription', '')
                    series_number = getattr(ds, 'SeriesNumber', '')
                    modality = getattr(ds, 'Modality', '')
                    
                    # Create folder name
                    folder_parts = []
                    if series_number:
                        folder_parts.append(
                            f"{series_number:03d}" if isinstance(series_number, int)
                            else str(series_number)
                        )
                    if modality:
                        folder_parts.append(modality)
                    if series_desc:
                        clean_desc = "".join(
                            c for c in series_desc if c.isalnum() or c in " -_"
                        ).strip()
                        clean_desc = "_".join(clean_desc.split())
                        if clean_desc:
                            folder_parts.append(clean_desc[:30])
                    
                    folder_name = "_".join(folder_parts) if folder_parts else series_uid
                    
                    # Limit folder name length
                    if len(folder_name) > 100:
                        clean_series_uid = "".join(
                            c for c in series_uid if c.isalnum() or c in ".-_"
                        )
                        folder_name = folder_name[:100] + "_" + clean_series_uid[-10:]
                    
                    series_metadata[series_uid] = {
                        'folder_name': folder_name,
                        'series_description': series_desc,
                        'series_number': series_number,
                        'modality': modality
                    }
                
                series_groups[series_uid].append(file_path)
                
            except Exception as e:
                logger.warning(f"Could not read DICOM file {file_path}: {e}")
                # Add to unknown series group
                if 'unknown' not in series_groups:
                    series_groups['unknown'] = []
                    series_metadata['unknown'] = {
                        'folder_name': 'unknown_series',
                        'series_description': None,
                        'series_number': None,
                        'modality': None
                    }
                series_groups['unknown'].append(file_path)
        
        # Create DicomSeries objects
        all_series = []
        for series_uid, files in series_groups.items():
            metadata = series_metadata[series_uid]
            series = DicomSeries(series_uid, metadata['folder_name'])
            
            # Set metadata
            series.series_description = metadata['series_description']
            series.series_number = metadata['series_number']
            series.modality = metadata['modality']
            
            # Create and add DicomFile objects
            files.sort()  # Ensure consistent ordering
            for file_path in files:
                dicom_file = DicomFile(file_path, series_uid)
                series.add_file(dicom_file)
            
            all_series.append(series)
        
        logger.info(f"Created {len(all_series)} series from {len(dicom_files)} files")
        
        # Create and return coordinator
        return cls(all_series, output_directory, config, logger, num_workers, 
                  llm_cache, patient_uid_db, recipe)
