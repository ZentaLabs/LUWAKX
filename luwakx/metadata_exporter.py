"""Metadata exporter service for UID mappings and Parquet export.

This module provides the MetadataExporter class which handles exporting
UID mappings to CSV and DICOM metadata to Parquet format, plus handling
NRRD file movement to final destinations.

Extracted from anonymize.py in Phase 2 refactoring.
"""

import os
import csv
import shutil
import traceback
from typing import Any, Dict, List, Set
import pydicom
import pandas as pd

from dicom_series import DicomSeries
from luwak_logger import log_project_stacktrace


class MetadataExporter:
    """Handles export of UID mappings, DICOM metadata, and NRRD file placement.
    
    This service manages the export of anonymization results to CSV
    (UID mappings) and Parquet (DICOM metadata) formats, as well as moving
    NRRD files to their final destinations.
    
    Attributes:
        config: Configuration dictionary (read-only)
        logger: Logger instance
        excluded_tags_from_parquet: Set of DICOM tag integers to exclude from Parquet export
    """
    
    def __init__(self, config: Dict[str, Any], logger):
        """Initialize MetadataExporter.
        
        Args:
            config: Configuration dictionary
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        
        # Parse excluded tags from Parquet export
        self.excluded_tags_from_parquet = self._parse_excluded_tags(
            config.get('excludedTagsFromParquet', [])
        )
    
    def _parse_excluded_tags(self, excluded_tags: List) -> Set[int]:
        """Parse excluded tags configuration into set of tag integers.
        
        Args:
            excluded_tags: List of tag ints or strings
            
        Returns:
            Set[int]: Set of tag integers to exclude
        """
        result = set()
        for tag in excluded_tags:
            if isinstance(tag, int):
                result.add(tag)
            elif isinstance(tag, str):
                tag_str = tag.strip().strip('()')
                if ',' in tag_str:
                    parts = tag_str.split(',')
                    if len(parts) == 2:
                        try:
                            group = int(parts[0].strip(), 16)
                            elem = int(parts[1].strip(), 16)
                            result.add((group << 16) | elem)
                        except ValueError:
                            pass
                else:
                    try:
                        result.add(int(tag_str, 16))
                    except ValueError:
                        pass
        return result
    
    # ============================================================================
    # Streaming Export Methods (Memory-Efficient Incremental Export)
    # ============================================================================
    
    def append_series_uid_mappings(self, uid_mappings_file: str, 
                                   series: 'DicomSeries',
                                   mappings: Dict[str, Any],
                                   input_folder: str,
                                   output_folder: str) -> None:
        """Append UID mappings for one series to worker's CSV file.
        
        Creates file with headers on first write, then appends data.
        CSV structure:
        - One row per DICOM file
        - Dynamic columns for each UID type found
        - Patient info: PatientName, PatientID, PatientBirthDate
        - UID pairs: {field}_original and {field}_anonymized
        
        Args:
            uid_mappings_file: Path to worker's UID mappings CSV file
            series: DicomSeries object containing file information
            mappings: File-based UID mappings {file_path: {field: {original, anonymized}}}
            input_folder: Input directory from config (for relative paths)
            output_folder: Output directory from config (for relative paths)
        """
        if not mappings:
            return
        
        # Check if file exists to determine if we need headers
        file_exists = os.path.exists(uid_mappings_file)
        
        # Discover all modified fields in this batch
        all_modified_fields = set()
        for file_path, file_mappings in mappings.items():
            all_modified_fields.update(file_mappings.keys())
        
        # Sort for consistent column ordering
        sorted_fields = sorted(all_modified_fields)
        
        # Build CSV structure with dynamic columns
        patient_columns = ['PatientName', 'PatientID_original', 'PatientID_anonymized', 'PatientBirthDate']
        fieldnames = ['original_file_path', 'anonymized_file_path'] + patient_columns
        for field in sorted_fields:
            fieldnames.extend([f'{field}_original', f'{field}_anonymized'])
        
        # Create a map from input paths to DicomFile objects for quick lookup
        # The mappings dict uses the path that was INPUT to anonymization (defaced or organized)
        # not the anonymized output path
        file_map = {}
        for dicom_file in series.files:
            # Get the path that was used as input during anonymization
            if dicom_file.defaced_path:
                input_path = dicom_file.defaced_path
            elif dicom_file.organized_path:
                input_path = dicom_file.organized_path
            else:
                input_path = dicom_file.original_path
            file_map[input_path] = dicom_file
                
        # Open in append mode
        with open(uid_mappings_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            
            # Write header only on first write
            if not file_exists:
                writer.writeheader()
            
            # Write one row per file
            for file_path, file_mappings in mappings.items():
                # Get the DicomFile object for this path (file_path is the input path during anonymization)
                dicom_file = file_map.get(file_path)
                
                if dicom_file:
                    # Use DicomFile methods to get relative paths
                    original_rel_path = dicom_file.get_relative_original_path(input_folder)
                    anonymized_rel_path = dicom_file.get_relative_anonymized_path(output_folder)
                else:
                    # Fallback if file not found (shouldn't happen)
                    self.logger.warning(
                        f"DicomFile not found for mapping key: {file_path}. "
                        f"Available keys: {list(file_map.keys())[:3]}"
                    )
                    original_rel_path = os.path.basename(file_path)
                    anonymized_rel_path = os.path.basename(file_path)
                
                row = {
                    'original_file_path': original_rel_path,
                    'anonymized_file_path': anonymized_rel_path
                }
                
                # Try to extract patient info from original file
                try:
                    row['PatientName'] = series.original_patient_name
                    row['PatientID_original'] = series.original_patient_id
                    row['PatientID_anonymized'] = series.anonymized_patient_id
                    row['PatientBirthDate'] = series.original_patient_birthdate
                except Exception as e:
                    self.logger.warning(f"Could not read patient info from {file_path}: {e}")
                
                # Add UID mappings
                for field, mapping in file_mappings.items():
                    row[f'{field}_original'] = mapping.get('original', '')
                    row[f'{field}_anonymized'] = mapping.get('anonymized', '')
                
                writer.writerow(row)
    
    def append_series_metadata(self, metadata_file: str, 
                              metadata: List[Dict[str, Any]]) -> None:
        """Append metadata for one series to worker's Parquet file.
        
        Args:
            metadata_file: Path to worker's metadata Parquet file
            metadata: List of metadata dictionaries for one series
        """
        if not metadata:
            return
        
        # Convert to DataFrame and ensure all columns are strings to avoid dtype conflicts
        # This prevents issues with mixed types (e.g., single float vs multi-value string)
        df_new = pd.DataFrame(metadata).astype(str)
        
        # Append to Parquet file (or create if first write)
        if os.path.exists(metadata_file):
            # Read existing data and concatenate
            df_existing = pd.read_parquet(metadata_file, engine='pyarrow')
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_parquet(
                metadata_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
        else:
            # Create new file
            df_new.to_parquet(
                metadata_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )

    
    def _move_nrrd_files(self, all_series: List[DicomSeries],
                        output_directory: str,
                        private_mapping_folder: str) -> None:
        """Move NRRD files from temp locations to final destinations.
        
        - image.nrrd → private_folder/series_structure/ (identifiable)
        - image_defaced.nrrd → output_directory/series_structure/ (anonymized)
        
        Args:
            all_series: List of all processed DicomSeries
            output_directory: Public output directory
            private_mapping_folder: Private mapping folder
        """
        self.logger.info("Moving NRRD files to final destinations...")
        
        nrrd_count = 0
        for series in all_series:
            if 'nrrd_image_path' not in series.metadata:
                continue
            
            nrrd_image_src = series.metadata.get('nrrd_image_path')
            nrrd_defaced_src = series.metadata.get('nrrd_defaced_path')
            
            if not nrrd_image_src or not nrrd_defaced_src:
                continue
            
            if not os.path.exists(nrrd_image_src) or not os.path.exists(nrrd_defaced_src):
                self.logger.debug(
                    f"NRRD files already moved or not created for series {series.original_series_uid}"
                )
                continue
            
            try:
                # Get series output path structure
                series_output_path = series.output_base_path
                if not series_output_path:
                    self.logger.warning(f"No output path for series {series.original_series_uid}")
                    continue
                
                # Calculate relative path for structure mirroring
                rel_path = os.path.relpath(series_output_path, output_directory)
                
                # Destination: image.nrrd → private folder with same structure
                nrrd_image_dst = os.path.join(private_mapping_folder, rel_path, "image.nrrd")
                os.makedirs(os.path.dirname(nrrd_image_dst), exist_ok=True)
                
                # Destination: image_defaced.nrrd → public output
                # Note: series_output_path directory already created in organize stage
                nrrd_defaced_dst = os.path.join(series_output_path, "image_defaced.nrrd")
                
                # Move files
                shutil.move(nrrd_image_src, nrrd_image_dst)
                shutil.move(nrrd_defaced_src, nrrd_defaced_dst)
                
                series_display = f"series:{series.anonymized_series_uid}, of study:{series.anonymized_study_uid}, for patient:{series.anonymized_patient_id}"
                self.logger.info(f"Moved NRRD files for series {series_display}")
                self.logger.private(f"  image.nrrd: {nrrd_image_dst}")
                self.logger.private(f"  image_defaced.nrrd: {nrrd_defaced_dst}")
                
                nrrd_count += 1
                
            except Exception as e:
                tb = traceback.extract_tb(e.__traceback__)
                log_project_stacktrace(self.logger, e)
                self.logger.error(f"Failed to move NRRD files for series {series.original_series_uid}: {e}")
        
        if nrrd_count > 0:
            self.logger.info(f"Moved {nrrd_count} remaining NRRD file(s)")
        else:
            self.logger.debug(
                "No remaining NRRD files to move "
                "(files were moved during processing or defacing was not performed)"
            )
    
    def extract_dicom_metadata(self, dicom_file: str, anonymized_file_path: str,
                               output_folder: str, private_map_folder: str) -> Dict[str, Any]:
        """Extract metadata from anonymized DICOM file for Parquet export.
        
        Args:
            dicom_file: Path to original DICOM file (for reference/logging)
            anonymized_file_path: Path to anonymized DICOM file to extract from
            output_folder: Output directory for relative path calculation (outputDeidentifiedFolder)
            private_map_folder: Private mapping folder for relative path calculation (outputPrivateMappingFolder)
            
        Returns:
            Dict[str, Any]: Metadata dictionary with all retained DICOM tags
        """
        try:
            self.logger.debug(f"Extracting metadata from: {anonymized_file_path}")
            
            # Read the anonymized DICOM file
            ds = pydicom.dcmread(anonymized_file_path, force=True)
            
            # Compute paths relative to outputDeidentifiedFolder
            try:
                # AnonymizedFilePath: DICOM file path relative to outputDeidentifiedFolder
                # E.g., "series_Subfolder/file.dcm"
                anonymized_rel_path = os.path.relpath(anonymized_file_path, output_folder)
                anonymized_rel_path = anonymized_rel_path.replace(os.sep, '/')
                
                # Get the series folder name from the anonymized path
                series_folder = os.path.dirname(anonymized_rel_path)
                
                # DefacedNiftiPath: image_defaced.nrrd in output folder, relative to outputDeidentifiedFolder
                # E.g., "series_Subfolder/image_defaced.nrrd"
                defaced_nifti_path = os.path.join(series_folder, 'image_defaced.nrrd')
                defaced_nifti_path = defaced_nifti_path.replace(os.sep, '/')
                
                # OriginalNiftiPath: image.nrrd in private folder, but path relative to outputDeidentifiedFolder
                # E.g., "../privateMapping/series_Subfolder/image.nrrd"
                # The full path is: private_map_folder/series_folder/image.nrrd
                original_nifti_full_path = os.path.join(private_map_folder, series_folder, 'image.nrrd')
                original_nifti_path = os.path.relpath(original_nifti_full_path, output_folder)
                original_nifti_path = original_nifti_path.replace(os.sep, '/')
                
            except Exception as e:
                self.logger.warning(f"Could not compute relative paths: {e}")
                anonymized_rel_path = os.path.basename(anonymized_file_path)
                original_nifti_path = ''
                defaced_nifti_path = ''
            
            metadata = {
                'AnonymizedFilePath': anonymized_rel_path,
                'OriginalNiftiPath': original_nifti_path,
                'DefacedNiftiPath': defaced_nifti_path,
            }
            
            # Extract all retained DICOM tags
            for elem in ds:
                tag_int = int(elem.tag)
                
                # Skip file meta information, pixel data, and excluded tags
                if elem.tag.group == 0x0002:
                    continue
                if tag_int == 0x7FE00010:
                    continue
                if tag_int in self.excluded_tags_from_parquet:
                    continue
                
                # Handle private tags
                if elem.is_private and elem.private_creator:
                    try:
                        private_creator = elem.private_creator.replace(' ', '_')
                        if elem.name and elem.name != "Unknown":
                            keyword = f'{private_creator}_{elem.name[1:-1]}'
                        else:
                            keyword = f'{private_creator}_{elem.tag.group:04X}xx{elem.tag.element & 0xFF:02X}'
                    except Exception as e:
                        self.logger.warning(f"Skipping private tag ({elem.tag}): {e}")
                        continue
                else:
                    keyword = elem.keyword
                
                # Extract value based on VR
                try:
                    value = self._extract_dicom_value(elem)
                    metadata[keyword] = value
                    self.logger.private(f"Extracted element {keyword} ({elem.tag}): {value}")
                except Exception as e:
                    self.logger.warning(f"Skipping element {keyword} ({elem.tag}): {e}")
                    continue
            
            return metadata
            
        except Exception as e:
            self.logger.warning(f"Could not extract metadata from {dicom_file}: {e}")
            return {}
    
    def _extract_dicom_value(self, elem) -> Any:
        """Extract value from DICOM element based on VR type.
        
        Args:
            elem: PyDicom data element
            
        Returns:
            Appropriate Python type for the value
        """
        # String types
        if elem.VR in ['PN', 'DA', 'TM', 'DT', 'UI', 'SH', 'LO', 'ST', 'LT', 'UT', 'AE', 'CS', 'AS']:
            if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                return str(list(elem.value)) if elem.value else ''
            return str(elem.value) if elem.value else ''
        
        # Integer String
        elif elem.VR == 'IS':
            try:
                if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                    # Multi-value integer field - convert to string list
                    return str(list(elem.value)) if elem.value else ''
                else:
                    return int(elem.value) if elem.value else 0
            except (ValueError, TypeError):
                return str(elem.value) if elem.value else ''
        
        # Decimal String
        elif elem.VR == 'DS':
            try:
                if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                    # Multi-value decimal field - convert to string list
                    return str(list(elem.value)) if elem.value else ''
                else:
                    return float(elem.value) if elem.value else 0.0
            except (ValueError, TypeError):
                return str(elem.value) if elem.value else ''
        
        # Numeric types
        elif elem.VR in ['US', 'SS', 'UL', 'SL']:
            try:
                if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                    # Multi-value field - convert to string list
                    return str(list(elem.value)) if elem.value else ''
                else:
                    return int(elem.value) if elem.value is not None else 0
            except (ValueError, TypeError):
                return str(elem.value) if elem.value else ''
        
        # Float types
        elif elem.VR in ['FL', 'FD']:
            try:
                if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                    # Multi-value field - convert to string list
                    return str(list(elem.value)) if elem.value else ''
                else:
                    return float(elem.value) if elem.value is not None else 0.0
            except (ValueError, TypeError):
                return str(elem.value) if elem.value else ''
        
        # Sequence - skip
        elif elem.VR == 'SQ':
            return ''
        
        # Default to string
        else:
            if hasattr(elem.value, '__iter__') and not isinstance(elem.value, (str, bytes)):
                return str(list(elem.value)) if elem.value else ''
            return str(elem.value) if elem.value is not None else ''
    
    def export_metadata_to_parquet(self, metadata_list: List[Dict[str, Any]],
                                   private_map_folder: str) -> str:
        """Export all collected metadata to Parquet file.
        
        Args:
            metadata_list: List of metadata dictionaries
            private_map_folder: Private folder to save Parquet file
            
        Returns:
            str: Path to created Parquet file, or None if export failed
        """
        try:
            if not metadata_list:
                self.logger.info("No metadata to export")
                return None
            
            # Create DataFrame
            df = pd.DataFrame(metadata_list)
            
            self.logger.debug(f"Dynamic Parquet schema detected {len(df.columns)} columns")
            
            # Optimize data types
            for col in df.columns:
                try:
                    # Skip if column is empty
                    sample_values = [v for v in df[col].dropna().head(100) if v != '']
                    if not sample_values:
                        continue
                    
                    # Check if numeric
                    if all(isinstance(v, (int, float)) for v in sample_values):
                        if all(isinstance(v, int) for v in sample_values):
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        else:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Convert DICOM dates
                    elif col.endswith('Date'):
                        try:
                            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
                        except:
                            df[col] = df[col].astype(str)
                    else:
                        df[col] = df[col].astype(str)
                        
                except Exception as e:
                    tb = traceback.extract_tb(e.__traceback__)
                    log_project_stacktrace(self.logger, e)
                    continue
            
            # Export to Parquet
            parquet_file = os.path.join(private_map_folder, "metadata.parquet")
            df.to_parquet(
                parquet_file,
                engine='pyarrow',
                compression='snappy',
                index=False,
                row_group_size=10000,
                use_dictionary=True
            )
            
            self.logger.info(f"Metadata exported to Parquet: {parquet_file}")
            self.logger.info(f"Exported {len(df)} DICOM metadata records with {len(df.columns)} columns")
            
            return parquet_file
            
        except ImportError:
            self.logger.warning("pandas and pyarrow required for Parquet export")
            return None
        except Exception as e:
            self.logger.error(f"Error exporting metadata to Parquet: {e}")
            return None
