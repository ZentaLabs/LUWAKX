import unittest
import os
import shutil
import pydicom
import json
import tempfile
import sys
import tarfile
import urllib.request
import csv
import pandas as pd

# Add luwakx directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
from anonymize import LuwakAnonymizer
from luwak_logger import setup_logger, get_logger

class TestExports(unittest.TestCase):
    """Test suite for CSV mapping and Parquet export functionality."""

    @classmethod
    def setUpClass(cls):
        # Create a temporary output directory
        cls.test_output_dir = "test_output_exports"

        # Path to the decompressed test data directory
        cls.test_data_dir = "test_data"

        # Check if the test data directory exists
        if not os.path.exists(cls.test_data_dir):

            # URL of the test data archive
            test_data_url = "https://github.com/Simlomb/Test-data-anonymization/releases/download/0.0.1-dicom-files-test/test-dicom-files-2.tar.gz"

            # Download the archive
            archive_path = "test-dicom-files-2.tar.gz"
            urllib.request.urlretrieve(test_data_url, archive_path)

            # Extract the archive with data filter for security
            with tarfile.open(archive_path, "r:gz") as tar:
                # Extract all files directly into the test_data_dir
                for member in tar.getmembers():
                    # Remove the top-level folder from the path
                    member.path = os.path.relpath(member.path, start="test-dicom-files-2")
                    tar.extract(member, path=cls.test_data_dir, filter='tar')

            # Clean up the downloaded archive
            os.remove(archive_path)

    @classmethod
    def tearDownClass(cls):
        try:
            # Perform cleanup of the test_data_dir
            if os.path.exists(cls.test_data_dir):
                shutil.rmtree(cls.test_data_dir)
        finally:
            pass

    def setUp(self):
        # Ensure the output directory is clean before each test
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        os.makedirs(self.test_output_dir, exist_ok=True)

        # Create a limited input directory with first 10 files for faster testing
        self.limited_input_dir = "test_input_exports"
        self.create_limited_input_dataset()
        
        print("\n######################START EXPORT TEST######################")
    
    def create_limited_input_dataset(self):
        """Create a dataset with only the first 10 DICOM files for testing."""
        if os.path.exists(self.limited_input_dir):
            shutil.rmtree(self.limited_input_dir)
        os.makedirs(self.limited_input_dir, exist_ok=True)
        
        # Get all DICOM files from test_data_dir and sort them
        all_files = [f for f in os.listdir(self.test_data_dir) if f.endswith('.dcm')]
        all_files.sort()

        # Take only first 2 files for faster export testing
        files_to_copy = all_files[:20]

        # Copy the first 2 files to the limited input directory
        for file in files_to_copy:
            src = os.path.join(self.test_data_dir, file)
            dst = os.path.join(self.limited_input_dir, file)
            shutil.copy2(src, dst)

    def tearDown(self):
        # Clean up output directory after each test
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        # Clean up limited input directory
        if os.path.exists(self.limited_input_dir):
            shutil.rmtree(self.limited_input_dir)
        
        self.logger.info("Export test case completed and cleaned up")
        print("\n######################END EXPORT TEST######################")

    def create_test_config(self, input_folder, output_folder, recipes=None, recipes_folder=None):
        """Helper method to create a temporary config file for testing."""
        if recipes is None:
            recipes = ""
        # Convert relative paths to absolute paths for the config
        if not os.path.isabs(input_folder):
            input_folder = os.path.abspath(input_folder)
        if not os.path.isabs(output_folder):
            output_folder = os.path.abspath(output_folder)
        # Recipes folder is always output_folder/recipe/
        recipes_folder = os.path.join(output_folder, "recipe")
        os.makedirs(recipes_folder, exist_ok=True)
        # Output mapping folder
        output_private_mapping_folder = os.path.join(output_folder, "private")
        
        # Setup logger with the actual output and recipe paths
        log_file_path = os.path.join(output_folder, 'luwak_test_exports.log')
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        setup_logger(log_level='INFO', log_file=log_file_path, console_output=False)
        self.logger = get_logger('test_exports')
        self.logger.info(f"Setting up test exports configuration with output: {output_folder}, recipes: {recipes_folder}")
        
        # Fill in all config keys
        config = {
            "inputFolder": input_folder,
            "outputDeidentifiedFolder": output_folder,
            "outputPrivateMappingFolder": output_private_mapping_folder,
            "recipesFolder": recipes_folder,
            "recipes": recipes,
        }
        # Create temporary config file
        config_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, config_file, indent=2)
        config_file.close()
        
        self.logger.info(f"Created test exports config file: {config_file.name}")
        return config_file.name

    def test_uid_mapping_file_creation(self):
        """Test that the UID mapping CSV file is created correctly with proper format."""
        print("Test UID mapping CSV file creation")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        # Read original file to get the original UIDs
        original_ds = pydicom.dcmread(original_file)
        original_uids = {
            'StudyInstanceUID': getattr(original_ds, 'StudyInstanceUID', None),
            'SeriesInstanceUID': getattr(original_ds, 'SeriesInstanceUID', None),
            'SOPInstanceUID': getattr(original_ds, 'SOPInstanceUID', None)
        }

        # Create test config with basic profile to trigger UID generation
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile"]
        )

        try:
            self.logger.info("Starting UID mapping CSV file creation test")
            self.logger.info(f"Original UIDs to be mapped: {original_uids}")
            
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()
            
            # Check that the mapping file was created - use absolute path to match what anonymizer uses
            mapping_file = os.path.join(os.path.abspath(self.test_output_dir), "private", "uid_mappings.csv")
            
            # List all files in the privateMapping directory
            private_mapping_dir = os.path.join(os.path.abspath(self.test_output_dir), "private")
            if os.path.exists(private_mapping_dir):
                files_in_dir = os.listdir(private_mapping_dir)
                self.logger.info(f"Files created in private directory: {files_in_dir}")
            else:
                self.logger.warning("Private directory doesn't exist")
            
            self.assertTrue(os.path.exists(mapping_file), "UID mapping CSV file was not created")
            self.logger.info(f"✓ UID mapping CSV file created at: {mapping_file}")

            # Read and verify the mapping file content
            with open(mapping_file, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                
                # Should have exactly one row for our single file
                self.assertEqual(len(rows), 1, f"Expected 1 row in mapping file, got {len(rows)}")
                self.logger.info(f"✓ CSV contains {len(rows)} row(s) as expected")
                
                row = rows[0]
                
                # Check that required columns exist
                required_columns = [
                    'anonymized_file_path',
                    'StudyInstanceUID_original', 'StudyInstanceUID_anonymized',
                    'SeriesInstanceUID_original', 'SeriesInstanceUID_anonymized', 
                    'SOPInstanceUID_original', 'SOPInstanceUID_anonymized'
                ]
                
                for column in required_columns:
                    self.assertIn(column, row, f"Required column '{column}' missing from mapping file")
                self.logger.info(f"✓ All required columns present: {required_columns}")
                
                # Verify file path
                self.assertEqual(row['anonymized_file_path'], '00000001.dcm', "File path in mapping is incorrect")
                self.logger.info(f"✓ File path verified: {row['anonymized_file_path']}")

                # Verify original UIDs match what we read from the file
                for uid_name in ['StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID']:
                    original_uid = original_uids[uid_name]
                    mapped_original = row[f'{uid_name}_original']
                    mapped_anonymized = row[f'{uid_name}_anonymized']
                    
                    if original_uid:  # Only check if the original file had this UID
                        self.assertEqual(original_uid, mapped_original,
                                       f"Original {uid_name} doesn't match in mapping file")
                        self.assertIsNotNone(mapped_anonymized,
                                           f"Anonymized {uid_name} is missing in mapping file")
                        self.assertNotEqual(original_uid, mapped_anonymized,
                                          f"Anonymized {uid_name} should be different from original")
                        self.logger.info(f"✓ {uid_name}: {original_uid} → {mapped_anonymized}")
                
                self.logger.info("UID mapping CSV file creation test completed successfully")
        
        finally:
            # Clean up config file
            os.unlink(config_path)
            self.logger.info("Test completed and config cleaned up")

    def test_parquet_metadata_export(self):
        """Test that the Parquet metadata file is created correctly with proper format."""
        print("Test Parquet metadata export")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        # Read original file to get some expected metadata
        original_ds = pydicom.dcmread(original_file)
        
        # Create test config with basic profile to trigger anonymization
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile"]
        )

        try:
            self.logger.info("Starting Parquet metadata export test")
            
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Check that the Parquet file was created
            parquet_file = os.path.join(os.path.abspath(self.test_output_dir), "private", "metadata.parquet")
            self.assertTrue(os.path.exists(parquet_file), "Parquet metadata file was not created")
            self.logger.info(f"✓ Parquet metadata file created at: {parquet_file}")

            # Read and verify the Parquet file content
            try:
                df = pd.read_parquet(parquet_file)
                
                # Should have exactly one row for our single file
                self.assertEqual(len(df), 1, f"Expected 1 row in Parquet file, got {len(df)}")
                self.logger.info(f"✓ Parquet file contains {len(df)} row(s) as expected")
                
                # Check that essential tracking columns exist
                required_columns = ['AnonymizedFilePath']
                for column in required_columns:
                    self.assertIn(column, df.columns, f"Required column '{column}' missing from Parquet file")
                self.logger.info(f"✓ Required columns present: {required_columns}")
                
                # Verify file paths
                row = df.iloc[0]
                self.assertEqual(row['AnonymizedFilePath'], '00000001.dcm', "Anonymized file path in Parquet is incorrect")
                self.logger.info(f"✓ File path verified: {row['AnonymizedFilePath']}")
                
                # Check that some DICOM metadata columns exist (after anonymization)
                # Note: Some fields may be removed/anonymized, so we check for commonly retained ones
                
                # Check for some basic DICOM fields that should survive anonymization
                basic_dicom_columns = ['SpecificCharacterSet', 'ImageType', 'SOPClassUID']
                found_columns = [col for col in basic_dicom_columns if col in df.columns]
                
                # We should have at least some DICOM metadata columns beyond just AnonymizedFilePath
                self.assertGreater(len(df.columns), 1, 
                                 "Parquet should contain DICOM metadata beyond just file paths")
                self.logger.info(f"✓ Parquet contains {len(df.columns)} total columns")
                
                # Check that we have some actual DICOM tags (not just our tracking columns)
                non_tracking_columns = [col for col in df.columns if col != 'AnonymizedFilePath']
                self.assertGreater(len(non_tracking_columns), 0,
                                 "Parquet should contain actual DICOM metadata columns")
                
                self.logger.info(f"✓ Found {len(non_tracking_columns)} DICOM metadata columns: {non_tracking_columns[:5]}...")  # Show first 5
                self.logger.info("Parquet metadata export test completed successfully")
                
            except ImportError:
                self.logger.error("pandas is required to read Parquet files for testing")
                self.fail("pandas is required to read Parquet files for testing. Please install pandas.")
            except Exception as e:
                self.logger.error(f"Failed to read or validate Parquet file: {e}")
                self.fail(f"Failed to read or validate Parquet file: {e}")
                
        finally:
            # Clean up config file
            os.unlink(config_path)
            self.logger.info("Test completed and config cleaned up")

    def test_parquet_metadata_export_multiple_files(self):
        """Test that the Parquet metadata file is created correctly for multiple input files."""
        print("Test Parquet metadata export with multiple files")
        
        # Use the existing limited_input_dir which already has multiple files (up to 2)
        # This avoids unnecessary file copying and directory creation
        
        # Create test config with basic profile to trigger anonymization
        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "retain_safe_private_tags"]
        )

        try:
            self.logger.info("Starting Parquet metadata export test with multiple files")
            
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Check that the Parquet file was created
            parquet_file = os.path.join(os.path.abspath(self.test_output_dir), "private", "metadata.parquet")
            self.assertTrue(os.path.exists(parquet_file), "Parquet metadata file was not created")
            self.logger.info(f"✓ Parquet metadata file created at: {parquet_file}")

            # Count expected files in input directory
            input_files = [f for f in os.listdir(self.limited_input_dir) if f.endswith('.dcm')]
            # Only one metadata row per series is expected now
            expected_count = 1  # Assume all test files are from the same series
            self.logger.info(f"Processing {len(input_files)} DICOM files from input directory, expecting {expected_count} metadata row(s)")

            # Read and verify the Parquet file content
            try:
                df = pd.read_parquet(parquet_file)
                
                # Should have one row per series (not per file)
                self.assertEqual(len(df), expected_count, 
                               f"Expected {expected_count} row(s) in Parquet file (one per series), got {len(df)}")
                self.logger.info(f"✓ Parquet file contains {len(df)} row(s) as expected (one per series)")
 
                # Check that essential tracking columns exist
                required_columns = ['AnonymizedFilePath']
                for column in required_columns:
                    self.assertIn(column, df.columns, f"Required column '{column}' missing from Parquet file")
                self.logger.info(f"✓ Required columns present: {required_columns}")
                
                # Verify that we have the expected number of unique file paths (one per series)
                unique_paths = df['AnonymizedFilePath'].nunique()
                self.assertEqual(unique_paths, expected_count,
                               f"Expected {expected_count} unique file path(s), got {unique_paths}")
                self.logger.info(f"✓ Found {unique_paths} unique file path(s)")
                
                # Check that we have DICOM metadata columns beyond just file paths
                non_tracking_columns = [col for col in df.columns if col != 'AnonymizedFilePath']
                self.assertGreater(len(non_tracking_columns), 0,
                                 "Parquet should contain actual DICOM metadata columns")
                
                self.logger.info(f"✓ Found {len(non_tracking_columns)} DICOM metadata columns")
                self.logger.info(f"Sample columns: {non_tracking_columns[:5]}...")
                
                # Verify that each row has data (not all null values)
                for idx, row in df.iterrows():
                    non_null_count = row.notna().sum()
                    self.assertGreater(non_null_count, 1,  # At least AnonymizedFilePath + some DICOM data
                                     f"Row {idx} should have more than just file path data")
                self.logger.info(f"✓ All {len(df)} rows contain meaningful metadata")
                
                # Test with at least 2 files to verify different files can have different metadata
                if len(df) > 1:
                    row1_data = df.iloc[0].to_dict()
                    row2_data = df.iloc[1].to_dict()
                    
                    # Files should have different AnonymizedFilePath
                    self.assertNotEqual(row1_data['AnonymizedFilePath'], row2_data['AnonymizedFilePath'],
                                      "Different files should have different AnonymizedFilePath values")
                    
                    self.logger.info(f"✓ File 1: {row1_data['AnonymizedFilePath']}")
                    self.logger.info(f"✓ File 2: {row2_data['AnonymizedFilePath']}")
                
                self.logger.info("Parquet metadata export test with multiple files completed successfully")
                
            except ImportError:
                self.logger.error("pandas is required to read Parquet files for testing")
                self.fail("pandas is required to read Parquet files for testing. Please install pandas.")
            except Exception as e:
                self.logger.error(f"Failed to read or validate Parquet file: {e}")
                self.fail(f"Failed to read or validate Parquet file: {e}")
                
        finally:
            # Clean up config file
            os.unlink(config_path)
            self.logger.info("Test completed and config cleaned up")

    def test_csv_and_parquet_consistency(self):
        """Test that CSV mapping and Parquet export are consistent with each other."""
        print("Test CSV and Parquet export consistency")
        
        # Use a few files for testing consistency
        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["basic_profile"]
        )

        try:
            self.logger.info("Starting CSV and Parquet consistency test")
            
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Check that both files were created
            mapping_file = os.path.join(os.path.abspath(self.test_output_dir), "private", "uid_mappings.csv")
            parquet_file = os.path.join(os.path.abspath(self.test_output_dir), "private", "metadata.parquet")
            
            self.assertTrue(os.path.exists(mapping_file), "CSV mapping file was not created")
            self.assertTrue(os.path.exists(parquet_file), "Parquet metadata file was not created")
            self.logger.info(f"✓ Both CSV and Parquet files created successfully")

            # Read both files
            with open(mapping_file, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                csv_rows = list(reader)
            
            df = pd.read_parquet(parquet_file)
            
            # Should have same number of series processed (not files)
            self.assertEqual(len(csv_rows), 20, 
                           f"CSV has {len(csv_rows)} rows but should be 20 rows (should match series count)")
            self.assertEqual(len(df), 1, 
                           f"Parquet has {len(df)} rows but should be 1 row (should have 1 row per series)")

            self.logger.info(f"✓ Both files contain the right amount of lines per series")
            
            # Check that file names are consistent
            csv_files = set(row['anonymized_file_path'] for row in csv_rows)
            parquet_files = set(df['AnonymizedFilePath'].tolist())

            self.assertTrue(parquet_files.issubset(csv_files),
                f"Parquet file names should be a subset of CSV file names: CSV={csv_files}, Parquet={parquet_files}")
            
            self.logger.info(f"✓ File names consistent between formats: {csv_files}")
            self.logger.info(f"Successfully verified consistency between CSV ({len(csv_rows)} files) and Parquet ({len(df)} files)")
            self.logger.info("CSV and Parquet consistency test completed successfully")
                
        finally:
            # Clean up config file
            os.unlink(config_path)
            self.logger.info("Test completed and config cleaned up")

    def test_logger_file_created_and_filled(self):
        """Test that the logger file is created and contains log output in the expected directory."""
        # Setup minimal config and logger
        input_file = os.path.join(self.test_data_dir, "00000001.dcm")
        output_folder = self.test_output_dir
        config_path = self.create_test_config(
            input_folder=input_file,
            output_folder=output_folder,
            recipes=["basic_profile"]
        )
        # The logger file path as set in create_test_config
        log_file_path = os.path.join(self.test_output_dir, 'luwak_test_exports.log')
        try:
            # Run a minimal anonymization to trigger logging
            anonymizer = LuwakAnonymizer(config_path)
            anonymizer.anonymize()
            # Check that the log file exists
            self.assertTrue(os.path.exists(log_file_path), f"Logger file should exist at {log_file_path}")
            # Check that the log file is not empty
            with open(log_file_path, 'r') as logf:
                log_contents = logf.read().strip()
            self.assertTrue(len(log_contents) > 0, "Logger file should not be empty after export test")
        finally:
            # Clean up config file and log file
            if os.path.exists(config_path):
                os.unlink(config_path)
            if os.path.exists(log_file_path):
                os.remove(log_file_path)


if __name__ == "__main__":
    unittest.main()
