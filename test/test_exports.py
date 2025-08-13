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
            print("Test data directory not found. Downloading and extracting test data...")

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
            print(f"Test data extracted to {cls.test_data_dir}")

    @classmethod
    def tearDownClass(cls):
        try:
            # Perform cleanup of the test_data_dir
            if os.path.exists(cls.test_data_dir):
                shutil.rmtree(cls.test_data_dir)
                print("Test data directory cleaned up.")
        finally:
            print("tearDownClass executed, ensuring cleanup.")

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

        # Take only first 10 files for faster export testing
        files_to_copy = all_files[:10]

        print(f"Creating limited input dataset with {len(files_to_copy)} files out of {len(all_files)} total files")

        # Copy the first 10 files to the limited input directory
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
        print("\n######################END EXPORT TEST######################")

    def create_test_config(self, input_folder, output_folder, recipes=None, encryption_root=None, recipes_folder=None):
        """Helper method to create a temporary config file for testing."""
        if recipes is None:
            recipes = ""
        
        # Convert relative paths to absolute paths for the config
        if not os.path.isabs(input_folder):
            input_folder = os.path.abspath(input_folder)
        if not os.path.isabs(output_folder):
            output_folder = os.path.abspath(output_folder)
        
        # Ensure recipes is always a list or string (not converting string to list)
        config = {
            "inputFolder": input_folder,
            "outputDeidentifiedFolder": output_folder,
            "outputPrivateMappingFolder": os.path.join(output_folder, "privateMapping"),
            "recipesFolder": recipes_folder or os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "scripts", "anonymization_recipes"),
            "recipes": recipes,
            "outputFolderHierarchy": "copy_from_input",
            "encryptionRoot": encryption_root or "test_encryption_key"
        }
        
        # Create temporary config file
        config_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, config_file, indent=2)
        config_file.close()
        
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
            recipes=["dicom_basic_profile"],
            encryption_root="test_mapping_key"
        )

        try:
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()
            
            # Check that the mapping file was created - use absolute path to match what anonymizer uses
            mapping_file = os.path.join(os.path.abspath(self.test_output_dir), "privateMapping", "uid_mappings.csv")
            
            # List all files in the privateMapping directory
            private_mapping_dir = os.path.join(os.path.abspath(self.test_output_dir), "privateMapping")
            if os.path.exists(private_mapping_dir):
                files_in_dir = os.listdir(private_mapping_dir)
            else:
                print("privateMapping directory doesn't exist")
            
            self.assertTrue(os.path.exists(mapping_file), "UID mapping CSV file was not created")

            # Read and verify the mapping file content
            with open(mapping_file, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                
                # Should have exactly one row for our single file
                self.assertEqual(len(rows), 1, f"Expected 1 row in mapping file, got {len(rows)}")
                
                row = rows[0]
                
                # Check that required columns exist
                required_columns = [
                    'file_path',
                    'StudyInstanceUID_original', 'StudyInstanceUID_anonymized',
                    'SeriesInstanceUID_original', 'SeriesInstanceUID_anonymized', 
                    'SOPInstanceUID_original', 'SOPInstanceUID_anonymized'
                ]
                
                for column in required_columns:
                    self.assertIn(column, row, f"Required column '{column}' missing from mapping file")
                
                # Verify file path
                self.assertEqual(row['file_path'], '00000001.dcm', "File path in mapping is incorrect")
                
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
                
        finally:
            # Clean up config file
            os.unlink(config_path)

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
            recipes=["dicom_basic_profile"],
            encryption_root="test_parquet_key"
        )

        try:
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Check that the Parquet file was created
            parquet_file = os.path.join(os.path.abspath(self.test_output_dir), "privateMapping", "metadata.parquet")
            self.assertTrue(os.path.exists(parquet_file), "Parquet metadata file was not created")

            # Read and verify the Parquet file content
            try:
                df = pd.read_parquet(parquet_file)
                
                # Should have exactly one row for our single file
                self.assertEqual(len(df), 1, f"Expected 1 row in Parquet file, got {len(df)}")
                
                # Check that essential tracking columns exist
                required_columns = ['AnonymizedFilePath']
                for column in required_columns:
                    self.assertIn(column, df.columns, f"Required column '{column}' missing from Parquet file")
                
                # Verify file paths
                row = df.iloc[0]
                self.assertEqual(row['AnonymizedFilePath'], '00000001.dcm', "Anonymized file path in Parquet is incorrect")
                
                # Check that some DICOM metadata columns exist (after anonymization)
                # Note: Some fields may be removed/anonymized, so we check for commonly retained ones
                
                # Check for some basic DICOM fields that should survive anonymization
                basic_dicom_columns = ['SpecificCharacterSet', 'ImageType', 'SOPClassUID']
                found_columns = [col for col in basic_dicom_columns if col in df.columns]
                
                # We should have at least some DICOM metadata columns beyond just AnonymizedFilePath
                self.assertGreater(len(df.columns), 1, 
                                 "Parquet should contain DICOM metadata beyond just file paths")
                
                # Check that we have some actual DICOM tags (not just our tracking columns)
                non_tracking_columns = [col for col in df.columns if col != 'AnonymizedFilePath']
                self.assertGreater(len(non_tracking_columns), 0,
                                 "Parquet should contain actual DICOM metadata columns")
                
                print(f"Found {len(non_tracking_columns)} DICOM metadata columns: {non_tracking_columns[:5]}...")  # Show first 5
                
            except ImportError:
                self.fail("pandas is required to read Parquet files for testing. Please install pandas.")
            except Exception as e:
                self.fail(f"Failed to read or validate Parquet file: {e}")
                
        finally:
            # Clean up config file
            os.unlink(config_path)

    def test_parquet_metadata_export_multiple_files(self):
        """Test that the Parquet metadata file is created correctly for multiple input files."""
        print("Test Parquet metadata export with multiple files")
        
        # Use the existing limited_input_dir which already has multiple files (up to 10)
        # This avoids unnecessary file copying and directory creation
        
        # Create test config with basic profile to trigger anonymization
        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["dicom_basic_profile"],
            encryption_root="test_multi_parquet_key"
        )

        try:
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Check that the Parquet file was created
            parquet_file = os.path.join(os.path.abspath(self.test_output_dir), "privateMapping", "metadata.parquet")
            self.assertTrue(os.path.exists(parquet_file), "Parquet metadata file was not created")

            # Count expected files in input directory
            input_files = [f for f in os.listdir(self.limited_input_dir) if f.endswith('.dcm')]
            expected_count = len(input_files)

            # Read and verify the Parquet file content
            try:
                df = pd.read_parquet(parquet_file)
                
                # Should have one row per input file
                self.assertEqual(len(df), expected_count, 
                               f"Expected {expected_count} rows in Parquet file, got {len(df)}")
 
                # Check that essential tracking columns exist
                required_columns = ['AnonymizedFilePath']
                for column in required_columns:
                    self.assertIn(column, df.columns, f"Required column '{column}' missing from Parquet file")
                
                # Verify that we have the expected number of unique file paths
                unique_paths = df['AnonymizedFilePath'].nunique()
                self.assertEqual(unique_paths, expected_count,
                               f"Expected {expected_count} unique file paths, got {unique_paths}")
                
                # Check that we have DICOM metadata columns beyond just file paths
                non_tracking_columns = [col for col in df.columns if col != 'AnonymizedFilePath']
                self.assertGreater(len(non_tracking_columns), 0,
                                 "Parquet should contain actual DICOM metadata columns")
                
                print(f"Found {len(non_tracking_columns)} DICOM metadata columns")
                print(f"Sample columns: {non_tracking_columns[:5]}...")
                
                # Verify that each row has data (not all null values)
                for idx, row in df.iterrows():
                    non_null_count = row.notna().sum()
                    self.assertGreater(non_null_count, 1,  # At least AnonymizedFilePath + some DICOM data
                                     f"Row {idx} should have more than just file path data")
                
                # Test with at least 2 files to verify different files can have different metadata
                if len(df) > 1:
                    row1_data = df.iloc[0].to_dict()
                    row2_data = df.iloc[1].to_dict()
                    
                    # Files should have different AnonymizedFilePath
                    self.assertNotEqual(row1_data['AnonymizedFilePath'], row2_data['AnonymizedFilePath'],
                                      "Different files should have different AnonymizedFilePath values")
                    
                    print(f"File 1: {row1_data['AnonymizedFilePath']}")
                    print(f"File 2: {row2_data['AnonymizedFilePath']}")
                
            except ImportError:
                self.fail("pandas is required to read Parquet files for testing. Please install pandas.")
            except Exception as e:
                self.fail(f"Failed to read or validate Parquet file: {e}")
                
        finally:
            # Clean up config file
            os.unlink(config_path)

    def test_csv_and_parquet_consistency(self):
        """Test that CSV mapping and Parquet export are consistent with each other."""
        print("Test CSV and Parquet export consistency")
        
        # Use a few files for testing consistency
        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["dicom_basic_profile"],
            encryption_root="test_consistency_key"
        )

        try:
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Check that both files were created
            mapping_file = os.path.join(os.path.abspath(self.test_output_dir), "privateMapping", "uid_mappings.csv")
            parquet_file = os.path.join(os.path.abspath(self.test_output_dir), "privateMapping", "metadata.parquet")
            
            self.assertTrue(os.path.exists(mapping_file), "CSV mapping file was not created")
            self.assertTrue(os.path.exists(parquet_file), "Parquet metadata file was not created")

            # Read both files
            with open(mapping_file, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                csv_rows = list(reader)
            
            df = pd.read_parquet(parquet_file)
            
            # Should have same number of files processed
            self.assertEqual(len(csv_rows), len(df), 
                           f"CSV has {len(csv_rows)} rows but Parquet has {len(df)} rows")
            
            # Check that file names are consistent
            csv_files = set(row['file_path'] for row in csv_rows)
            parquet_files = set(df['AnonymizedFilePath'].tolist())
            
            self.assertEqual(csv_files, parquet_files,
                           f"File names don't match between CSV and Parquet: CSV={csv_files}, Parquet={parquet_files}")
            
            print(f"Successfully verified consistency between CSV ({len(csv_rows)} files) and Parquet ({len(df)} files)")
                
        finally:
            # Clean up config file
            os.unlink(config_path)


if __name__ == "__main__":
    unittest.main()
