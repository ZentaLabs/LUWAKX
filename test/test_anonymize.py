import unittest
import subprocess
import os
import shutil
import pydicom
import re
import tarfile
import urllib.request
import json
import tempfile
import sys

# Add luwakx directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
from anonymize import LuwakAnonymizer

class TestAnonymizeScript(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a temporary output directory
        cls.test_output_dir = "test_output"

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

            # Extract the archive
            with tarfile.open(archive_path, "r:gz") as tar:
                # Extract all files directly into the test_data_dir
                for member in tar.getmembers():
                    # Remove the top-level folder from the path
                    member.path = os.path.relpath(member.path, start="test-dicom-files-2")
                    tar.extract(member, path=cls.test_data_dir, filter='data')

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

        # Create a limited input directory with first 50 files
        self.limited_input_dir = "test_input_50"
        self.create_limited_input_dataset()
        
        print("\n######################START TEST######################")
    
    def create_limited_input_dataset(self):
        """Create a dataset with only the first 50 DICOM files for testing."""
        if os.path.exists(self.limited_input_dir):
            shutil.rmtree(self.limited_input_dir)
        os.makedirs(self.limited_input_dir, exist_ok=True)
        
        # Get all DICOM files from test_data_dir and sort them
        all_files = [f for f in os.listdir(self.test_data_dir) if f.endswith('.dcm')]
        all_files.sort()

        # Take only first 50 files
        files_to_copy = all_files[:50]

        print(f"Creating limited input dataset with {len(files_to_copy)} files out of {len(all_files)} total files")

        # Copy the first 50 files to the limited input directory
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
        print("\n######################END TEST######################")

    def create_test_config(self, input_folder, output_folder, recipes=None, recipes_folder=None):
        """Helper method to create a temporary config file for testing."""
        if recipes is None:
            recipes = ""
        
        # Convert relative paths to absolute paths for the config
        if not os.path.isabs(input_folder):
            input_folder = os.path.abspath(input_folder)
        if not os.path.isabs(output_folder):
            output_folder = os.path.abspath(output_folder)
        # Recipes folder default
        recipes_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "scripts", "anonymization_recipes")

        # Output mapping folder
        output_private_mapping_folder = os.path.join(output_folder, "private")
        # Fill in all config keys
        config = {
            "inputFolder": input_folder,
            "outputDeidentifiedFolder": output_folder,
            "outputPrivateMappingFolder": output_private_mapping_folder,
            "recipesFolder": recipes_folder,
            "recipes": recipes if recipes is not None else "deid.dicom",
        }
        # Create temporary config file
        config_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, config_file, indent=2)
        config_file.close()
        return config_file.name

    def test_script_runs_on_first_file(self):
        """Test that the anonymize script runs on the file `00000001.dcm` without errors."""
        print("Test anonymize script runs on the file `00000001.dcm`")

        # Define the path to the specific file
        first_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(first_file), "File `00000001.dcm` not found in the dataset.")

        # Create test config pointing to the specific file
        config_path = self.create_test_config(
            input_folder=first_file,
            output_folder=self.test_output_dir,
            recipes=None
        )

        try:
            # Run the anonymize script using the new class structure
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Check if the output directory contains the anonymized file
            anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
            self.assertTrue(os.path.exists(anonymized_file), "Anonymized file `00000001.dcm` not found in output directory")
            
        finally:
            # Clean up config file
            os.unlink(config_path)

    def test_keep_specific_private_tags_should_be_original_value(self):
        """Test KEEP private tags using retain_safe_private_tags recipe."""
        print("Test KEEP private tags with retain_safe_private_tags recipe (first 50 input files)")
        
        # Create test config with retain_safe_private_tags recipe
        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["retain_safe_private_tags"]  # Use the built-in retain_safe_private_tags recipe
        )

        try:
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Define the private tags to check (using lowercase for case-insensitive comparison)
            expected_private_tags = {
                "0019109e": "gems_acqu_01",  # lowercase hex tag and private creator
                "00251007": "gems_sers_01"   # lowercase private creator
            }

            # Verify that the specified private tags are retained in the output files
            for file in os.listdir(self.test_output_dir):
                if not file.endswith(".dcm"):
                    continue

                anonymized_file = os.path.join(self.test_output_dir, file)
                self.assertTrue(os.path.exists(anonymized_file), f"Anonymized file {file} not found.")
                ds = pydicom.dcmread(anonymized_file)
                
                for tag_str, expected_creator in expected_private_tags.items():
                    tag = pydicom.tag.Tag(f"0x{tag_str}")
                    # Check if the private tag exists in files that originally had it
                    self.assertIn(tag, ds)
                    element = ds[tag]
                    self.assertEqual(element.private_creator.lower(), expected_creator, 
                                       f"Private creator mismatch for tag {tag_str} in file {file}.")
                        
        finally:
            # Clean up config file
            os.unlink(config_path)

    def test_luwakx_wrapper_script(self):
        """Test that the luwakx.py wrapper script works with config files."""
        print("Test luwakx.py wrapper script with config file")

        # Create test config
        config_path = self.create_test_config(
            input_folder=os.path.join(self.test_data_dir, "00000001.dcm"),
            output_folder=self.test_output_dir,
            recipes=None
        )

        try:
            # Run the luwakx wrapper script (path to luwakx directory)
            script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "luwakx.py")
            result = subprocess.run([
                "python", script_path,
                "--config_path", config_path
            ], capture_output=True, text=True)
            
            # Print the captured output to see deid prints
            #print("STDOUT:", result.stdout)
            #print("STDERR:", result.stderr)

            # Check if the script ran without errors  
            self.assertEqual(result.returncode, 0, f"luwakx.py failed with error: {result.stderr}")

            # Check if the output file was created
            anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
            self.assertTrue(os.path.exists(anonymized_file), "Anonymized file not found from luwakx.py")
            
        finally:
            # Clean up config file
            os.unlink(config_path)

    def test_uid_generation(self):
        """Test the generation of new UIDs for StudyInstanceUID, SeriesInstanceUID, and SOPInstanceUID."""
        print("Test UID generation on first file")
        
        # First, read the original file to get the original UIDs
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        original_ds = pydicom.dcmread(original_file)
        original_uids = {
            'StudyInstanceUID': getattr(original_ds, 'StudyInstanceUID', None),
            'SeriesInstanceUID': getattr(original_ds, 'SeriesInstanceUID', None),
            'SOPInstanceUID': getattr(original_ds, 'SOPInstanceUID', None)
        }
        
        # Verify original file has the UIDs we want to test
        for uid_name, uid_value in original_uids.items():
            self.assertIsNotNone(uid_value, f"Original file missing {uid_name}")

        # Create test config with basic profile (which should trigger UID generation)
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["dicom_basic_profile"],  # Use basic profile recipe which should trigger UID generation
        )

        try:
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Check if the anonymized file was created
            anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
            self.assertTrue(os.path.exists(anonymized_file), "Anonymized file not found.")

            # Read the anonymized file and check that UIDs have been changed
            anonymized_ds = pydicom.dcmread(anonymized_file)
            anonymized_uids = {
                'StudyInstanceUID': getattr(anonymized_ds, 'StudyInstanceUID', None),
                'SeriesInstanceUID': getattr(anonymized_ds, 'SeriesInstanceUID', None),
                'SOPInstanceUID': getattr(anonymized_ds, 'SOPInstanceUID', None)
            }
            
            # Verify that UIDs have been changed
            for uid_name in ['StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID']:
                original_uid = original_uids[uid_name]
                anonymized_uid = anonymized_uids[uid_name]  
                self.assertIsNotNone(anonymized_uid, f"Anonymized file missing {uid_name}")
                self.assertNotEqual(original_uid, anonymized_uid, 
                                  f"{uid_name} was not changed during anonymization")

        finally:
            # Clean up config file
            os.unlink(config_path)

    def test_date_shift_generation(self):
        """Test the date shift functionality for DA, DT, and TM fields."""
        print("Test date shift generation and application")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        # Read original file to get date/time values
        original_ds = pydicom.dcmread(original_file)
        original_values = {}
        # TODO
          

    def test_dummy_datetime_generation(self):
        """Test the dummy datetime generation for DA, DT, and TM VR types."""
        print("Test dummy datetime generation for different VR types")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        original_ds = pydicom.dcmread(original_file)
        
        # Create test config 
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["dicom_basic_profile"],
        )

        try:
            # Initialize anonymizer to test dummy datetime generation
            anonymizer = LuwakAnonymizer(config_path)
            
            # Test DA (Date) VR
            print("  Testing DA (Date) VR...")
            # Create mock field with actual DICOM date value in element
            mock_da_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'DA',
                    'value': '20240315'  # Original DICOM date value
                })()
            })()
            
            # The value parameter should be the recipe string, not the original value
            dummy_da = anonymizer.set_fixed_datetime("item1", "func:set_fixed_datetime", mock_da_field, original_ds)
            self.assertEqual(dummy_da, "00010101", f"DA dummy should be '00010101', got '{dummy_da}'")
            
            # Test DT (DateTime) VR
            print("  Testing DT (DateTime) VR...")
            mock_dt_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'DT',
                    'value': '20240315143022.123456+0200'  # Original DICOM datetime value
                })()
            })()
            
            dummy_dt = anonymizer.set_fixed_datetime("item1", "func:set_fixed_datetime", mock_dt_field, original_ds)
            expected_dt = "00010101010101.000000+0000"
            self.assertEqual(dummy_dt, expected_dt, f"DT dummy should be '{expected_dt}', got '{dummy_dt}'")
            
            # Test TM (Time) VR
            print("  Testing TM (Time) VR...")
            mock_tm_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'TM',
                    'value': '143022.123'  # Original DICOM time value
                })()
            })()
            
            dummy_tm = anonymizer.set_fixed_datetime("item1", "func:set_fixed_datetime", mock_tm_field, original_ds)
            expected_tm = "000000.00"
            self.assertEqual(dummy_tm, expected_tm, f"TM dummy should be '{expected_tm}', got '{dummy_tm}'")
            
            print("  All dummy datetime generation tests passed!")
            print(f"    - DA (Date): '{dummy_da}'")
            print(f"    - DT (DateTime): '{dummy_dt}'") 
            print(f"    - TM (Time): '{dummy_tm}'")
            
        finally:
            # Clean up config file
            os.unlink(config_path)

    def test_dummy_datetime_with_dicom_basic_profile_recipe(self):
        """Test dummy datetime generation when running full anonymization with DICOM basic profile recipe."""
        print("Test dummy datetime generation with DICOM basic profile recipe")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")

        # Read original file to get date/time values
        original_ds = pydicom.dcmread(original_file)
        print(f"  Checking for date/time fields that use set_fixed_datetime in basic-profile-2...")
        
        # DICOM tag keywords from deid.dicom.basic-profile-2 that use func:set_fixed_datetime
        # These correspond to the actual tags from the recipe file
        datetime_fields_to_check = ['StudyDate', 'StudyTime']
        
        # Create test config using dicom_basic_profile recipe which includes basic-profile-2
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=['dicom_basic_profile'],
        )

        try:
            # Run full anonymization
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()
            
            # Check if the anonymized file was created
            anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
            self.assertTrue(os.path.exists(anonymized_file), "Anonymized file not found.")
            
            # Read the anonymized file and check specific fields
            anonymized_ds = pydicom.dcmread(anonymized_file)
            
            # Check date fields (DA VR) - should be "00010101"
            self.assertEqual(anonymized_ds['00080020'].value, "00010101", f"DA dummy should be '00010101', got '{anonymized_ds['00080020'].value}'")
            # Check datetime fields (DT VR) - should be "00010101010101.000000+0000"
            #self.assertEqual(anonymized_ds['0040A13A'].value, "00010101010101.000000+0000", f"DT dummy should be '00010101010101.000000+0000', got '{anonymized_ds['0040A13A'].value}'")
            # Check time fields (TM VR) - should be "000000.00" if using dummy generation
            self.assertEqual(anonymized_ds['00080030'].value, "000000.00", f"TM dummy should be '000000.00', got '{anonymized_ds['00080030'].value}'")

        finally:
            # Clean up files
            os.unlink(config_path)



if __name__ == "__main__":
    unittest.main()

