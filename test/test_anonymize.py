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
                    tar.extract(member, path=cls.test_data_dir)

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

        # Create a limited input directory with first 100 files
        self.limited_input_dir = "test_input_100"
        self.create_limited_input_dataset()
        
        print("\n######################START TEST######################")
    
    def create_limited_input_dataset(self):
        """Create a dataset with only the first 100 DICOM files for testing."""
        if os.path.exists(self.limited_input_dir):
            shutil.rmtree(self.limited_input_dir)
        os.makedirs(self.limited_input_dir, exist_ok=True)
        
        # Get all DICOM files from test_data_dir and sort them
        all_files = [f for f in os.listdir(self.test_data_dir) if f.endswith('.dcm')]
        all_files.sort()

        # Take only first 100 files
        files_to_copy = all_files[:100]
        
        print(f"Creating limited input dataset with {len(files_to_copy)} files out of {len(all_files)} total files")
        
        # Copy the first 300 files to the limited input directory
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
        
        # Ensure recipes is always a list or string (not converting string to list)
        config = {
            "inputFolder": input_folder,
            "outputDeidentified_folder": output_folder,
            "outputPrivateMappingFolder": os.path.join(output_folder, "privateMapping"),
            "recipesFolder": recipes_folder or os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "scripts", "anonymization_recipes"),
            "recipes": recipes,
            "output_folder_hierarchy": "copy_from_input",
            "encryption_root": "test_encryption_key"
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

    def test_private_tags_removed(self):
        """Test that private tags are removed when using remove_private_tags recipe."""
        print("Test private tags are removed when using remove_private_tags recipe (first 300 input files)")
        
        # Create test config for the limited dataset with remove_private_tags recipe
        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["remove_private_tags"]  # Use the built-in remove_private_tags recipe (string, not list)
        )

        try:
            # Run the anonymize script
            anonymizer = LuwakAnonymizer(config_path)
            result = anonymizer.anonymize()

            # Verify that all files in the output directory have no private tags
            for file in os.listdir(self.test_output_dir):
                if not file.endswith(".dcm"):
                    continue

                anonymized_file = os.path.join(self.test_output_dir, file)
                self.assertTrue(os.path.exists(anonymized_file), f"Anonymized file {file} not found.")

                ds = pydicom.dcmread(anonymized_file)
                private_tags = [tag for tag in ds.iterall() if tag.is_private]
                self.assertEqual(len(private_tags), 0, f"Private tags were not removed in file {file}.")
                
        finally:
            # Clean up config file
            os.unlink(config_path)

    def test_keep_specific_private_tags_should_be_original_value(self):
        """Test KEEP private tags using retain_safe_private_tags recipe."""
        print("Test KEEP private tags with retain_safe_private_tags recipe (first 300 input files)")
        
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
                    tag = pydicom.tag.Tag(tag_str.lower())
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

            # Check if the script ran without errors  
            self.assertEqual(result.returncode, 0, f"luwakx.py failed with error: {result.stderr}")

            # Check if the output file was created
            anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
            self.assertTrue(os.path.exists(anonymized_file), "Anonymized file not found from luwakx.py")
            
        finally:
            # Clean up config file
            os.unlink(config_path)



if __name__ == "__main__":
    unittest.main()

