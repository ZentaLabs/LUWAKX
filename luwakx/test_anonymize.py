import unittest
import subprocess
import os
import shutil
import pydicom
import re
import tarfile
import urllib.request

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
        print("\n######################START TEST######################")

    def tearDown(self):
        # Clean up output directory after each test
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        print("\n######################END TEST######################")

    def test_script_runs_on_first_file(self):
        """Test that the anonymize script runs on the file `00000001.dcm` without errors."""
        print("Test anonymize script runs on the file `00000001.dcm`")

        # Define the path to the specific file
        first_file = os.path.join(self.test_data_dir, "00000001.dcm")

        self.assertTrue(os.path.exists(first_file), "File `00000001.dcm` not found in the dataset.")

        # Run the anonymize script on the file
        result = subprocess.run([
            "python", "luwakx/anonymize.py",
            "--base", first_file,
            "--output", self.test_output_dir
        ], capture_output=True, text=True)

        # Check if the script ran without errors
        self.assertEqual(result.returncode, 0, f"Script failed with error: {result.stderr}")

        # Check if the output directory contains the anonymized file
        anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(anonymized_file), "Anonymized file `00000001.dcm` not found in output directory")

    def test_private_tags_removed(self):
        """Test that private tags are removed when retain_safe_private_tags is False."""

        print("Test private tags are removed when retain_safe_private_tags is False")

        # Run the anonymize script on the entire dataset
        result = subprocess.run([
            "python", "luwakx/anonymize.py",
            "--base", self.test_data_dir,
            "--output", self.test_output_dir,
            "--retain_safe_private_tags", "False"
        ], capture_output=True, text=True)

        # Check if the script ran without errors
        self.assertEqual(result.returncode, 0, f"Script failed with error: {result.stderr}")

        # Verify that all files in the output directory have no private tags
        for file in os.listdir(self.test_output_dir):
            if not file.endswith(".dcm"):
                continue

            anonymized_file = os.path.join(self.test_output_dir, file)
            self.assertTrue(os.path.exists(anonymized_file), f"Anonymized file {file} not found.")

            ds = pydicom.dcmread(anonymized_file)
            private_tags = [tag for tag in ds.iterall() if tag.is_private]
            self.assertEqual(len(private_tags), 0, f"Private tags were not removed in file {file}.")

    def test_keep_specific_private_tags_should_be_original_value(self):
        """RECIPE RULE
        KEEP (0019,"GEMS_ACQU_01",9E)
        KEEP (0025,"GEMS_SERS_01",07)
        """

        print("Test KEEP private tags")

        # Run the anonymize script on the entire dataset
        result = subprocess.run([
            "python", "luwakx/anonymize.py",
            "--base", self.test_data_dir,
            "--output", self.test_output_dir,
            "--retain_safe_private_tags", "True"
        ], capture_output=True, text=True)

        # Check if the script ran without errors
        self.assertEqual(result.returncode, 0, f"Script failed with error: {result.stderr}")

        # Define the private tags to check
        expected_private_tags = {
            "0019109E": "GEMS_ACQU_01",
            "00251007": "GEMS_SERS_01"
        }

        # Verify that the specified private tags are retained in the output files
        for file in os.listdir(self.test_output_dir):
            if not file.endswith(".dcm"):
                continue

            anonymized_file = os.path.join(self.test_output_dir, file)
            self.assertTrue(os.path.exists(anonymized_file), f"Anonymized file {file} not found.")
            ds = pydicom.dcmread(anonymized_file)
            for tag_str, expected_creator in expected_private_tags.items():
                tag = pydicom.tag.Tag(tag_str)
                # Check if the private tag exists
                self.assertIn(tag, ds, f"Private tag {tag_str} not found in file {file}.")
                # Check if the private creator matches
                element = ds[tag]
                self.assertEqual(element.private_creator, expected_creator, f"Private creator mismatch for tag {tag_str} in file {file}.")

    '''
    #Decide what to do with this test, is it necessary? How to improve it?
    def test_safe_private_tags_retention(self):
        """Test that private tags matching the safe_private_tags recipe are retained, and others are removed."""
        print("Test safe private tags retention based on recipe")

        # Path to the test DICOM file
        print("Checking if test_data directory exists...")
        if not os.path.exists(self.test_data_dir):
            print(f"Test data directory '{self.test_data_dir}' not found. Please ensure it exists.")
            return
        test_dicom_file = os.path.join(self.test_data_dir, "00000001.dcm")
        safe_private_tags_recipe = os.path.join(os.getenv("GITHUB_WORKSPACE", ""), "luwakx/scripts/anonymization_recipes/deid.dicom.safe-private-tags")
        #safe_private_tags_recipe = "./scripts/anonymization_recipes/deid.dicom.safe-private-tags" #
        # Parse the safe_private_tags recipe to extract KEEP expressions
        keep_expressions = []
        with open(safe_private_tags_recipe, "r") as recipe_file:
            for line in recipe_file:
                line = line.strip()
                if line.startswith("KEEP"):
                    expression = line.split("KEEP", 1)[1].strip()
                    keep_expressions.append(expression)
        
        # Load the original DICOM file and identify private tags matching KEEP expressions
        
        ds = pydicom.dcmread(test_dicom_file)
        matching_private_tags = []
        for tag in ds.iterall():
            if tag.is_private and tag.private_creator is not None:
                stripped_private_tag = f'{tag.tag.group:04X},"{tag.private_creator}",{(tag.tag.element & 0xFF):02X}'
                for expression in keep_expressions:
                    if re.search(expression, stripped_private_tag, re.IGNORECASE):
                        matching_private_tags.append(stripped_private_tag)
                        break
        
        # Run the anonymize script
        result = subprocess.run([
            "python", "luwakx/anonymize.py",
            "--base", test_dicom_file,
            "--output", self.test_output_dir,
            "--retain_safe_private_tags", "True"
        ], capture_output=True, text=True)

        # Check if the script ran without errors
        self.assertEqual(result.returncode, 0, f"Script failed with error: {result.stderr}")

        # Load the anonymized file and verify private tags
        anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(anonymized_file), "Anonymized file not found.")

        # Verify the file exists before reading
        self.assertTrue(os.path.exists(anonymized_file), f"Anonymized file not found at {anonymized_file}")

        # Log file details for debugging
        print(f"File path: {anonymized_file}")
        print(f"File size: {os.path.getsize(anonymized_file)} bytes")

        # Force read the file with pydicom to bypass header issues
        ds_anonymized = pydicom.dcmread(anonymized_file, force=True)
        retained_private_tags = []
        removed_private_tags = []
        for tag in ds_anonymized.iterall():
            if tag.is_private and tag.private_creator is not None:
                stripped_private_tag = f'{tag.tag.group:04X},"{tag.private_creator}",{(tag.tag.element & 0xFF):02X}'
                retained_private_tags.append(stripped_private_tag)

        # Check that all matching private tags are retained
        for tag in matching_private_tags:
            self.assertIn(tag, retained_private_tags, f"Expected private tag {tag} to be retained but it was removed.")

        # Check that non-matching private tags are removed
        for tag in retained_private_tags:
            if tag not in matching_private_tags:
                removed_private_tags.append(tag)
        self.assertEqual(len(removed_private_tags), 0, f"Unexpected private tags retained: {removed_private_tags}")
    '''

if __name__ == "__main__":
    unittest.main()

