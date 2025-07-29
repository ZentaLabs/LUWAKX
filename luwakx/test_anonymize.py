import unittest
import subprocess
import os
import shutil
import pydicom
import re

class TestAnonymizeScript(unittest.TestCase):

    def setUp(self):
        # Create a temporary input directory with test files
        self.test_input_dir = "test_input"
        self.test_output_dir = "test_output"
        os.makedirs(self.test_input_dir, exist_ok=True)
        with open(os.path.join(self.test_input_dir, "test_file.dcm"), "w") as f:
            f.write("Dummy DICOM content")

        # Ensure the output directory is clean
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        print("\n######################START######################")

    def tearDown(self):
        # Clean up test directories
        if os.path.exists(self.test_input_dir):
            shutil.rmtree(self.test_input_dir)
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
        print("\n######################END######################")

    def test_script_runs_correctly(self):
        """Test that the anonymize script runs without errors and produces output."""
        print("Test anonymize script runs without errors")
        # Run the anonymize script
        result = subprocess.run([
            "python", "anonymize.py",
            "--base", self.test_input_dir,
            "--output", self.test_output_dir
        ], capture_output=True, text=True)

        # Check if the script ran without errors
        self.assertEqual(result.returncode, 0, f"Script failed with error: {result.stderr}")

        # Check if the output directory contains the anonymized file
        output_files = os.listdir(self.test_output_dir)
        self.assertIn("test_file.dcm", output_files, "Anonymized file not found in output directory")

    def test_private_tags_removed(self):
        """Test that private tags are removed when retain_safe_private_tags is False."""

        print("Test private tags are removed when retain_safe_private_tags is False")
        # Path to the test DICOM file
        test_dicom_file = "/home/simona/Downloads/input_data_2/1104142010/3.1.363.1.0.6227606.3.741.1202632684086966693/3.1.363.1.0.6227606.3.741.1212668621585478698/00000001.dcm"
        
        # Run the anonymize script
        result = subprocess.run([
            "python", "anonymize.py",
            "--base", test_dicom_file,
            "--output", self.test_output_dir,
            "--retain_safe_private_tags", "False"
        ], capture_output=True, text=True)

        # Check if the script ran without errors
        self.assertEqual(result.returncode, 0, f"Script failed with error: {result.stderr}")

        # Load the anonymized file and check for private tags
        anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(anonymized_file), "Anonymized file not found.")

        ds = pydicom.dcmread(anonymized_file)
        private_tags = [tag for tag in ds.iterall() if tag.is_private]
        self.assertEqual(len(private_tags), 0, "Private tags were not removed.")

    def test_safe_private_tags_retention(self):
        """Test that private tags matching the safe_private_tags recipe are retained, and others are removed."""
        print("Test safe private tags retention based on recipe")

        # Path to the test DICOM file
        test_dicom_file = "/home/simona/Downloads/input_data_2/1104142010/3.1.363.1.0.6227606.3.741.1202632684086966693/3.1.363.1.0.6227606.3.741.1212668621585478698/00000001.dcm"
        safe_private_tags_recipe = "./scripts/anonymization_recipes/deid.dicom.safe-private-tags"

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
            "python", "anonymize.py",
            "--base", test_dicom_file,
            "--output", self.test_output_dir,
            "--retain_safe_private_tags", "True"
        ], capture_output=True, text=True)

        # Check if the script ran without errors
        self.assertEqual(result.returncode, 0, f"Script failed with error: {result.stderr}")

        # Load the anonymized file and verify private tags
        anonymized_file = os.path.join(self.test_output_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(anonymized_file), "Anonymized file not found.")

        ds_anonymized = pydicom.dcmread(anonymized_file)
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

if __name__ == "__main__":
    unittest.main()


