import unittest
import subprocess
import os
import shutil

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

    def tearDown(self):
        # Clean up test directories
        if os.path.exists(self.test_input_dir):
            shutil.rmtree(self.test_input_dir)
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)

    def test_script_runs_correctly(self):
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

    def test_placeholder_for_additional_checks(self):
        # Placeholder for additional tests to check specific actions
        #to do: add tests for all actions on recipe
        pass

if __name__ == "__main__":
    unittest.main()


