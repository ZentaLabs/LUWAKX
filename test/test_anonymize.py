import unittest
import subprocess
import os
import shutil
import pydicom
import tarfile
import json
import tempfile
import sys

# Add luwakx directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
from anonymize import LuwakAnonymizer
from luwak_logger import setup_logger, get_logger
from utils import download_github_asset_by_tag
from dicom_processor import DicomProcessor

class TestAnonymizeScript(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a temporary output directory
        cls.test_output_dir = "test_output"

        # Path to the decompressed test data directory
        cls.test_data_dir = "test_data"
        token = os.environ.get("TEST_DATA_TOKEN")
        #target_dir = os.path.join(cls.test_data_dir, "test-dicom-files-Midi-B-2024")
        # Check if the test data directory exists
        if not os.path.exists(cls.test_data_dir):
            os.makedirs(cls.test_data_dir, exist_ok=True)
            archive_path = os.path.join(cls.test_data_dir, "test-dicom-files-Midi-B-2024.tar.gz")
            download_github_asset_by_tag(
                "ZentaLabs", "luwak", "testing-data", "test-dicom-files-Midi-B-2024.tar.gz", archive_path, token
            )
            # Extract the archive
            with tarfile.open(archive_path, "r:gz") as tar:
                # Extract all files directly into the test_data_dir
                for member in tar.getmembers():
                    # Remove the top-level folder from the path
                    member.path = os.path.relpath(member.path, start="test-dicom-files-2")
                    tar.extract(member, path=cls.test_data_dir, filter='data')
            # Clean up the downloaded archive
            os.remove(archive_path)


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

    def get_output_path_for_file(self, coordinator, input_file_path):
        """Helper method to find the output file path for a given input file.
        
        Args:
            coordinator: PipelineCoordinator instance returned by anonymize()
            input_file_path: Path to the original input DICOM file
            
        Returns:
            Path to the anonymized output file, or None if not found
        """
        input_basename = os.path.basename(input_file_path)
        
        # Search through all series in the coordinator
        for series in coordinator.all_series:
            for dicom_file in series.files:
                # Check if this file matches the input file
                if os.path.basename(dicom_file.original_path) == input_basename:
                    if dicom_file.anonymized_path and os.path.exists(dicom_file.anonymized_path):
                        return dicom_file.anonymized_path
        
        # Fallback: search the output directory recursively
        for root, dirs, files in os.walk(self.test_output_dir):
            if input_basename in files:
                return os.path.join(root, input_basename)
        
        return None
    
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
        recipes_folder = os.path.join(output_folder, "recipes")
        os.makedirs(recipes_folder, exist_ok=True)
        # Output mapping folder
        output_private_mapping_folder = os.path.join(output_folder, "private")
        
        # Setup logger with the actual output and recipe paths
        log_file_path = os.path.join(self.test_output_dir, 'luwak_test.log')
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        setup_logger(log_level='INFO', log_file=log_file_path, console_output=False)
        self.logger = get_logger('test_anonymize')
        self.logger.info(f"Setting up test configuration with output: {output_folder}, recipes: {recipes_folder}")
        
        # Fill in all config keys
        config = {
            "inputFolder": input_folder,
            "outputDeidentifiedFolder": output_folder,
            "outputPrivateMappingFolder": output_private_mapping_folder,
            "recipesFolder": recipes_folder,
            "recipes": recipes if recipes is not None else "deid.dicom",
            "cleanDescriptorsLlmBaseUrl": "https://api.openai.com/v1",
            "cleanDescriptorsLlmModel": "gpt-4o-mini",
            "cleanDescriptorsLlmApiKeyEnvVar": "ZENTA_OPENAI_API_KEY"
        }
        # Create temporary config file
        config_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, config_file, indent=2)
        config_file.close()
        
        self.logger.info(f"Created test config file: {config_file.name}")
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
            self.logger.info("Starting anonymization of first file test")
            # Run the anonymize script using the new class structure
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()

            # Get the correct output path for the file using the new helper method
            expected_output_path = self.get_output_path_for_file(coordinator, first_file)
            self.assertIsNotNone(expected_output_path, f"Could not find output path for {first_file}")
            self.assertTrue(os.path.exists(expected_output_path), f"Anonymized file `00000001.dcm` not found at expected path: {expected_output_path}")
            self.logger.info(f"Successfully anonymized file: {expected_output_path}")
            
        finally:
            # Clean up config file
            os.unlink(config_path)
            self.logger.info("Test completed and config cleaned up")

    def test_keep_specific_private_tags_should_be_original_value(self):
        """Test KEEP private tags using retain_safe_private_tags recipe on batch input."""
        print("Test KEEP private tags with retain_safe_private_tags recipe (first 50 input files)")

        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["retain_safe_private_tags"]
        )
        try:
            self.logger.info("Starting private tags retention test (batch)")
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()

            expected_private_tags = {
                "0019109e": "gems_acqu_01",
                "00251007": "gems_sers_01"
            }
            unexpected_private_tags = {
                "0019109d": "gems_acqu_01",  # This tag should be removed
            }

            for file in os.listdir(self.limited_input_dir):
                if not file.endswith(".dcm"):
                    continue
                input_file = os.path.join(self.limited_input_dir, file)
                
                # Find corresponding output file using helper method
                output_file = self.get_output_path_for_file(coordinator, input_file)
                
                self.assertIsNotNone(output_file, f"Anonymized file {file} not found in output directory")
                ds = pydicom.dcmread(output_file)
                for tag_str, expected_creator in expected_private_tags.items():
                    tag = pydicom.tag.Tag(f"0x{tag_str}")
                    self.assertIn(tag, ds)
                    element = ds[tag]
                    self.assertEqual(element.private_creator.lower(), expected_creator, f"Private creator mismatch for tag {tag_str} in file {file}.")
                for tag_str, unexpected_creator in unexpected_private_tags.items():
                    tag = pydicom.tag.Tag(f"0x{tag_str}")
                    self.assertNotIn(tag, ds, f"Unexpected private tag {tag_str} found in file {file}.")
            self.logger.info("Private tags retention batch test completed successfully")
        finally:
            os.unlink(config_path)
            self.logger.info("Test completed and config cleaned up")

    def test_luwakx_wrapper_script(self):
        """Test that the luwakx.py wrapper script works with config files (batch input)."""
        print("Test luwakx.py wrapper script with config file (batch input)")

        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=None
        )

        try:
            self.logger.info("Starting luwakx wrapper script batch test")
            script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "luwakx.py")
            self.logger.info(f"Running luwakx script: {script_path}")
            result = subprocess.run([
                "python", script_path,
                "--config_path", config_path
            ], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, f"luwakx.py failed with error: {result.stderr}")
            
            for file in os.listdir(self.limited_input_dir):
                if not file.endswith(".dcm"):
                    continue
                input_file = os.path.join(self.limited_input_dir, file)
                
                # Find corresponding output file by filename (subprocess doesn't return coordinator)
                output_file = None
                for root, dirs, files in os.walk(self.test_output_dir):
                    if file in files:
                        output_file = os.path.join(root, file)
                        break
                
                self.assertIsNotNone(output_file, f"Anonymized file {file} not found in output directory")
                self.logger.info(f"Successfully created anonymized file via luwakx wrapper: {output_file}")
        finally:
            os.unlink(config_path)
            self.logger.info("Test completed and config cleaned up")

    def test_uid_generation(self):
        """Test the generation of new UIDs for StudyInstanceUID, SeriesInstanceUID, and SOPInstanceUID on batch input."""
        print("Test UID generation on batch input (first 50 files)")

        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["basic_profile"],
        )
        try:
            self.logger.info("Starting UID generation batch test")
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()

            # Match input files to output files by filename
            for file in os.listdir(self.limited_input_dir):
                if not file.endswith(".dcm"):
                    continue
                input_file = os.path.join(self.limited_input_dir, file)
                original_ds = pydicom.dcmread(input_file)
                original_uids = {
                    'StudyInstanceUID': getattr(original_ds, 'StudyInstanceUID', None),
                    'SeriesInstanceUID': getattr(original_ds, 'SeriesInstanceUID', None),
                    'SOPInstanceUID': getattr(original_ds, 'SOPInstanceUID', None)
                }
                for uid_name, uid_value in original_uids.items():
                    self.assertIsNotNone(uid_value, f"Original file missing {uid_name}")
                
                # Find corresponding output file using helper method
                output_file = self.get_output_path_for_file(coordinator, input_file)
                
                self.assertIsNotNone(output_file, f"Anonymized file {file} not found in output directory")
                anonymized_ds = pydicom.dcmread(output_file)
                anonymized_uids = {
                    'StudyInstanceUID': getattr(anonymized_ds, 'StudyInstanceUID', None),
                    'SeriesInstanceUID': getattr(anonymized_ds, 'SeriesInstanceUID', None),
                    'SOPInstanceUID': getattr(anonymized_ds, 'SOPInstanceUID', None)
                }
                for uid_name in ['StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID']:
                    original_uid = original_uids[uid_name]
                    anonymized_uid = anonymized_uids[uid_name]
                    self.assertIsNotNone(anonymized_uid, f"Anonymized file missing {uid_name}")
                    self.assertNotEqual(original_uid, anonymized_uid, f"{uid_name} was not changed during anonymization for file {file}")
                    self.logger.info(f"✓ {uid_name}: {original_uid} → {anonymized_uid} (file: {file})")
            self.logger.info("UID generation batch test completed and config cleaned up")
        finally:
            os.unlink(config_path)
            self.logger.info("UID generation batch test completed and config cleaned up")

    def test_basic_retain_uid_should_have_original_uid(self):
        """Test that mixing basic profile and retain uid option keeps original UID for retain fields (batch input)."""
        print("Test that mixing basic profile and retain uid option keeps original UID for retain fields (batch input).")

        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "retain_uid"],
        )
        try:
            self.logger.info("Starting basic profile + retain UID batch test")
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()

            for file in os.listdir(self.limited_input_dir):
                if not file.endswith(".dcm"):
                    continue
                input_file = os.path.join(self.limited_input_dir, file)
                original_ds = pydicom.dcmread(input_file)
                original_uids = {
                    'StudyInstanceUID': getattr(original_ds, 'StudyInstanceUID', None),
                    'SeriesInstanceUID': getattr(original_ds, 'SeriesInstanceUID', None),
                    'SOPInstanceUID': getattr(original_ds, 'SOPInstanceUID', None)
                }
                
                # Find corresponding output file using helper method
                output_file = self.get_output_path_for_file(coordinator, input_file)
                
                self.assertIsNotNone(output_file, f"Anonymized file {file} not found in output directory")
                anonymized_ds = pydicom.dcmread(output_file)
                anonymized_uids = {
                    'StudyInstanceUID': getattr(anonymized_ds, 'StudyInstanceUID', None),
                    'SeriesInstanceUID': getattr(anonymized_ds, 'SeriesInstanceUID', None),
                    'SOPInstanceUID': getattr(anonymized_ds, 'SOPInstanceUID', None)
                }
                for uid_name in ['StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID']:
                    original_uid = original_uids[uid_name]
                    anonymized_uid = anonymized_uids[uid_name]
                    self.assertIsNotNone(anonymized_uid, f"Anonymized file missing {uid_name}")
                    self.assertEqual(original_uid, anonymized_uid, f"{uid_name} was changed during anonymization for file {file}")
                    self.logger.info(f"✓ {uid_name} retained: {original_uid} (file: {file})")
            self.logger.info("Retain UID batch test completed and config cleaned up")
        finally:
            os.unlink(config_path)
            self.logger.info("Retain UID batch test completed and config cleaned up")

    def test_hash_increment_date(self):
        """Test the date shift functionality for DA, DT, and TM fields."""
        print("Test date shift generation and application")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        # Read original file to get date/time values
        original_ds = pydicom.dcmread(original_file)
        original_value = original_ds['00080021'].value
        
        # Create test config with basic profile (which should trigger date shifting)
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile"],
        )
        try:
            self.logger.info("Starting date shift test")
            self.logger.info(f"Original SeriesDate: {original_value}")
            
            anonymizer = LuwakAnonymizer(config_path)
            # Run anonymization
            coordinator = anonymizer.anonymize()
            expected_output_path = self.get_output_path_for_file(coordinator, original_file)
            self.assertIsNotNone(expected_output_path, f"Could not find output path for {original_file}")
            self.assertTrue(os.path.exists(expected_output_path), f"Anonymized file not found at expected path: {expected_output_path}")
            anonymized_ds = pydicom.dcmread(expected_output_path)
            # Check that the SeriesDate has been shifted correctly (if present)
            self.assertEqual(anonymized_ds.SeriesDate, '20130730',
                        "SeriesDate should be shifted by 181 days: expected '20130730', got {anonymized_ds.SeriesDate}")
            self.logger.info(f"✓ SeriesDate shifted: {original_value} → {anonymized_ds.SeriesDate}")
        finally:
            os.unlink(config_path)
            self.logger.info("Date shift test completed and config cleaned up")
          

    def test_fixed_datetime_generation(self):
        """Test the fixed datetime generation for DA, DT, and TM VR types."""
        print("Test fixed datetime generation for different VR types")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        original_ds = pydicom.dcmread(original_file)
        
        # Create test config 
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile"],
        )

        try:
            self.logger.info("Starting fixed datetime generation test")
            # Initialize anonymizer to get config and logger
            anonymizer = LuwakAnonymizer(config_path)
            
            # Create a DicomProcessor instance to test the method
            processor = DicomProcessor(
                config=anonymizer.config,
                logger=anonymizer.logger,
                llm_cache=None  # Not needed for this specific method test
            )
            
            # Test DA (Date) VR
            self.logger.info("Testing DA (Date) VR...")
            
            # Create mock field with actual DICOM date value in element
            mock_da_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'DA',
                    'value': '20240315'  # Original DICOM date value
                })()
            })()

            # The value parameter should be the recipe string, not the original value
            fixed_da = processor.set_fixed_datetime("item1", "func:set_fixed_datetime", mock_da_field, original_ds)
            self.assertEqual(fixed_da, "00010101", f"DA fixed should be '00010101', got '{fixed_da}'")
            self.logger.info(f"✓ DA (Date) VR: {mock_da_field.element.value} → {fixed_da}")
            
            # Test DT (DateTime) VR
            self.logger.info("Testing DT (DateTime) VR...")
            mock_dt_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'DT',
                    'value': '20240315143022.123456+0200'  # Original DICOM datetime value
                })()
            })()
            
            fixed_dt = processor.set_fixed_datetime("item1", "func:set_fixed_datetime", mock_dt_field, original_ds)
            expected_dt = "00010101010101.000000+0000"
            self.assertEqual(fixed_dt, expected_dt, f"DT fixed should be '{expected_dt}', got '{fixed_dt}'")
            self.logger.info(f"✓ DT (DateTime) VR: {mock_dt_field.element.value} → {fixed_dt}")
            
            # Test TM (Time) VR
            self.logger.info("Testing TM (Time) VR...")
            mock_tm_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'TM',
                    'value': '143022.123'  # Original DICOM time value
                })()
            })()
            
            fixed_tm = processor.set_fixed_datetime("item1", "func:set_fixed_datetime", mock_tm_field, original_ds)
            expected_tm = "000000.00"
            self.assertEqual(fixed_tm, expected_tm, f"TM fixed should be '{expected_tm}', got '{fixed_tm}'")
            self.logger.info(f"✓ TM (Time) VR: {mock_tm_field.element.value} → {fixed_tm}")
            self.logger.info("All fixed datetime generation tests passed!")
            self.logger.info(f"    - DA (Date): '{fixed_da}'")
            self.logger.info(f"    - DT (DateTime): '{fixed_dt}'")
            self.logger.info(f"    - TM (Time): '{fixed_tm}'")

        finally:
            # Clean up config file
            os.unlink(config_path)
            self.logger.info("Fixed datetime generation test completed and config cleaned up")

    def test_fixed_datetime_with_basic_profile_recipe(self):
        """Test fixed datetime generation when running full anonymization with DICOM basic profile recipe (batch input)."""
        print("Test fixed datetime generation with DICOM basic profile recipe (batch input)")

        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=['basic_profile'],
        )

        try:
            self.logger.info("Starting fixed datetime with basic profile recipe batch test")
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()

            for file in os.listdir(self.limited_input_dir):
                if not file.endswith(".dcm"):
                    continue
                input_file = os.path.join(self.limited_input_dir, file)
                
                # Find corresponding output file using helper method
                output_file = self.get_output_path_for_file(coordinator, input_file)
                
                self.assertIsNotNone(output_file, f"Anonymized file {file} not found in output directory")
                anonymized_ds = pydicom.dcmread(output_file)
                self.assertEqual(anonymized_ds['00080020'].value, "00010101", f"DA fixed should be '00010101', got '{anonymized_ds['00080020'].value}' (file: {file})")
                self.logger.info(f"✓ StudyDate (DA): {anonymized_ds['00080020'].value} (file: {file})")
                self.assertEqual(anonymized_ds['00080030'].value, "000000.00", f"TM fixed should be '000000.00', got '{anonymized_ds['00080030'].value}' (file: {file})")
                self.logger.info(f"✓ StudyTime (TM): {anonymized_ds['00080030'].value} (file: {file})")
            self.logger.info("Fixed datetime with basic profile batch test completed and config cleaned up")
        finally:
            os.unlink(config_path)
            self.logger.info("Fixed datetime with basic profile batch test completed and config cleaned up")

    def test_basic_retain_date_should_have_original_date(self):
        """Test that mixing retain and date shift keeps original date for retain fields (batch input)."""
        print("Test that mixing retain and date shift keeps original date for retain fields (batch input).")

        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "retain_long_full_dates", "retain_long_modified_dates"],
        )
        try:
            self.logger.info("Starting basic profile + retain dates batch test")
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()

            for file in os.listdir(self.limited_input_dir):
                if not file.endswith(".dcm"):
                    continue
                input_file = os.path.join(self.limited_input_dir, file)
                original_ds = pydicom.dcmread(input_file)
                original_value = original_ds['AcquisitionDate'].value if 'AcquisitionDate' in original_ds else None
                
                # Find corresponding output file using helper method
                output_file = self.get_output_path_for_file(coordinator, input_file)
                
                self.assertIsNotNone(output_file, f"Anonymized file {file} not found in output directory")
                anonymized_ds = pydicom.dcmread(output_file)
                if original_value is not None:
                    self.assertEqual(anonymized_ds.AcquisitionDate, original_value,
                        f"AcquisitionDate should be the original value: expected {original_value}, got {anonymized_ds.AcquisitionDate} (file: {file})")
                    self.logger.info(f"✓ AcquisitionDate retained: {original_value} (file: {file})")
            self.logger.info("Retain dates batch test completed and config cleaned up")
        finally:
            os.unlink(config_path)
            self.logger.info("Retain dates batch test completed and config cleaned up")
    
    def test_basic_modified_date_should_have_modified_date(self):
        """Test the that mixing basic profile and date shift modifies original date."""
        print("Test the that mixing basic profile and date shift modifies original date.")

        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        # Read original file to get date/time values
        original_ds = pydicom.dcmread(original_file)
        original_value = original_ds['AcquisitionDate'].value
        # Create test config with basic profile (which should trigger date shifting)
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "retain_long_modified_dates"],
        )
        try:
            self.logger.info("Starting basic profile + modified dates test")
            self.logger.info(f"Original AcquisitionDate to be modified: {original_value}")
            
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()
            expected_output_path = self.get_output_path_for_file(coordinator, original_file)
            self.assertIsNotNone(expected_output_path, f"Could not find output path for {original_file}")
            self.assertTrue(os.path.exists(expected_output_path), f"Anonymized file not found at expected path: {expected_output_path}")
            anonymized_ds = pydicom.dcmread(expected_output_path)
            # Check that the AcquisitionDate has been retained
            self.assertEqual(anonymized_ds.AcquisitionDate, '20130730',
                        "AcquisitionDate should be the original value: expected '20130730', got {anonymized_ds.AcquisitionDate}")
            self.logger.info(f"✓ AcquisitionDate modified: {original_value} → {anonymized_ds.AcquisitionDate}")
            
            self.assertEqual(anonymized_ds.LongitudinalTemporalInformationModified, 'MODIFIED',
                        "LongitudinalTemporalInformationModified should be 'MODIFIED': expected 'MODIFIED', got {anonymized_ds.LongitudinalTemporalInformationModified}")
            self.logger.info(f"✓ LongitudinalTemporalInformationModified: {anonymized_ds.LongitudinalTemporalInformationModified}")
        finally:
            os.unlink(config_path)
            self.logger.info("Modified dates test completed and config cleaned up")
    
    def test_basic_clean_descriptors_should_have_clean_value(self):
        """Test that mixing basic profile and clean descriptors clean the fields."""
        print("Test that mixing basic profile and clean descriptors clean the fields.")

        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        # Read original file to get date/time values
        original_ds = pydicom.dcmread(original_file)
        original_value = original_ds['RequestedProcedureDescription'].value
        
        # Create test config with basic profile (which should trigger date shifting)
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "clean_descriptors"],
        )
        try:
            self.logger.info("Starting basic profile + clean descriptors test")
            self.logger.info(f"Original RequestedProcedureDescription to be cleaned: {original_value}")

            anonymizer = LuwakAnonymizer(config_path)
            # Run anonymization
            coordinator = anonymizer.anonymize()
            expected_output_path = self.get_output_path_for_file(coordinator, original_file)
            self.assertIsNotNone(expected_output_path, f"Could not find output path for {original_file}")
            self.assertTrue(os.path.exists(expected_output_path), f"Anonymized file not found at expected path: {expected_output_path}")
            anonymized_ds = pydicom.dcmread(expected_output_path)
            # Check that the RequestedProcedureDescription has been cleaned (tag should be removed)
            self.assertNotIn('RequestedProcedureDescription', anonymized_ds, f"Unexpected tag RequestedProcedureDescription found in file {expected_output_path}.")
            self.logger.info(f"RequestedProcedureDescription cleaned: {original_value} → removed")
            self.assertEqual(anonymized_ds['PerformedProcedureStepDescription'].value, original_ds['PerformedProcedureStepDescription'].value,
                        "PerformedProcedureStepDescription should be empty: expected {original_ds['PerformedProcedureStepDescription'].value}, got {anonymized_ds['PerformedProcedureStepDescription'].value}")
            self.logger.info(f"PerformedProcedureStepDescription cleaned: {original_ds['PerformedProcedureStepDescription'].value} → {anonymized_ds['PerformedProcedureStepDescription'].value}")
        finally:
            os.unlink(config_path)
            self.logger.info("Basic profile + clean descriptors test completed and config cleaned up")

        
if __name__ == "__main__":
    unittest.main()

