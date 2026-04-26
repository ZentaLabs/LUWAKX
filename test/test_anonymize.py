import gc
import unittest
import subprocess
import os
import shutil
import pydicom
import tarfile
import json
import tempfile
import sys

from luwakx.anonymize import LuwakAnonymizer
from luwakx.logging.luwak_logger import setup_logger, get_logger
from luwakx.utils import download_github_asset_by_tag
from luwakx.dicom.dicom_processor import DicomProcessor

class TestAnonymizeScript(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a temporary output directory
        cls.test_output_dir = "test_output"

        # Create llm_cache folder, clearing stale DB files from previous runs
        cls.llm_cache_folder = "test_llm_cache"
        os.makedirs(cls.llm_cache_folder, exist_ok=True)
        for db_file in ("job_checkpoint.db", "llm_cache.db", "patient_uid.db"):
            db_path = os.path.join(cls.llm_cache_folder, db_file)
            if os.path.exists(db_path):
                os.remove(db_path)
        if not os.path.isabs(cls.llm_cache_folder):
            cls.llm_cache_folder = os.path.abspath(cls.llm_cache_folder)
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

        # Initialize logger
        log_file_path = os.path.join(self.test_output_dir, 'luwak_test.log')
        setup_logger(log_level='INFO', log_file=log_file_path, console_output=False)
        self.logger = get_logger('test_anonymize')

        # Remove patient UID database file to avoid stale prefix
        private_mapping_folder = os.path.join(self.test_output_dir, "private")
        uid_db_file = os.path.join(private_mapping_folder, "patient_uid.db")
        if os.path.exists(uid_db_file):
            os.remove(uid_db_file)

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
        # Close log file (open file would prevent rmtree)
        import logging
        root_logger = logging.getLogger('luwak')
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)
        # Force GC to close any SQLite connections (e.g. patient_uid.db) still
        # held by test-local LuwakAnonymizer / DicomProcessor objects.
        gc.collect()
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
    
    def create_test_config(self, input_folder, output_folder, recipes=None, recipes_folder=None, patientIdPrefix=None, analysisCacheFolder=None):
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
        
        self.logger.info(f"Setting up test configuration with output: {output_folder}, recipes: {recipes_folder}")
        
        # Fill in all config keys
        config = {
            "inputFolder": input_folder,
            "outputDeidentifiedFolder": output_folder,
            "outputPrivateMappingFolder": output_private_mapping_folder,
            "recipesFolder": recipes_folder,
            "recipes": recipes if recipes is not None else "deid.dicom",
        }
        
        # Add patientIdPrefix if provided
        if patientIdPrefix is not None:
            config["patientIdPrefix"] = patientIdPrefix
        
        # Add analysisCacheFolder if provided
        if analysisCacheFolder is not None:
            config["analysisCacheFolder"] = analysisCacheFolder
        
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
            recipes=["basic_profile","retain_safe_private_tags"]
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
                "000910e9": "gems_iden_01",  # This tag should be removed
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
            self.logger.info("Running luwakx via python -m luwakx.luwakx")
            result = subprocess.run([
                sys.executable, "-m", "luwakx.luwakx",
                "--config_path", config_path
            ], capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(__file__)))
            self.assertEqual(result.returncode, 0, f"luwakx.py failed with error: {result.stderr}")
            
            # Map input to output files using sequential ordering
            # Both input and output files are sorted, so we can match by position
            input_files = sorted([f for f in os.listdir(self.limited_input_dir) if f.endswith(".dcm")])
            
            # Find all output DICOM files recursively
            output_files = []
            for root, dirs, files in os.walk(self.test_output_dir):
                for file in sorted(files):
                    if file.endswith(".dcm"):
                        output_files.append(os.path.join(root, file))
            output_files.sort()
            
            # Verify we have the same number of files
            self.assertEqual(len(input_files), len(output_files), 
                f"Mismatch in file count: {len(input_files)} input files vs {len(output_files)} output files")
            
            # Match files by position in sorted order
            for i, input_filename in enumerate(input_files):
                input_file = os.path.join(self.limited_input_dir, input_filename)
                output_file = output_files[i]
                
                self.assertTrue(os.path.exists(output_file), f"Anonymized file {output_file} not found")
                self.logger.info(f"Successfully matched: {input_filename} -> {os.path.basename(output_file)}")
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
                    self.logger.info(f" {uid_name}: {original_uid} -> {anonymized_uid} (file: {file})")
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
                    self.logger.info(f" {uid_name} retained: {original_uid} (file: {file})")
            self.logger.info("Retain UID batch test completed and config cleaned up")
        finally:
            os.unlink(config_path)
            self.logger.info("Retain UID batch test completed and config cleaned up")

    def test_generate_hmacdate_shift(self):
        """Test the date shift functionality for DA, DT, and TM fields."""
        print("Test date shift generation and application")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        # Read original file to get date/time values
        original_ds = pydicom.dcmread(original_file)
        original_series_date = original_ds['00080021'].value
        
        # Create persistent analysis cache folder for this test
        cache_folder = os.path.abspath(os.path.join(self.test_output_dir, "private", "analysis_cache"))
        os.makedirs(cache_folder, exist_ok=True)
        uid_db_path = os.path.join(cache_folder, "patient_uid.db")
        
        # Create test config with persistent cache
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["retain_long_modified_dates"],
            analysisCacheFolder=cache_folder
        )
        try:
            self.logger.info("Starting date shift test with persistent UID database")
            self.logger.info(f"Original SeriesDate: {original_series_date}")
            
            anonymizer = LuwakAnonymizer(config_path)
            # Run anonymization
            coordinator = anonymizer.anonymize()
            
            # Now retrieve the patient's random token from the database to calculate expected shift
            from datetime import datetime, timedelta
            import sqlite3
            import hmac
            import hashlib
            
            # Query the database for the random token (we have only one patient entry)
            conn = sqlite3.connect(uid_db_path)
            cursor = conn.cursor()
            # Get the first row from the patient_mappings table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            self.logger.info(f"Available tables: {tables}")
            cursor.execute("SELECT random_token FROM patient_mappings LIMIT 1")
            result = cursor.fetchone()
            conn.close()
            
            self.assertIsNotNone(result, "Patient not found in database")
            random_token = result[0]
            
            # Calculate expected date shift using same logic as generate_hmacdate_shift
            project_hash_root = anonymizer.config.get('projectHashRoot', '')
            data = f"{project_hash_root}".encode('utf-8')
            mac = hmac.new(random_token, data, hashlib.sha512)
            hash_hex = mac.hexdigest()
            hash_int = int(hash_hex[:16], 16)
            max_shift = anonymizer.config.get('maxDateShiftDays', 1095)
            expected_shift_days = (hash_int % max_shift) + 1  # +1 to ensure non-zero
            
            # Calculate expected shifted date
            original_date = datetime.strptime(original_series_date, '%Y%m%d')
            expected_shifted_date = original_date - timedelta(days=expected_shift_days)
            expected_shifted_date_str = expected_shifted_date.strftime('%Y%m%d')
            
            self.logger.info(f"Calculated expected shift: -{expected_shift_days} days")
            self.logger.info(f"Expected SeriesDate: {expected_shifted_date_str}")
            
            # Verify the anonymized file
            expected_output_path = self.get_output_path_for_file(coordinator, original_file)
            self.assertIsNotNone(expected_output_path, f"Could not find output path for {original_file}")
            self.assertTrue(os.path.exists(expected_output_path), f"Anonymized file not found at expected path: {expected_output_path}")
            anonymized_ds = pydicom.dcmread(expected_output_path)
            
            # Check that the SeriesDate has been shifted correctly
            self.assertEqual(anonymized_ds.SeriesDate, expected_shifted_date_str,
                        f"SeriesDate should be shifted by {expected_shift_days} days: expected '{expected_shifted_date_str}', got '{anonymized_ds.SeriesDate}'")
            self.logger.info(f" SeriesDate shifted: {original_series_date} -> {anonymized_ds.SeriesDate} (shift: -{expected_shift_days} days)")
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
            self.assertEqual(fixed_da, "19000101", f"DA fixed should be '19000101', got '{fixed_da}'")
            self.logger.info(f" DA (Date) VR: {mock_da_field.element.value} -> {fixed_da}")
            
            # Test DT (DateTime) VR
            self.logger.info("Testing DT (DateTime) VR...")
            mock_dt_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'DT',
                    'value': '20240315143022.123456+0200'  # Original DICOM datetime value
                })()
            })()
            
            fixed_dt = processor.set_fixed_datetime("item1", "func:set_fixed_datetime", mock_dt_field, original_ds)
            expected_dt = "19000101000000.000000+0000"
            self.assertEqual(fixed_dt, expected_dt, f"DT fixed should be '{expected_dt}', got '{fixed_dt}'")
            self.logger.info(f" DT (DateTime) VR: {mock_dt_field.element.value} -> {fixed_dt}")
            
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
            self.logger.info(f" TM (Time) VR: {mock_tm_field.element.value} -> {fixed_tm}")
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
                self.assertEqual(anonymized_ds['00080020'].value, "19000101", f"DA fixed should be '19000101', got '{anonymized_ds['00080020'].value}' (file: {file})")
                self.logger.info(f" StudyDate (DA): {anonymized_ds['00080020'].value} (file: {file})")
                self.assertEqual(anonymized_ds['00080030'].value, "000000.00", f"TM fixed should be '000000.00', got '{anonymized_ds['00080030'].value}' (file: {file})")
                self.logger.info(f" StudyTime (TM): {anonymized_ds['00080030'].value} (file: {file})")
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
                    self.logger.info(f" AcquisitionDate retained: {original_value} (file: {file})")
            self.logger.info("Retain dates batch test completed and config cleaned up")
        finally:
            os.unlink(config_path)
            self.logger.info("Retain dates batch test completed and config cleaned up")
    
    def test_basic_modified_date_should_have_modified_date(self):
        """Test that mixing basic profile and date shift modifies original date."""
        print("Test that mixing basic profile and date shift modifies original date.")

        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        # Read original file to get date/time values
        original_ds = pydicom.dcmread(original_file)
        original_acquisition_date = original_ds['AcquisitionDate'].value
        
        # Create persistent analysis cache folder for this test
        cache_folder = os.path.abspath(os.path.join(self.test_output_dir, "private", "analysis_cache"))
        os.makedirs(cache_folder, exist_ok=True)
        uid_db_path = os.path.join(cache_folder, "patient_uid.db")
        
        # Create test config with basic profile and persistent cache
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "retain_long_modified_dates"],
            analysisCacheFolder=cache_folder
        )
        try:
            self.logger.info("Starting basic profile + modified dates test with persistent UID database")
            self.logger.info(f"Original AcquisitionDate to be modified: {original_acquisition_date}")
            
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()
            
            # Now retrieve the patient's random token from the database to calculate expected shift
            from datetime import datetime, timedelta
            import sqlite3
            import hmac
            import hashlib
            # Query the database for the random token (we have only one patient entry)
            conn = sqlite3.connect(uid_db_path)
            cursor = conn.cursor()
            # Get the first row from the patient_mappings table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            self.logger.info(f"Available tables: {tables}")
            cursor.execute("SELECT random_token FROM patient_mappings LIMIT 1")
            result = cursor.fetchone()
            conn.close()
            
            self.assertIsNotNone(result, "Patient not found in database")
            random_token = result[0]
            
            # Calculate expected date shift using same logic as generate_hmacdate_shift
            project_hash_root = anonymizer.config.get('projectHashRoot', '')
            data = f"{project_hash_root}".encode('utf-8')
            mac = hmac.new(random_token, data, hashlib.sha512)
            hash_hex = mac.hexdigest()
            hash_int = int(hash_hex[:16], 16)
            max_shift = anonymizer.config.get('maxDateShiftDays', 1095)
            expected_shift_days = (hash_int % max_shift) + 1  # +1 to ensure non-zero
            
            # Calculate expected shifted date
            original_date = datetime.strptime(original_acquisition_date, '%Y%m%d')
            expected_shifted_date = original_date - timedelta(days=expected_shift_days)
            expected_shifted_date_str = expected_shifted_date.strftime('%Y%m%d')
            
            self.logger.info(f"Calculated expected shift: -{expected_shift_days} days")
            self.logger.info(f"Expected AcquisitionDate: {expected_shifted_date_str}")
            
            # Verify the anonymized file
            expected_output_path = self.get_output_path_for_file(coordinator, original_file)
            self.assertIsNotNone(expected_output_path, f"Could not find output path for {original_file}")
            self.assertTrue(os.path.exists(expected_output_path), f"Anonymized file not found at expected path: {expected_output_path}")
            anonymized_ds = pydicom.dcmread(expected_output_path)
            
            # Check that the AcquisitionDate has been shifted correctly
            self.assertEqual(anonymized_ds.AcquisitionDate, expected_shifted_date_str,
                        f"AcquisitionDate should be shifted by {expected_shift_days} days: expected '{expected_shifted_date_str}', got '{anonymized_ds.AcquisitionDate}'")
            self.logger.info(f" AcquisitionDate modified: {original_acquisition_date} -> {anonymized_ds.AcquisitionDate} (shift: -{expected_shift_days} days)")
            
            self.assertEqual(anonymized_ds.LongitudinalTemporalInformationModified, 'MODIFIED',
                        f"LongitudinalTemporalInformationModified should be 'MODIFIED': expected 'MODIFIED', got '{anonymized_ds.LongitudinalTemporalInformationModified}'")
            self.logger.info(f" LongitudinalTemporalInformationModified: {anonymized_ds.LongitudinalTemporalInformationModified}")
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
        
        if os.environ.get("TEST_INITIALIZE_LLM_CACHE_FROM_TEST_DATA") == "1":
            # Download pre-populated LLM cache from test data and copy to the test output folder for use in this test
            src_cache = os.path.join(self.test_data_dir, "test_llm_cache.db")
            dst_cache = os.path.join(self.llm_cache_folder, "llm_cache.db")
            if not os.path.exists(src_cache):
                token = os.environ.get("TEST_DATA_TOKEN")
                download_github_asset_by_tag("ZentaLabs", "luwak", "testing-data", "test_llm_cache.db", src_cache, token)

            shutil.copy2(src_cache, dst_cache)
            self.logger.info(f"Initialized LLM cache from {src_cache}")

        # Create test config with basic profile (which should trigger date shifting)
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "clean_descriptors"],
            analysisCacheFolder=self.llm_cache_folder
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
            self.logger.info(f"RequestedProcedureDescription cleaned: {original_value} -> removed")
            self.assertEqual(anonymized_ds['PerformedProcedureStepDescription'].value, original_ds['PerformedProcedureStepDescription'].value,
                        "PerformedProcedureStepDescription should be empty: expected {original_ds['PerformedProcedureStepDescription'].value}, got {anonymized_ds['PerformedProcedureStepDescription'].value}")
            self.logger.info(f"PerformedProcedureStepDescription cleaned: {original_ds['PerformedProcedureStepDescription'].value} -> {anonymized_ds['PerformedProcedureStepDescription'].value}")
        finally:
            os.unlink(config_path)
            self.logger.info("Basic profile + clean descriptors test completed and config cleaned up")

    def test_generate_patient_id_method(self):
        """Test the generate_patient_id method directly to verify correct patient ID generation."""
        print("Test generate_patient_id method for patient ID consistency")
        
        # Use the first file for testing
        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "Original file `00000001.dcm` not found.")
        
        original_ds = pydicom.dcmread(original_file)
        
        # Create test config with custom patientIdPrefix
        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile"],
            patientIdPrefix="TestPatient"
        )

        try:
            self.logger.info("Starting generate_patient_id method test")
            # Initialize anonymizer to get config and logger
            anonymizer = LuwakAnonymizer(config_path)
            
            # Create a DicomProcessor instance with patient_uid_db
            processor = DicomProcessor(
                config=anonymizer.config,
                logger=anonymizer.logger,
                llm_cache=None,
                patient_uid_db=anonymizer.patient_uid_db
            )
            
            # Create mock series object with required patient attributes
            mock_series = type('MockSeries', (), {
                'original_patient_id': original_ds.PatientID,
                'original_patient_name': str(getattr(original_ds, 'PatientName', '')),
                'original_patient_birthdate': getattr(original_ds, 'PatientBirthDate', '')
            })()
            
            # Set the mock series on the processor
            processor.series = mock_series
            
            # Create mock field for PatientID tag (0010,0020)
            mock_patient_id_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'LO',
                    'tag': pydicom.tag.Tag(0x0010, 0x0020),
                    'keyword': 'PatientID',
                    'value': original_ds.PatientID
                })()
            })()

            # Create mock field for PatientName tag (0010,0010)
            mock_patient_name_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'PN',
                    'tag': pydicom.tag.Tag(0x0010, 0x0010),
                    'keyword': 'PatientName',
                    'value': str(getattr(original_ds, 'PatientName', ''))
                })()
            })()

            # Test patient ID generation - first call should create new ID
            self.logger.info(f"Testing patient ID generation for PatientID: {original_ds.PatientID}")
            patient_id_1 = processor.generate_patient_id("PatientID", "func:generate_patient_id", mock_patient_id_field, original_ds)
            patient_name_1 = processor.generate_patient_id("PatientName", "func:generate_patient_id", mock_patient_name_field, original_ds)
            # Verify format: should start with prefix and end with digits
            self.assertTrue(patient_id_1.startswith("TestPatient"), f"Patient ID should start with 'TestPatient', got '{patient_id_1}'")
            self.assertTrue(patient_id_1[11:].isdigit(), f"Patient ID should end with digits, got '{patient_id_1}'")
            self.logger.info(f" Generated patient ID and patient name: {patient_id_1} and {patient_name_1}")
            
            # Test patient ID generation - second call with same patient should return same ID
            patient_id_2 = processor.generate_patient_id("PatientID", "func:generate_patient_id", mock_patient_id_field, original_ds)
            self.assertEqual(patient_id_1, patient_id_2, f"Patient ID should be consistent: {patient_id_1} != {patient_id_2}")
            patient_name_2 = processor.generate_patient_id("PatientName", "func:generate_patient_id", mock_patient_name_field, original_ds) 
            self.assertEqual(patient_name_1, patient_name_2, f"Patient Name should be consistent: {patient_name_1} != {patient_name_2}")
            self.logger.info(f" Patient ID consistent on second call: {patient_id_2}")
            
            # Test with different patient - should generate different ID
            modified_ds = original_ds.copy()
            modified_ds.PatientID = "DIFFERENT_PATIENT_123"
            
            # Update mock series with different patient attributes
            mock_series_2 = type('MockSeries', (), {
                'original_patient_id': modified_ds.PatientID,
                'original_patient_name': str(getattr(modified_ds, 'PatientName', '')),
                'original_patient_birthdate': getattr(modified_ds, 'PatientBirthDate', '')
            })()
            processor.series = mock_series_2
            
            mock_different_patient_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'LO',
                    'tag': pydicom.tag.Tag(0x0010, 0x0020),
                    'keyword': 'PatientID',
                    'value': modified_ds.PatientID
                })()
            })()

            mock_different_patient_name_field = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'PN',
                    'tag': pydicom.tag.Tag(0x0010, 0x0010),
                    'keyword': 'PatientName',
                    'value': str(getattr(modified_ds, 'PatientName', ''))
                })()
            })()
            
            patient_id_3 = processor.generate_patient_id("PatientID", "func:generate_patient_id", mock_different_patient_field, modified_ds)
            patient_name_3 = processor.generate_patient_id("PatientName", "func:generate_patient_id", mock_different_patient_name_field, modified_ds)
            self.assertNotEqual(patient_id_3, patient_name_3, f"Patient ID and Patient Name should be different: {patient_id_3} != {patient_name_3}")
            self.assertNotEqual(patient_id_1, patient_id_3, f"Different patients should get different IDs: {patient_id_1} == {patient_id_3}")
            self.assertTrue(patient_id_3.startswith("TestPatient"), f"Patient ID should start with 'TestPatient', got '{patient_id_3}'")
            self.logger.info(f"Different patient gets different ID: {patient_id_3}")
            
            self.logger.info("All generate_patient_id method tests passed!")
            self.logger.info(f"    - Patient 1: '{patient_id_1}' (consistent across calls)")
            self.logger.info(f"    - Patient 2: '{patient_id_3}' (different patient)")

        finally:
            # Clean up config file
            os.unlink(config_path)
            self.logger.info("Generate patient ID method test completed and config cleaned up")

    def test_patient_id_with_basic_profile_recipe(self):
        """Test patient ID generation when running full anonymization with basic profile recipe (batch input)."""
        print("Test patient ID generation with basic profile recipe (batch input)")

        config_path = self.create_test_config(
            input_folder=self.limited_input_dir,
            output_folder=self.test_output_dir,
            recipes=['basic_profile'],
            patientIdPrefix="Zenta"
        )

        try:
            self.logger.info("Starting patient ID generation with basic profile recipe batch test")
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()

            # Track original patient IDs and their anonymized counterparts
            patient_id_mapping = {}
            
            for file in os.listdir(self.limited_input_dir):
                if not file.endswith(".dcm"):
                    continue
                input_file = os.path.join(self.limited_input_dir, file)
                original_ds = pydicom.dcmread(input_file)
                original_patient_id = original_ds.PatientID
                
                # Find corresponding output file using helper method
                output_file = self.get_output_path_for_file(coordinator, input_file)
                
                self.assertIsNotNone(output_file, f"Anonymized file {file} not found in output directory")
                anonymized_ds = pydicom.dcmread(output_file)
                anonymized_patient_id = anonymized_ds.PatientID
                anonymized_patient_name = anonymized_ds.PatientName
                
                # Verify patient ID was changed
                self.assertNotEqual(original_patient_id, anonymized_patient_id, 
                    f"Patient ID should be anonymized in file {file}")
                
                # Verify format
                self.assertTrue(anonymized_patient_id.startswith("Zenta"), 
                    f"Patient ID should start with 'Zenta', got '{anonymized_patient_id}' in file {file}")
                self.assertTrue(anonymized_patient_id[5:].isdigit(), 
                    f"Patient ID should end with digits, got '{anonymized_patient_id}' in file {file}")
                
                # Check consistency: same original patient should get same anonymized ID
                if original_patient_id in patient_id_mapping:
                    expected_anonymized_id = patient_id_mapping[original_patient_id]
                    self.assertEqual(anonymized_patient_id, expected_anonymized_id,
                        f"Same patient should get same anonymized ID: expected '{expected_anonymized_id}', got '{anonymized_patient_id}' in file {file}")
                    self.logger.info(f"Patient ID consistent for '{original_patient_id}': {anonymized_patient_id} (file: {file})")
                else:
                    # First time seeing this original patient ID
                    patient_id_mapping[original_patient_id] = anonymized_patient_id
                    self.logger.info(f" Patient ID anonymized: {original_patient_id} -> {anonymized_patient_id} (file: {file})")
            
            # Log summary
            self.logger.info(f"Patient ID generation batch test completed:")
            self.logger.info(f"  - Total unique original patients: {len(patient_id_mapping)}")
            self.logger.info(f"  - All patient IDs consistently anonymized across {len([f for f in os.listdir(self.limited_input_dir) if f.endswith('.dcm')])} files")
            
        finally:
            os.unlink(config_path)
            self.logger.info("Patient ID generation batch test completed and config cleaned up")


    def test_check_patient_age_method(self):
        """Test the check_patient_age method directly: 60Y and 89Y are kept, 91Y is capped to 90Y."""
        print("Test check_patient_age method: 60Y kept, 89Y kept, 91Y capped to 90Y")

        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "File `00000001.dcm` not found.")
        original_ds = pydicom.dcmread(original_file)

        config_path = self.create_test_config(
            input_folder=original_file,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "retain_patient_chars"],
        )

        try:
            self.logger.info("Starting check_patient_age method test")
            anonymizer = LuwakAnonymizer(config_path)

            processor = DicomProcessor(
                config=anonymizer.config,
                logger=anonymizer.logger,
                llm_cache=None,
            )

            # Set a mock series (required by warning logging paths in check_patient_age)
            mock_series = type('MockSeries', (), {
                'anonymized_series_uid': 'test_series_uid',
                'anonymized_study_uid': 'test_study_uid',
                'anonymized_patient_id': 'test_patient_id',
            })()
            processor.series = mock_series


            # 60Y: under threshold -> keep original
            mock_field_60 = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'AS',
                    'tag': pydicom.tag.Tag(0x0010, 0x1010),
                    'value': "060Y"
                })()
            })()
            result_60 = processor.check_patient_age(original_ds, "func:check_patient_age", mock_field_60, "item1")
            self.assertEqual(result_60, "060Y", f"60Y should be kept as '060Y', got '{result_60}'")
            self.logger.info(f"[OK] PatientAge 060Y -> {result_60} (kept)")

            # 89Y: at threshold boundary -> keep original
            mock_field_89 = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'AS',
                    'tag': pydicom.tag.Tag(0x0010, 0x1010),
                    'value': "089Y"
                })()
            })()
            result_89 = processor.check_patient_age(original_ds, "func:check_patient_age", mock_field_89, "item1")
            self.assertEqual(result_89, "089Y", f"89Y should be kept as '089Y', got '{result_89}'")
            self.logger.info(f"[OK] PatientAge 089Y -> {result_89} (kept)")

            # 91Y: over threshold -> cap to 90Y
            mock_field_91 = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'AS',
                    'tag': pydicom.tag.Tag(0x0010, 0x1010),
                    'value': "091Y"
                })()
            })()
            result_91 = processor.check_patient_age(original_ds, "func:check_patient_age", mock_field_91, "item1")
            self.assertEqual(result_91, "090Y", f"91Y should be capped to '90Y', got '{result_91}'")
            self.logger.info(f"[OK] PatientAge 091Y -> {result_91} (capped to 90Y)")

            # Empty value -> return empty string
            mock_field_empty = type('MockField', (), {
                'element': type('MockElement', (), {
                    'VR': 'AS',
                    'tag': pydicom.tag.Tag(0x0010, 0x1010),
                    'value': ""
                })()
            })()
            result_empty = processor.check_patient_age(original_ds, "func:check_patient_age", mock_field_empty, "item1")
            self.assertEqual(result_empty, "", f"Empty age should return '', got '{result_empty}'")
            self.logger.info(f"[OK] PatientAge (empty) -> '{result_empty}'")

            self.logger.info("All check_patient_age method tests passed!")
            self.logger.info(f"    - 060Y -> '{result_60}' (kept)")
            self.logger.info(f"    - 089Y -> '{result_89}' (kept)")
            self.logger.info(f"    - 091Y -> '{result_91}' (capped to 90Y)")
            self.logger.info(f"    - (empty) -> '{result_empty}'")

        finally:
            os.unlink(config_path)
            self.logger.info("check_patient_age method test completed and config cleaned up")

    def test_retain_patient_chars_recipe(self):
        """Test that basic_profile + retain_patient_chars correctly handles PatientAge and Clean tags:
        ages <= 89Y are kept unchanged, ages > 89Y are capped to 90Y."""
        print("Test patient age and clean tags with basic_profile + retain_patient_chars recipe")

        original_file = os.path.join(self.test_data_dir, "00000001.dcm")
        self.assertTrue(os.path.exists(original_file), "File `00000001.dcm` not found.")
        original_ds = pydicom.dcmread(original_file)

        # Create a temp input directory with 3 DICOM files, each with a different PatientAge
        age_input_dir = os.path.join(self.test_output_dir, "age_test_input")
        os.makedirs(age_input_dir, exist_ok=True)

        # (filename, age set on input file, expected age in output)
        test_cases = [
            ("age_60.dcm", "060Y", "060Y"),  # 60Y <= 89 -> kept
            ("age_89.dcm", "089Y", "089Y"),  # 89Y == 89 -> kept
            ("age_91.dcm", "091Y", "090Y"),   # 91Y > 89 -> capped to 90Y
        ]

        for filename, input_age, _ in test_cases:
            ds = original_ds.copy()
            ds.PatientAge = input_age
            ds.PreMedication = "Jonny"  # VR=LO, example value
            ds.SpecialNeeds = "None"    # VR=LO, example value
            ds.save_as(os.path.join(age_input_dir, filename))
            self.logger.info(f"Created test file {filename} with PatientAge={input_age}, PreMedication={ds.PreMedication}, SpecialNeeds={ds.SpecialNeeds}")
        config_path = self.create_test_config(
            input_folder=age_input_dir,
            output_folder=self.test_output_dir,
            recipes=["basic_profile", "retain_patient_chars"],
            analysisCacheFolder=self.llm_cache_folder
        )

        try:
            self.logger.info("Starting patient age integration test")
            anonymizer = LuwakAnonymizer(config_path)
            coordinator = anonymizer.anonymize()

            for filename, input_age, expected_age in test_cases:
                input_file = os.path.join(age_input_dir, filename)
                output_file = self.get_output_path_for_file(coordinator, input_file)

                self.assertIsNotNone(output_file, f"Anonymized file {filename} not found in output")
                anon_ds = pydicom.dcmread(output_file)

                actual_age = str(anon_ds.PatientAge) if hasattr(anon_ds, 'PatientAge') and anon_ds.PatientAge else ""
                self.assertEqual(actual_age, expected_age,
                    f"PatientAge for {filename}: expected '{expected_age}', got '{actual_age}'")
                self.logger.info(f"[OK] PatientAge {input_age} -> {actual_age} (expected: {expected_age})")

                # Check that PreMedication and SpecialNeeds are removed after deidentification using assertNotIn
                self.assertNotIn(pydicom.tag.Tag(0x00400012), anon_ds,
                    f"Unexpected tag PreMedication (0040,0012) found in file {filename} after deidentification.")
                self.assertNotIn(pydicom.tag.Tag(0x00380050), anon_ds,
                    f"Unexpected tag SpecialNeeds (0038,0050) found in file {filename} after deidentification.")

            self.logger.info("Patient age integration test completed successfully!")

        finally:
            os.unlink(config_path)
            if os.path.exists(age_input_dir):
                shutil.rmtree(age_input_dir)
            self.logger.info("Patient age integration test completed and config cleaned up")


if __name__ == "__main__":
    unittest.main()

