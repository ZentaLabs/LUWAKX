import unittest
import os
import json
import tempfile
import sys
# Add luwakx directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
from anonymize import LuwakAnonymizer

class TestConfigOptions(unittest.TestCase):
    """Test suite for each config option in luwak-config.json."""

    def setUp(self):
        # Minimal valid config for required fields
        self.base_config = {
            "inputFolder": "./inputs",
            "outputDeidentifiedFolder": "./outputs/deidentified",
            "outputPrivateMappingFolder": "./outputs/privateMapping",
            "recipesFolder": "./outputs/recipes",
            "recipes": ["basic_profile"],
            "maxDateShiftDays": 1095,
            "excludedTagsFromParquet": ["(7FE0,0010)"],
            "projectHashRoot": "testhashroot"
        }
        self.tempfiles = []

    def tearDown(self):
        for f in self.tempfiles:
            if os.path.exists(f):
                os.unlink(f)

    def make_config(self, overrides=None):
        config = self.base_config.copy()
        if overrides:
            config.update(overrides)
        tf = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(config, tf)
        tf.close()
        self.tempfiles.append(tf.name)
        return tf.name

    def test_input_folder(self):
        config_path = self.make_config({"inputFolder": "./inputs_test"})
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "inputs_test"))
        self.assertEqual(anonymizer.config["inputFolder"], expected)

    def test_output_deidentified_folder(self):
        config_path = self.make_config({"outputDeidentifiedFolder": "./outputs/test_deid"})
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "outputs/test_deid"))
        self.assertEqual(anonymizer.config["outputDeidentifiedFolder"], expected)

    def test_output_private_mapping_folder(self):
        config_path = self.make_config({"outputPrivateMappingFolder": "./outputs/test_private"})
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "outputs/test_private"))
        self.assertEqual(anonymizer.config["outputPrivateMappingFolder"], expected)

    def test_recipes_folder(self):
        config_path = self.make_config({"recipesFolder": "./output/test_recipes"})
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "output/test_recipes"))        
        self.assertEqual(anonymizer.config["recipesFolder"], expected)

    def test_recipes(self):
        config_path = self.make_config({"recipes": ["basic_profile", "retain_safe_private_tags"]})
        anonymizer = LuwakAnonymizer(config_path)
        self.assertIn("basic_profile", anonymizer.config["recipes"])
        self.assertIn("retain_safe_private_tags", anonymizer.config["recipes"])

    def test_max_date_shift_days(self):
        config_path = self.make_config({"maxDateShiftDays": 365})
        anonymizer = LuwakAnonymizer(config_path)
        self.assertEqual(anonymizer.config["maxDateShiftDays"], 365)

    def test_excluded_tags_from_parquet(self):
        config_path = self.make_config({"excludedTagsFromParquet": ["(0010,0010)"]})
        anonymizer = LuwakAnonymizer(config_path)
        # Verify config contains the excluded tag
        self.assertIn("(0010,0010)", anonymizer.config["excludedTagsFromParquet"])

    def test_project_hash_root(self):
        config_path = self.make_config({"projectHashRoot": "mytestroot"})
        anonymizer = LuwakAnonymizer(config_path)
        self.assertEqual(anonymizer.config["projectHashRoot"], "mytestroot")

    def test_manually_revised_tags_standard(self):
        config_path = self.make_config({"customTags": {"standard": "./data/custom_standard.csv"}})
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "data/custom_standard.csv"))
        self.assertEqual(anonymizer.config["customTags"]["standard"], expected)

    def test_manually_revised_tags_private(self):
        config_path = self.make_config({"customTags": {"private": "./data/custom_private.csv"}})
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "data/custom_private.csv"))
        self.assertEqual(anonymizer.config["customTags"]["private"], expected)

    def test_manually_revised_tags_both(self):
        config_path = self.make_config({
            "customTags": {
                "standard": "./data/custom_standard.csv",
                "private": "./data/custom_private.csv"
            }
        })
        anonymizer = LuwakAnonymizer(config_path)
        expected_standard = os.path.abspath(os.path.join(os.path.dirname(config_path), "data/custom_standard.csv"))
        expected_private = os.path.abspath(os.path.join(os.path.dirname(config_path), "data/custom_private.csv"))
        self.assertEqual(anonymizer.config["customTags"]["standard"], expected_standard)
        self.assertEqual(anonymizer.config["customTags"]["private"], expected_private)

    def test_llm_cache_folder(self):
        # Include clean_descriptors recipe so llmCacheFolder is resolved
        config_path = self.make_config({
            "llmCacheFolder": "./cache/llm",
            "recipes": ["basic_profile", "clean_descriptors"]
        })
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "cache/llm"))
        self.assertEqual(anonymizer.config["llmCacheFolder"], expected)

    def test_test_options(self):
        # Only use allowed properties per schema (useExistingMaskDefacer)
        test_opts = {"useExistingMaskDefacer": ["/path/to/mask1.nii.gz", "/path/to/mask2.nii.gz"]}
        config_path = self.make_config({"testOptions": test_opts})
        anonymizer = LuwakAnonymizer(config_path)
        self.assertEqual(anonymizer.config["testOptions"]["useExistingMaskDefacer"], ["/path/to/mask1.nii.gz", "/path/to/mask2.nii.gz"])

    def test_patient_id_prefix_custom(self):
        config_path = self.make_config({"patientIdPrefix": "CustomPrefix"})
        anonymizer = LuwakAnonymizer(config_path)
        self.assertEqual(anonymizer.config["patientIdPrefix"], "CustomPrefix")

    def test_patient_id_prefix_default(self):
        # When not specified, should default to 'Zenta'
        config_path = self.make_config()
        anonymizer = LuwakAnonymizer(config_path)
        # Check that patient_uid_db is initialized with default prefix
        self.assertIsNotNone(anonymizer.patient_uid_db)
        self.assertEqual(anonymizer.patient_uid_db.patient_id_prefix, "Zenta")

    def test_patient_uid_database_path(self):
        # Test relative path resolution for persistent patient UID database
        config_path = self.make_config({"patientUidDatabasePath": "./persistent/patient_uid.db"})
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "persistent/patient_uid.db"))
        self.assertEqual(anonymizer.config["patientUidDatabasePath"], expected)
        self.assertTrue(anonymizer.persistent_uid_db, "Should be marked as persistent database")

if __name__ == "__main__":
    unittest.main()
