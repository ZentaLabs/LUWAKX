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
            "recipesFolder": "./recipes",
            "recipes": ["basic_profile"],
            "maxDateShiftDays": 1095,
            "excludedTagsFromParquet": ["(7FE0,0010)"],
            "outputFolderHierarchy": "copy_from_input",
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
        config_path = self.make_config({"recipesFolder": "./test_recipes"})
        anonymizer = LuwakAnonymizer(config_path)
        expected = os.path.abspath(os.path.join(os.path.dirname(config_path), "test_recipes"))
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
        self.assertIn((0x0010 << 16) | 0x0010, anonymizer.excluded_tags_from_parquet)

    def test_output_folder_hierarchy(self):
        config_path = self.make_config({"outputFolderHierarchy": "flat"})
        anonymizer = LuwakAnonymizer(config_path)
        self.assertEqual(anonymizer.config["outputFolderHierarchy"], "flat")

    def test_project_hash_root(self):
        config_path = self.make_config({"projectHashRoot": "mytestroot"})
        anonymizer = LuwakAnonymizer(config_path)
        self.assertEqual(anonymizer.config["projectHashRoot"], "mytestroot")

if __name__ == "__main__":
    unittest.main()
