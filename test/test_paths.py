#!/usr/bin/env python3

import unittest
import os
import sys
# Add luwakx directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
from luwak_logger import setup_logger, get_logger

class TestPaths(unittest.TestCase):
    """Test that all paths are correctly resolved when test is moved to test directory."""

    def test_path_resolution(self):
        """Test that all required paths exist and are correctly resolved."""
        print("Testing path resolution from test directory")
        
        # Test the paths that would be used in the test (from test directory)
        test_dir = os.path.dirname(__file__)
        # print(f"Test directory: {test_dir}")

        # Path to parent directory (luwak)
        parent_dir = os.path.dirname(test_dir)
        # print(f"Parent directory: {parent_dir}")

        # Path to luwakx directory
        luwakx_dir = os.path.join(parent_dir, "luwakx")
        # print(f"luwakx directory: {luwakx_dir}")

        # Path to anonymize.py
        anonymize_py = os.path.join(luwakx_dir, "anonymize.py")
        # print(f"anonymize.py path: {anonymize_py}")

        # Path to data directory containing recipe files
        data_dir = os.path.join(luwakx_dir, "data", "BurnedPixelLocation")
        # print(f"data directory: {data_dir}")

        # Path to TagsArchive directory  
        tags_archive_dir = os.path.join(luwakx_dir, "data", "TagsArchive")
        # print(f"TagsArchive directory: {tags_archive_dir}")

        # Path to luwakx.py (using the test's approach)
        luwakx_py = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "luwakx.py")
        # print(f"luwakx.py path: {luwakx_py}")

        # Setup logger for this test
        log_dir = os.path.join(parent_dir, "test", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "test_paths.log")
        setup_logger(log_file, console_output=False)
        logger = get_logger("test_paths")
        # Only resolve and log the logger file path, do not assert existence

        self.assertTrue(os.path.exists(parent_dir), "Parent directory should exist")
        self.assertTrue(os.path.exists(luwakx_dir), "luwakx directory should exist")
        self.assertTrue(os.path.exists(anonymize_py), "anonymize.py should exist")
        self.assertTrue(os.path.exists(data_dir), "data/BurnedPixelLocation directory should exist")
        
        # Verify data directory contains expected recipe files
        if os.path.exists(data_dir):
            data_files = os.listdir(data_dir)
            # print(f"Files in data directory: {data_files}")
            self.assertIn('deid.dicom.burnedin-pixel-recipe', data_files, "burnedin-pixel-recipe should exist")

        self.assertTrue(os.path.exists(tags_archive_dir), "TagsArchive directory should exist")
        
        # Verify TagsArchive contains template files
        if os.path.exists(tags_archive_dir):
            archive_files = os.listdir(tags_archive_dir)
            # print(f"Files in TagsArchive: {archive_files}")
            self.assertIn('private_tags_template.csv', archive_files, "private_tags_template.csv should exist")
            self.assertIn('standard_tags_template.csv', archive_files, "standard_tags_template.csv should exist")

        self.assertTrue(os.path.exists(luwakx_py), "luwakx.py should exist")

    def test_import_resolution(self):
        """Test that the LuwakAnonymizer can be imported correctly."""
        print("Testing import resolution")
        
        # Test import
        sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
        try:
            from anonymize import LuwakAnonymizer
            # print("Import successful: LuwakAnonymizer imported")
            self.assertTrue(True, "LuwakAnonymizer should be importable")
        except ImportError as e:
            self.fail(f"Import failed: {e}")

    def test_working_directory_independence(self):
        """Test that paths work regardless of current working directory."""
        print("Testing working directory independence")
        
        current_cwd = os.getcwd()
        # print(f"Current working directory: {current_cwd}")
        
        # Test that all paths are absolute and don't depend on cwd
        luwakx_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx")
        self.assertTrue(os.path.isabs(luwakx_dir), "luwakx path should be absolute")
        
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "data", "BurnedPixelLocation")
        self.assertTrue(os.path.isabs(data_dir), "data path should be absolute")
        
        tags_archive_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "data", "TagsArchive")
        self.assertTrue(os.path.isabs(tags_archive_dir), "TagsArchive path should be absolute")
        
        luwakx_py = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "luwakx.py")
        self.assertTrue(os.path.isabs(luwakx_py), "luwakx.py path should be absolute")


if __name__ == "__main__":
    unittest.main()
