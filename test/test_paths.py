#!/usr/bin/env python3

import unittest
import os
import sys

class TestPaths(unittest.TestCase):
    """Test that all paths are correctly resolved when test is moved to test directory."""

    def test_path_resolution(self):
        """Test that all required paths exist and are correctly resolved."""
        print("Testing path resolution from test directory")
        
        # Test the paths that would be used in the test (from test directory)
        test_dir = os.path.dirname(__file__)
        print(f"Test directory: {test_dir}")

        # Path to parent directory (luwak)
        parent_dir = os.path.dirname(test_dir)
        print(f"Parent directory: {parent_dir}")
        self.assertTrue(os.path.exists(parent_dir), "Parent directory should exist")

        # Path to luwakx directory
        luwakx_dir = os.path.join(parent_dir, "luwakx")
        print(f"luwakx directory: {luwakx_dir}")
        self.assertTrue(os.path.exists(luwakx_dir), "luwakx directory should exist")

        # Path to anonymize.py
        anonymize_py = os.path.join(luwakx_dir, "anonymize.py")
        print(f"anonymize.py path: {anonymize_py}")
        self.assertTrue(os.path.exists(anonymize_py), "anonymize.py should exist")

        # Path to scripts directory (from test directory perspective, corrected)
        scripts_dir_correct = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "scripts", "anonymization_recipes")
        print(f"scripts directory (correct): {scripts_dir_correct}")
        self.assertTrue(os.path.exists(scripts_dir_correct), "scripts directory should exist")
        
        # Verify scripts directory contains expected files
        if os.path.exists(scripts_dir_correct):
            script_files = os.listdir(scripts_dir_correct)
            print(f"Files in scripts directory: {script_files}")
            self.assertIn('deid.dicom.remove-private-tags', script_files, "remove-private-tags recipe should exist")
            self.assertIn('deid.dicom.safe-private-tags', script_files, "safe-private-tags recipe should exist")

        # Path to luwakx.py (using the test's approach)
        luwakx_py = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "luwakx.py")
        print(f"luwakx.py path: {luwakx_py}")
        self.assertTrue(os.path.exists(luwakx_py), "luwakx.py should exist")

    def test_import_resolution(self):
        """Test that the LuwakAnonymizer can be imported correctly."""
        print("Testing import resolution")
        
        # Test import
        sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'luwakx'))
        try:
            from anonymize import LuwakAnonymizer
            print("Import successful: LuwakAnonymizer imported")
            self.assertTrue(True, "LuwakAnonymizer should be importable")
        except ImportError as e:
            self.fail(f"Import failed: {e}")

    def test_working_directory_independence(self):
        """Test that paths work regardless of current working directory."""
        print("Testing working directory independence")
        
        current_cwd = os.getcwd()
        print(f"Current working directory: {current_cwd}")
        
        # Test that all paths are absolute and don't depend on cwd
        luwakx_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx")
        self.assertTrue(os.path.isabs(luwakx_dir), "luwakx path should be absolute")
        
        scripts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "scripts", "anonymization_recipes")
        self.assertTrue(os.path.isabs(scripts_dir), "scripts path should be absolute")
        
        luwakx_py = os.path.join(os.path.dirname(os.path.dirname(__file__)), "luwakx", "luwakx.py")
        self.assertTrue(os.path.isabs(luwakx_py), "luwakx.py path should be absolute")


if __name__ == "__main__":
    unittest.main()
