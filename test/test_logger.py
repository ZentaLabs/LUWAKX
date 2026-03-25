#!/usr/bin/env python3
"""
Test script to verify the new luwak_logger system works correctly.
"""

import unittest
import sys
import os
import tempfile

from luwakx.logging.luwak_logger import setup_logger, get_logger


class TestLuwakLogger(unittest.TestCase):
    """Test cases for the luwak_logger module."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create a temporary directory for log files
        self.temp_dir = tempfile.mkdtemp()
        self.test_log_file = os.path.join(self.temp_dir, 'test_luwak.log')

    def tearDown(self):
        """Clean up after each test."""
        # Remove test log file if it exists
        if os.path.exists(self.test_log_file):
            os.remove(self.test_log_file)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    def test_basic_logger_functionality(self):
        """Test basic logger functionality with fallback configuration."""
        # Test getting logger before setup (should create fallback)
        logger1 = get_logger('test_module')
        
        # This should not raise an exception
        logger1.info("This is a test message from logger1")
        
        # Verify logger was created
        self.assertIsNotNone(logger1)
        self.assertTrue(hasattr(logger1, 'info'))

    def test_configured_logger(self):
        """Test configured logger functionality with file output."""
        # Setup logger with specific configuration
        setup_logger(
            log_level='DEBUG',
            log_file=self.test_log_file,
            console_output=False
        )
        
        # Test multiple loggers
        logger_main = get_logger('main')
        logger_sub = get_logger('submodule')
        
        # Test different log levels
        logger_main.debug("Debug message from main")
        logger_main.info("Info message from main")
        logger_main.warning("Warning message from main")
        
        logger_sub.info("Info message from submodule")
        logger_sub.error("Error message from submodule")
        
        # Verify loggers were created
        self.assertIsNotNone(logger_main)
        self.assertIsNotNone(logger_sub)
        
        # Test that log file was created and contains content
        self.assertTrue(os.path.exists(self.test_log_file), "Log file should be created")
        
        with open(self.test_log_file, 'r') as f:
            log_content = f.read()
            self.assertGreater(len(log_content), 0, "Log file should contain content")
            self.assertIn("Debug message from main", log_content)
            self.assertIn("Info message from submodule", log_content)

    def test_logger_hierarchy(self):
        """Test that logger hierarchy and naming works correctly."""
        # Setup logger first
        setup_logger(log_level='INFO', console_output=False)
        
        # Test different logger naming patterns
        logger_main = get_logger(__name__)
        logger_module = get_logger('test_module')
        logger_file = get_logger('test_file.py')
        
        # Test that they all work without errors
        logger_main.info("Message from __name__ logger")
        logger_module.info("Message from module logger") 
        logger_file.info("Message from file logger")
        
        # Verify all loggers were created
        self.assertIsNotNone(logger_main)
        self.assertIsNotNone(logger_module)
        self.assertIsNotNone(logger_file)

    def test_anonymize_import(self):
        """Test that anonymize.py can import and use the logger."""
        try:
            # This should work without errors now
            from luwakx.anonymize import LuwakAnonymizer, register_private_tags_from_csv
            
            # Test that we can get a logger
            logger = get_logger('test_anonymize')
            logger.info("Logger from anonymize test works!")
            
            # Verify import was successful
            self.assertIsNotNone(LuwakAnonymizer)
            self.assertIsNotNone(register_private_tags_from_csv)
            
        except ImportError as e:
            self.fail(f"Failed to import anonymize module: {e}")

    def test_luwakx_import(self):
        """Test that luwakx.py can import and use the logger."""
        try:
            # This should work without errors now
            import luwakx.luwakx as luwakx_module
            
            # Verify import was successful
            self.assertIsNotNone(luwakx_module)
            
        except ImportError as e:
            self.fail(f"Failed to import luwakx module: {e}")

    def test_private_log_level(self):
        """Test that PRIVATE log level works and logs sensitive information."""
        # Setup logger with PRIVATE level to capture all messages
        setup_logger(
            log_level='PRIVATE',
            log_file=self.test_log_file,
            console_output=False
        )
        
        # Test getting a logger and using the private method
        logger = get_logger('test_private')
        
        # Test private logging directly
        logger.private("This is a PRIVATE message with sensitive data")
        logger.info("This is a regular INFO message")
        logger.debug("This is a DEBUG message")
        
        # Verify log file was created and contains the private message
        self.assertTrue(os.path.exists(self.test_log_file), "Log file should be created")
        
        with open(self.test_log_file, 'r') as f:
            log_content = f.read()
            self.assertGreater(len(log_content), 0, "Log file should contain content")
            self.assertIn("PRIVATE message with sensitive data", log_content)
            self.assertIn("regular INFO message", log_content)
            self.assertIn("DEBUG message", log_content)
            # Check that PRIVATE level appears in the log
            self.assertIn("PRIVATE", log_content)

    def test_private_level_filtering(self):
        """Test that PRIVATE messages are filtered out when log level is higher."""
        # Setup logger with INFO level (should NOT show PRIVATE messages)
        setup_logger(
            log_level='INFO',
            log_file=self.test_log_file,
            console_output=False
        )
        
        # Test getting a logger and using different levels
        logger = get_logger('test_private_filter')
        
        # Log messages at different levels
        logger.private("This PRIVATE message should NOT appear")
        logger.debug("This DEBUG message should NOT appear")
        logger.info("This INFO message SHOULD appear")
        logger.warning("This WARNING message SHOULD appear")
        
        # Verify log file filtering
        self.assertTrue(os.path.exists(self.test_log_file), "Log file should be created")
        
        with open(self.test_log_file, 'r') as f:
            log_content = f.read()
            self.assertGreater(len(log_content), 0, "Log file should contain content")
            # Should NOT contain PRIVATE or DEBUG messages
            self.assertNotIn("PRIVATE message should NOT appear", log_content)
            self.assertNotIn("DEBUG message should NOT appear", log_content)
            # Should contain INFO and WARNING messages
            self.assertIn("INFO message SHOULD appear", log_content)
            self.assertIn("WARNING message SHOULD appear", log_content)


if __name__ == "__main__":
    unittest.main()
