#!/usr/bin/env python

# Test the shared logger setup

import sys
import os
sys.path.append('luwakx')

# Test 1: Import luwakx and setup logger
from luwakx.luwakx import setup_global_logger, get_logger

# Setup logger with DEBUG level
logger = setup_global_logger(log_level='DEBUG', log_file='test_luwak.log')

logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")

# Test 2: Import anonymizer (should use the same logger)
try:
    from luwakx.anonymize import get_anonymizer_logger
    anon_logger = get_anonymizer_logger()
    
    anon_logger.info("Anonymizer logger is working!")
    
    # Check if it's the same logger
    print(f"Same logger instance: {logger is anon_logger}")
    
except Exception as e:
    print(f"Error testing anonymizer logger: {e}")

print("Test completed. Check test_luwak.log for output.")
