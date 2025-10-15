#!/usr/bin/env python

"""
Custom exceptions for the Luwak anonymization system.

This module contains all custom exception classes used throughout the Luwak project,
providing centralized error handling with consistent formatting and context.
"""


class ConfigurationError(Exception):
    """Custom exception for configuration file errors with filename context."""
    
    def __init__(self, message, filename=None, original_exception=None):
        """Initialize configuration error with context.
        
        Args:
            message (str): Error description
            filename (str): Path to configuration file that caused the error
            original_exception (Exception): Original exception that was caught
        """
        self.message = message
        self.filename = filename
        self.original_exception = original_exception
        super().__init__(message)
    
    def __str__(self):
        """Return formatted error message including filename context."""
        if self.filename:
            base_msg = f"Configuration error in '{self.filename}': {self.message}"
        else:
            base_msg = f"Configuration error: {self.message}"
        
        if self.original_exception:
            base_msg += f" (Original error: {self.original_exception})"
        
        return base_msg