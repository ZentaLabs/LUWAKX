#!/usr/bin/env python
"""
Luwak Logging Module

Provides centralized logging configuration for the Luwak DICOM anonymization project.
This module handles logger setup, configuration, and provides a consistent logging
interface across all Luwak modules.

Usage:
    from luwak_logger import setup_logger, get_logger
    
    # Setup logger with configuration
    setup_logger(log_level='INFO', log_file='luwak.log', console_output=True)
    
    # Get logger in any module
    logger = get_logger(__name__)
    logger.info("Processing started")
"""

import logging
import os
import sys
from typing import Optional

# Global logger registry
_loggers = {}
_logger_configured = False
_default_config = {
    'log_level': 'INFO',
    'log_file': None,
    'console_output': False,
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S'
}

PRIVATE_LEVEL = 5
logging.addLevelName(PRIVATE_LEVEL, "PRIVATE")

def private(self, message, *args, **kwargs):
    if self.isEnabledFor(PRIVATE_LEVEL):
        self._log(PRIVATE_LEVEL, message, args, **kwargs)
logging.Logger.private = private


def setup_logger(log_level: str = 'INFO', 
                log_file: Optional[str] = None, 
                console_output: bool = False,
                log_format: Optional[str] = None,
                date_format: Optional[str] = None) -> None:
    """
    Configure the global logging system for Luwak.
    
    Args:
        log_level (str): Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
        log_file (str, optional): Path to log file. If None, no file logging.
        console_output (bool): Whether to output logs to console
        log_format (str, optional): Custom log format string
        date_format (str, optional): Custom date format string
        
    Returns:
        None
        
    Note:
        This function should be called once at the start of the application.
        Subsequent calls will reconfigure the existing loggers.
    """
    global _logger_configured, _default_config
    
    # Update default configuration
    _default_config.update({
        'log_level': log_level.upper(),
        'log_file': log_file,
        'console_output': console_output,
        'format': log_format or _default_config['format'],
        'date_format': date_format or _default_config['date_format']
    })
    
    # Convert string level to logging constant
    if _default_config['log_level'] == 'PRIVATE':
        numeric_level = PRIVATE_LEVEL
    else:
        numeric_level = getattr(logging, _default_config['log_level'], logging.INFO)
    
    # Get root logger for Luwak
    root_logger = logging.getLogger('luwak')
    root_logger.setLevel(numeric_level)
    
    # Properly close existing handlers before clearing to avoid resource warnings
    for handler in root_logger.handlers[:]:  # Copy list to avoid modification during iteration
        if hasattr(handler, 'close'):
            handler.close()
        root_logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt=_default_config['format'],
        datefmt=_default_config['date_format']
    )
    
    # Setup console handler
    if _default_config['console_output']:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Setup file handler
    if _default_config['log_file']:
        try:
            # Create log directory if it doesn't exist
            log_dir = os.path.dirname(_default_config['log_file'])
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            
            file_handler = logging.FileHandler(_default_config['log_file'])
            file_handler.setLevel(numeric_level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            # Fall back to console only if file logging fails
            print(f"Warning: Could not setup file logging to {_default_config['log_file']}: {e}")
    
    # Prevent propagation to avoid duplicate messages
    root_logger.propagate = False
    
    _logger_configured = True
    
    # Log the configuration
    logger = get_logger('luwak_logger')
    logger.info(f"Logging configured - Level: {_default_config['log_level']}")
    if _default_config['log_file']:
        logger.info(f"Log file: {_default_config['log_file']}")
    if _default_config['console_output']:
        logger.debug("Console logging enabled")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified module.
    
    Args:
        name (str): Logger name, typically __name__ from the calling module
        
    Returns:
        logging.Logger: Configured logger instance
        
    Note:
        If setup_logger() hasn't been called, this will create a basic fallback logger.
        The logger name will be prefixed with 'luwak.' for consistency.
    """
    global _loggers, _logger_configured
    
    # Ensure name is prefixed with 'luwak.'
    if not name.startswith('luwak.'):
        if name == '__main__' or name.endswith('.py'):
            # Extract module name from file path
            module_name = os.path.splitext(os.path.basename(name))[0]
            logger_name = f'luwak.{module_name}'
        else:
            logger_name = f'luwak.{name}'
    else:
        logger_name = name
    
    # Return existing logger if available
    if logger_name in _loggers:
        return _loggers[logger_name]
    
    # Create new logger
    logger = logging.getLogger(logger_name)
    
    # If global logging not configured, setup basic fallback
    if not _logger_configured:
        _setup_fallback_logger()
    
    # Cache the logger
    _loggers[logger_name] = logger
    
    return logger


def _setup_fallback_logger() -> None:
    """
    Setup a basic fallback logger configuration.
    
    This is used when get_logger() is called before setup_logger().
    Creates a simple console-only logger with INFO level.
    """
    global _logger_configured
    
    if _logger_configured:
        return
    
    # Setup basic configuration
    setup_logger(
        log_level='INFO',
        log_file=None,
        console_output=False
    )



