#!/usr/bin/env python

"""
DICOM Private Tag Registry and Management.

This module provides utilities for managing and registering private DICOM tags,
including tag conversion, keyword generation, and CSV-based tag registration.
"""

import re
import csv
from pydicom.datadict import add_private_dict_entry

# Import the centralized logger
from ..logging.luwak_logger import get_logger


def tag_str_to_int(group, element):
    """
    Convert a DICOM tag string like (0010,xx10) to an integer tag value.
    
    Args:
        group (str): DICOM group in the format 'GGGG'.
        element (str): DICOM element in the format 'xxEE'.

    Returns:
        int: Integer representation of the DICOM tag.
    """
    logger = get_logger('tag_str_to_int')
    
    try:
        group = int(group, 16)
        if str(element).startswith('xx'):
            element_int = int(str(element)[2:], 16)
        else:
            element_int = int(element, 16)
    except ValueError as e:
        logger.error(f"Invalid tag format: ({group},{element}) - {e}")
        raise ValueError(f"Invalid tag format: ({group},{element})")
    return (group << 16) | element_int


def name_to_keyword(name):
    """
    Convert a descriptive name string to a valid DICOM keyword.
    
    Args:
        name (str): The descriptive name to convert (e.g., 'Patient Age (years)').
    
    Returns:
        str: DICOM keyword (e.g., 'PatientAgeYears').
    """
    # Remove non-alphanumeric characters, except spaces
    cleaned = re.sub(r'[^0-9a-zA-Z ]+', '', name)
    # Split by spaces, capitalize each word, and join
    keyword = ''.join(word.capitalize() for word in cleaned.split())
    # Ensure it starts with a letter (prepend 'X' if not)
    if keyword and not keyword[0].isalpha():
        keyword = 'X' + keyword
    return keyword


def register_private_tags_from_csv(csv_path):
    """
    Register private DICOM tags from a CSV file.
    
    Args:
        csv_path (str): Path to the CSV file containing private tag definitions. The CSV should have at least five columns: tag_str, private_creator, vr, vm, description.
            - tag_str: DICOM tag string in the format '(GGGG,xxEE)', where 'xx' is a placeholder for the private block value.
            - private_creator: Name of the private creator.
            - vr: Value Representation (e.g., 'LO', 'CS').
            - vm: Value Multiplicity (e.g., '1', '1-n').
            - description: Description of the tag.
    
    Returns:
        None
    """
    logger = get_logger('register_private_tags')
    logger.debug(f"Loading private tags from: {csv_path}")
    
    tag_count = 0
    with open(csv_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Skip header if present
        for row in reader:
            if not row or len(row) < 6 or all(not cell.strip() for cell in row):
                continue  # Skip empty or incomplete rows
            group, element, private_creator, vr, vm, description = row[:6]
            try:
                tag = tag_str_to_int(group, element)
                description = name_to_keyword(description)
                add_private_dict_entry(private_creator, tag, vr, description, vm)
                tag_count += 1
            except Exception as e:
                logger.warning(f"Skipping row {row}: {e}")
                continue
    
    logger.info(f"Successfully registered {tag_count} private DICOM tags")