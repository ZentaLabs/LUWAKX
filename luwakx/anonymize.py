print("start")

#!/usr/bin/env python

import subprocess
import sys
import os
import argparse

def setup_deid_repo():
    repo_url = "https://github.com/Simlomb/deid.git"
    branch = "enhversion"
    repo_dir = os.path.expanduser("~/deid")  # Set repo_dir to the home directory

    # Check if the repository is already cloned
    if not os.path.exists(repo_dir):
        print("Cloning deid repository...")
        subprocess.check_call(["git", "clone", "--branch", branch, repo_url, repo_dir])
    else:
        # Check if the repository is already up-to-date
        print("Checking for updates in deid repository...")
        subprocess.check_call(["git", "-C", repo_dir, "fetch"])
        status = subprocess.check_output(["git", "-C", repo_dir, "status", "--porcelain", "-b"])
        if b"behind" in status:
            print("Updating deid repository...")
            subprocess.check_call(["git", "-C", repo_dir, "pull"])

    # Check if the repository is installed
    try:
        import deid
    except ImportError:
        print("Installing deid repository...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", repo_dir])

# Call the setup function before importing deid
setup_deid_repo()

from deid.config import DeidRecipe
from deid.dicom import get_files, get_identifiers, replace_identifiers

def is_tag_private(dicom, value, field, item):
    return field.element.is_private and (field.element.private_creator is not None)


def main():
    parser = argparse.ArgumentParser(description="Anonymize DICOM files using deid recipes.")
    parser.add_argument(
        "--base", 
        default="/path/to/default/input", 
        help="Base directory or single file containing input DICOM files. Default: /path/to/default/input"
    )
    parser.add_argument(
        "--output", 
        default="~/luwak_output_files", 
        help="Output directory for anonymized DICOM files. Default: ~/luwak_output_files"
    )
    parser.add_argument(
        "--deid_recipe", 
        default="deid.dicom", 
        help="Path to the deid recipe. Default: deid.dicom"
    )
    parser.add_argument(
        "--safe_private_tags", 
        default="./scripts/anonymization_recipes/deid.dicom.safe-private-tags", 
        help="Path to the deid.dicom.safe-private-tags recipe. Default: ./scripts/anonymization_recipes/deid.dicom.safe-private-tags"
    )
    parser.add_argument(
        "--retain_safe_private_tags", 
        default="True", 
        help="Whether to retain safe private tags. Default: True"
    )
    args = parser.parse_args()

    base = args.base
    output = args.output

    # Expand user directory for output path
    output = os.path.expanduser(output)

    # Handle single file or directory input
    if os.path.isfile(base):
        dicom_files = [base]
    elif os.path.isdir(base):
        # Recursively get all files in the directory and subdirectories
        dicom_files = [os.path.join(root, file) for root, _, files in os.walk(base) for file in files]
    else:
        print(f"WARNING: Cannot read input file or directory '{base}', skipping.")
        return
    #BUG: Check the output file name, if two input folders have files with same names, output file will be overwritten 
    # Prepare output directory
    if not os.path.exists(output):
        print(f"Output directory '{output}' does not exist. Creating it...")
        os.makedirs(output, exist_ok=True)
    
    items = get_identifiers(dicom_files)

    # Create the deid recipe
    if args.retain_safe_private_tags.lower() == "true":
        remove_other_private_tags = "./scripts/anonymization_recipes/deid.dicom.remove-private-tags"
        recipe = DeidRecipe(deid=[args.safe_private_tags, remove_other_private_tags], base=True, default_base=args.deid_recipe)
        remove_private=False
    else:
        recipe = DeidRecipe(deid=args.deid_recipe)
        remove_private=True 


    for item in items:
        items[item]["is_private"] = is_tag_private

    parsed_files = replace_identifiers(
        dicom_files=dicom_files, deid=recipe, strip_sequences=False,
        ids=items,
        remove_private=remove_private,
        save=True, output_folder=output,
        overwrite=True
    )


if __name__ == "__main__":
    main()

print("end of anonymization action")