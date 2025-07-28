print("start")

#!/usr/bin/env python

from glob import glob
import os

from deid.config import DeidRecipe
from deid.data import get_dataset
from deid.dicom import get_files, get_identifiers, replace_identifiers
from deid.tests.common import create_recipe

# This is supported for deid.dicom version 0.1.34

#base = r"c:\tmp\midi-b"
base = r"c:\tmp\midi-b\input_data\eigen"
output = r"c:\tmp\midi-b\public"
#dicom_files = [f for f in glob(f"{base}/**/*", recursive=True) if os.path.isfile(f)]
dicom_files = list(get_files(base))
#print(dicom_files)

items = get_identifiers(dicom_files)

# Load in the recipe, we want to REPLACE InstanceCreationDate with a function

recipe = DeidRecipe(deid=['/home/simona/dicom_project/luwak/luwakx/scripts/deid.dicom.safe_private_tags','/home/simona/dicom_project/luwak/luwakx/scripts/deid.dicom.remove-private-tags'], base=True)

def is_private(dicom, value, field, item):
    return field.element.is_private and (field.element.private_creator is not None)

for item in items:
    items[item]["is_private"] = is_private

# Parse the files
parsed_files = replace_identifiers(
    dicom_files=dicom_files, deid=recipe, strip_sequences=False,
    ids=items,
    remove_private=False,
    save=True, output_folder=output,
    overwrite=True
)

print(parsed_files[0])

print("end")