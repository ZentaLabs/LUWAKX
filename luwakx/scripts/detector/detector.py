import argparse
import os
from time import perf_counter

import pandas as pd
from openai import OpenAI
from pydicom import datadict, dcmread
from pydicom.dataset import Dataset


def detect_phi_or_pii(
    client,
    dicom_tag_description,
    dev_mode,
    model="openai/gpt-oss-20b",
):
    """
    Detect if the DICOM tag description contains PHI/PII.
    """
    # Development mode, returns always 0
    if dev_mode:
        return 0

    result = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an accurate and helpful protected health information (PHI) "
                    "and personally identifiable information (PII) detector. Based on a DICOM tag "
                    "description and DICOM tag content, you will classify if the tag contains PHI or PII. "
                    "The output is only binary, nothing else. Return 1 if it contains PHI or PII and 0 if not."
                ),
            },
            {"role": "user", "content": dicom_tag_description},
        ],
        temperature=0,  # greedy decoding, removes randomness
        top_p=1,  # disables nucleus sampling
    )
    return result.choices[0].message.content


def process_dataset(dataset: Dataset, client, out_dict, dev_mode, parent_path=""):
    """
    Recursively process a DICOM dataset, including sequences.
    """
    for tag in dataset.keys():
        element = dataset[tag]
        print(element)

        attribute = datadict.keyword_for_tag(tag) or "Unknown"

        # Skip pixel data
        if attribute == "PixelData":
            continue

        tag_path = f"{parent_path}/{attribute}" if parent_path else attribute

        vr = element.VR
        if element.VR == "SQ":  # Sequence
            out_dict["Tag"].append(str(tag))
            out_dict["Attribute"].append(tag_path)
            out_dict["Value"].append(f"<Sequence with {len(element.value)} item(s)>")
            out_dict["VR"].append(vr)
            out_dict["PII_or_PHI"].append(0)
            out_dict["Runtime_in_ms"].append(0)

            for i, item in enumerate(element.value):
                process_dataset(
                    item, client, out_dict, dev_mode, parent_path=f"{tag_path}[{i}]"
                )
        else:
            value = str(element.value)

            # If value of a tag is empty, run no detection but classify as 0
            if not value:
                result = 0
                run_time = 0
            else:

                # Start time
                start_time = perf_counter()
                result = detect_phi_or_pii(
                    client, f"{tag} {tag_path}: {value}", dev_mode
                )
                # End time
                end_time = perf_counter()
                run_time = end_time - start_time
                run_time = run_time * 1000  # in ms

            out_dict["Tag"].append(str(tag))
            out_dict["Attribute"].append(tag_path)
            out_dict["Value"].append(value)
            out_dict["VR"].append(vr)
            out_dict["PII_or_PHI"].append(result)
            out_dict["Runtime_in_ms"].append(run_time)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Detect PHI/PII in a DICOM file.")
    parser.add_argument("--fpath", required=True, help="Path to the input DICOM file")
    parser.add_argument(
        "--dev_mode", action="store_true", help="Run in development mode (no LLM calls)"
    )
    args = parser.parse_args()

    fpath = args.fpath
    dev_mode = args.dev_mode
    print(f"Development mode: {dev_mode}")

    # Set up client
    client = OpenAI(base_url="http://localhost:1234/v1", api_key="")

    # Read DICOM file
    dcm = dcmread(fpath)

    # Dict to store detector results
    out_dict = {
        "Tag": [],
        "Attribute": [],
        "Value": [],
        "VR": [],
        "PII_or_PHI": [],
        "Runtime_in_ms": [],
    }

    # Process a single DICOM dataset
    process_dataset(dcm, client, out_dict, dev_mode=dev_mode)

    # Save results as csv
    out_df = pd.DataFrame(out_dict)
    print(out_df)
    out_df.to_csv("results.csv", index=False)
