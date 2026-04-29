import argparse
import json
import os
from time import perf_counter

import httpx

import pandas as pd
from openai import OpenAI
from pydicom import datadict, dcmread
from pydicom.dataset import Dataset


def create_openai_client(base_url, api_key, http_headers=None):
    client_kwargs = {}
    client_kwargs["base_url"] = base_url
    client_kwargs["api_key"] = api_key or ""
    if http_headers is None:
        http_headers_raw = os.environ.get("CLEAN_DESCRIPTORS_LLM_HTTP_HEADERS")
        if http_headers_raw:
            http_headers = json.loads(http_headers_raw)
    if http_headers:
        client_kwargs["http_client"] = httpx.Client(headers=http_headers)
    return OpenAI(**client_kwargs)


def detect_phi_or_pii(
    client,
    dicom_tag_description,
    dev_mode,
    model,
):
    """
    Detect if the DICOM tag description contains PHI/PII.

    Returns:
        tuple: (content, reasoning) where content is the binary classification
               result ("0" or "1") and reasoning is the model's reasoning text
               if available, or None otherwise.
    """
    # Development mode, returns always 0
    if dev_mode:
        return 0, None

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
    content = result.choices[0].message.content
    reasoning = getattr(result.choices[0].message, 'reasoning', None)
    return content, reasoning


def process_dataset(
    dataset: Dataset, client, out_dict, dev_mode, model, parent_path=""
):
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
                    item,
                    client,
                    out_dict,
                    dev_mode,
                    model,
                    parent_path=f"{tag_path}[{i}]",
                )
        else:
            value = str(element.value)

            # If value of a tag is empty, run no detection but classify as 0
            if not value:
                result = 0
                run_time = 0
                reasoning = "empty value"
            else:

                # Start time
                start_time = perf_counter()
                result, reasoning = detect_phi_or_pii(
                    client, f"{tag} {tag_path}: {value}", dev_mode, model
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
            out_dict["Reasoning"].append(reasoning)
            out_dict["Runtime_in_ms"].append(run_time)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Detect PHI/PII in a DICOM file.")
    parser.add_argument("--fpath", required=True, help="Path to the input DICOM file")
    parser.add_argument("--base-url", default=None, help="LLM API base URL (use env var CLEAN_DESCRIPTORS_LLM_BASE_URL if not provided)")
    parser.add_argument("--model", default=None, help="Name of LLM model to use (use env var CLEAN_DESCRIPTORS_LLM_MODEL if not provided, default: openai/gpt-oss-20b)")
    parser.add_argument("--api-key", default=None, help="LLM API key (use env var CLEAN_DESCRIPTORS_LLM_API_KEY if not provided)")
    parser.add_argument(
        "--dev_mode", action="store_true", help="Run in development mode (no LLM calls)"
    )
    args = parser.parse_args()

    fpath = args.fpath
    dev_mode = args.dev_mode

    base_url = args.base_url or os.environ.get("CLEAN_DESCRIPTORS_LLM_BASE_URL")
    model = args.model or os.environ.get("CLEAN_DESCRIPTORS_LLM_MODEL", "openai/gpt-oss-20b")
    api_key = args.api_key or os.environ.get("CLEAN_DESCRIPTORS_LLM_API_KEY")

    if not base_url and not api_key:
        raise ValueError("You must provide either a base URL or an API key for the LLM.")

    print(f"Model: {model}")
    print(f"Development mode: {dev_mode}")

    if base_url:
        print(f"Using base URL: {base_url}")
    client = create_openai_client(base_url, api_key)

    # Read DICOM file
    dcm = dcmread(fpath)

    # Dict to store detector results
    out_dict = {
        "Tag": [],
        "Attribute": [],
        "Value": [],
        "VR": [],
        "PII_or_PHI": [],
        "Reasoning": [],
        "Runtime_in_ms": [],
    }

    # Process a single DICOM dataset
    process_dataset(dcm, client, out_dict, dev_mode=dev_mode, model=model)

    # Save results as csv
    out_df = pd.DataFrame(out_dict)
    print(out_df)
    out_df.to_csv("results.csv", index=False)
