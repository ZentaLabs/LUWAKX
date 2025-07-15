import pandas as pd
import requests
from bs4 import BeautifulSoup
import argparse
import os

def read_tcia_csv(tcia_csv_path, tcia_url=None):
    """
    Read the TCIA CSV file of private tags into a pandas DataFrame. If the file does not exist locally,
    download it from the provided URL.
    Args:
        tcia_csv_path (str): Path to the TCIA CSV file.
        tcia_url (str, optional): URL to download the CSV file if not present.
    Returns:
        pd.DataFrame: DataFrame containing the CSV data.
    """
    foldername = os.path.dirname(tcia_csv_path) or '.'
    filename = os.path.basename(tcia_csv_path)
    if os.path.exists(tcia_csv_path):
        print(f"File {filename} already in {foldername}.")
    else:
        if tcia_url is None:
            raise ValueError("No URL provided to download the CSV file.")
        print(f"Downloading {tcia_csv_path}")
        response = requests.get(tcia_url)
        response.raise_for_status()
        with open(tcia_csv_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {tcia_csv_path}.")
    tcia_df = pd.read_csv(tcia_csv_path)
    tcia_df.insert(1, "Private_Creator", "")
    return tcia_df

def transform_row(row):
    """
    Transform a row from the local CSV to extract tag, private creator, name, VR, and disposition.
    Args:
        row (pd.Series): Row from the DataFrame.
    Returns:
        list: Transformed row with [element_sig_pattern, Private_Creator, tag_name, vr, private_disposition].
    """
    s = row.iloc[0]
    segments = s.split('(')
    new_tag = []
    private_creator = []
    for seg in segments:
        if ')' in seg:
            content, rest = seg.split(')', 1)
            parts = content.split(',')
            if len(parts) >= 3:
                gggg = parts[0].replace('"', '').strip()
                string = parts[1].replace('"', '').strip()
                ff = parts[2].replace('"', '').strip()
                tag = f"{gggg},xx{ff}"
                new_tag.append(f"({tag})")
                if len(segments) > 2:
                    private_creator.append("("+string+")")
                else: 
                    private_creator.append(string)
            else:
                new_tag.append(f"({content})")
            if rest:
                new_tag.append(rest)
        else:
            if seg:
                new_tag.append(seg)
    new_s = ''.join(new_tag)
    private_c = ''.join(private_creator)
    return [new_s, private_c, row.iloc[2], row.iloc[3], row.iloc[4]]

def reorder_and_save(tcia_df, output_path, save_reformatted=False):
    """
    Apply transformation to DataFrame and save reformatted CSV if requested.
    Args:
        tcia_df (pd.DataFrame): DataFrame to transform.
        output_path (str): Path to save the reformatted CSV.
        save_reformatted (bool): Whether to save the reformatted CSV.
    """
    tcia_df[['element_sig_pattern', 'Private_Creator','tag_name', 'vr','private_disposition']] = tcia_df.apply(transform_row, axis=1, result_type='expand')
    if save_reformatted:
        tcia_df.to_csv(output_path, index=False, header=True)
        print(f"Saved reformatted CSV to {output_path}.")

def fetch_dicom_table(url=None, dicom_csv_path=None):
    """
    Download and parse the safe private tags DICOM Table E.3.10-1 from the given URL, or load from a local CSV file if present.
    Args:
        url (str, optional): URL to fetch the DICOM table from.
        dicom_csv_path (str, optional): Path to local CSV file to load/save the table.
    Returns:
        pd.DataFrame: DataFrame containing the table data.
    """
    if dicom_csv_path:
        foldername = os.path.dirname(dicom_csv_path) or '.'
        filename = os.path.basename(dicom_csv_path)
        if os.path.exists(dicom_csv_path):
            print(f"File {filename} already in {foldername}.")
            return pd.read_csv(dicom_csv_path)
    if url is None:
        raise ValueError("No URL provided to fetch the DICOM table.")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    target_table = None
    for tbl in tables:
        headers = [th.text.strip() for th in tbl.find_all("th")]
        if "Data Element" in headers:
            target_table = tbl
            break
    if target_table is None:
        raise ValueError("Could not find the table containing 'Data Element'")
    headers = [th.text.strip() for th in target_table.find_all("th")]
    rows = []
    for tr in target_table.find_all("tr")[1:]:
        cells = [td.text.strip() for td in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)
    dicom_std = pd.DataFrame(rows, columns=headers)
    if dicom_csv_path:
        dicom_std.to_csv(dicom_csv_path, index=False)
        print(f"Saved DICOM table to {dicom_csv_path}.")
    return dicom_std

def normalize_dicom_std(dicom_std):
    """
    Normalize columns in the web DataFrame for comparison.
    Args:
        dicom_std (pd.DataFrame): DataFrame to normalize.
    Returns:
        pd.DataFrame: Normalized DataFrame.
    """
    dicom_std["Data Element"] = dicom_std["Data Element"].str.strip().str.lower()
    dicom_std["Private Creator"] = dicom_std["Private Creator"].str.strip()
    dicom_std["VR"] = dicom_std["VR"].str.strip()
    dicom_std["VM"] = dicom_std["VM"].str.strip()
    dicom_std["Meaning"] = dicom_std["Meaning"].str.strip()
    dicom_std.rename(columns={"Data Element": "element_sig_pattern", "Private Creator": "Private_Creator", "VR": "vr", "VM": "vm", "Meaning": "tag_name"}, inplace=True)
    return dicom_std


def extract_last_paren(s):
    """
    Extracts the last element inside parentheses from a string and returns it in lower case.

    If the input string contains parentheses, the function returns the content of the last pair of parentheses in lower case.
    If there are no parentheses, it returns the string as is, in lower case.
    If the input is NaN (as detected by pandas), it returns the input unchanged.

    Args:
        s (str or pandas.NA): The input string to process.

    Returns:
        str or pandas.NA: The last element inside parentheses (lower case), or the original string in lower case if no parentheses are present, or NaN if input is NaN.
    """
    if pd.isna(s):
        return s
    parts = [p for p in s.split('(') if p]
    if len(parts) > 1:
        last = parts[-1].split(')')[0]
        return last.strip().lower()
    elif len(parts) == 1 and ')' in parts[0]:
        return parts[0].split(')')[0].strip().lower()
    return s.strip().lower()

def rtn_safe_priv_opt(row):
    """
    Determine the safe private attribute disposition for a DICOM row.

    Checks if the 'DICOM Safe Private Attribute' is set and if the 'private_disposition'
    is 'd' (case-insensitive, with whitespace stripped). If both conditions are met,
    returns 'k' to indicate a safe disposition. Otherwise, returns the original
    'private_disposition' value.

    Args:
        row (dict): A dictionary representing a DICOM row, expected to contain
            'DICOM Safe Private Attribute' and 'private_disposition' keys.

    Returns:
        str: The safe private disposition value ('k' or the original disposition).
    """
    if row['DICOM Safe Private Attribute'] and str(row['private_disposition']).strip().lower() == 'd':
        return 'k'
    return row['private_disposition']

def annotate_tcia_df(tcia_df, dicom_std, output_path, save_dicom_std_not_in_tcia=False):
    """
    Annotate local DataFrame with DICOM Safe Private Attribute and save to CSV.
    For matching rows, add vm from dicom_std and replace tag_name with dicom_std's tag_name.
    For non-matching dicom_std rows, add them with empty private_disposition and k for Basic Prof. and Rtn. Safe Priv. Opt.
    Args:
        tcia_df (pd.DataFrame): Local DataFrame to annotate.
        dicom_std (pd.DataFrame): Web DataFrame for comparison.
        output_path (str): Path to save the annotated CSV.
        save_dicom_std_not_in_tcia (bool): Whether to save dicom_std rows not in tcia_df.
    """
    # Prepare comparison columns
    tcia_df['element_sig_pattern_cmp'] = tcia_df['element_sig_pattern'].apply(extract_last_paren)
    tcia_df['Private_Creator_cmp'] = tcia_df['Private_Creator'].apply(extract_last_paren)
    dicom_std['element_sig_pattern_cmp'] = dicom_std['element_sig_pattern'].apply(extract_last_paren)
    dicom_std['Private_Creator_cmp'] = dicom_std['Private_Creator'].apply(extract_last_paren)
    cmp_cols = ['element_sig_pattern_cmp', 'Private_Creator_cmp', 'vr']

    # Merge matching rows
    merged = pd.merge(
        tcia_df,
        dicom_std[['element_sig_pattern_cmp', 'Private_Creator_cmp', 'vr', 'vm', 'tag_name']],
        on=cmp_cols,
        how='left',
        suffixes=('', '_dicom')
    )
    merged['DICOM Safe Private Attribute'] = merged[cmp_cols].apply(tuple, axis=1).isin(dicom_std[cmp_cols].apply(tuple, axis=1))
    merged['tag_name'] = merged['tag_name_dicom'].where(merged['tag_name_dicom'].notna(), merged['tag_name'])
    merged['vm'] = merged['vm']
    merged['Basic Prof.'] = merged['DICOM Safe Private Attribute'].apply(lambda v: 'k' if v else 'x')
    merged['Rtn. Safe Priv. Opt.'] = merged.apply(rtn_safe_priv_opt, axis=1)

    # Prepare non-matching dicom_std rows
    local_tuples = set(merged[cmp_cols].apply(tuple, axis=1))
    dicom_std_not_in_tcia = dicom_std[~dicom_std[cmp_cols].apply(tuple, axis=1).isin(local_tuples)].copy()
    dicom_std_not_in_tcia['private_disposition'] = ''
    dicom_std_not_in_tcia['DICOM Safe Private Attribute'] = False
    dicom_std_not_in_tcia['Basic Prof.'] = 'k'
    dicom_std_not_in_tcia['Rtn. Safe Priv. Opt.'] = 'k'

    # Select columns for output
    save_cols = ['element_sig_pattern', 'Private_Creator', 'tag_name', 'vr', 'private_disposition', 'DICOM Safe Private Attribute', 'vm', 'Basic Prof.', 'Rtn. Safe Priv. Opt.']
    merged_out = merged[save_cols]
    dicom_std_not_in_tcia_out = dicom_std_not_in_tcia[save_cols]
    final_out = pd.concat([merged_out, dicom_std_not_in_tcia_out], ignore_index=True)
    final_out.to_csv(output_path, index=False, columns=save_cols)
    print(f"Saved to {output_path} with DICOM Safe Private Attribute.")
    if save_dicom_std_not_in_tcia:
        print(f"Number of True in 'DICOM Safe Private Attribute from TCIA private tags list': {merged['DICOM Safe Private Attribute'].sum()}")
        print(f"Number of rows in DICOM Safe Private Attribute: {len(dicom_std)}")
        dicom_std_not_in_tcia_path = output_path.replace('.csv', '_DICOM_SAFE_PRIVATE_TAGS_not_in_TCIA_PRIVATE_TAGS.csv')
        dicom_std_not_in_tcia_out.to_csv(dicom_std_not_in_tcia_path, index=False)
        print(f"Saved dicom_std rows not in tcia_df to {dicom_std_not_in_tcia_path}.")

def main():
    """
    Main function to parse command-line arguments and execute processing steps.
    """
    parser = argparse.ArgumentParser(description="Format and annotate DICOM private tags.")
    parser.add_argument('--input_tcia', type=str, default="TCIAPrivateTagKB-02-01-2024-formatted.csv", help="Input CSV file path for TCIA private tags.")
    parser.add_argument('--tcia_url', type=str, default="https://wiki.cancerimagingarchive.net/download/attachments/3539047/TCIAPrivateTagKB-02-01-2024-formatted.csv?version=2&modificationDate=1707174689263&api=v2", help="URL to download CSV if not present.")
    parser.add_argument('--reformatted', type=str, default="TCIAPrivateTagKB-reformatted.csv", help="Output formatted TCIA private tags CSV file path.")
    parser.add_argument('--annotated', type=str, default="TCIAPrivateTagKB-annotated.csv", help="Output annotated CSV file path.")
    parser.add_argument('--dicom_url', type=str, default="https://dicom.nema.org/medical/dicom/current/output/chtml/part15/sect_E.3.10.html", help="URL for DICOM safe private tags Table E.3.10-1.")
    parser.add_argument('--dicom_table_csv', type=str, default="DICOM_SAFE_PRIVATE_TAGS.csv", help="Local CSV file path for DICOM safe private tags Table E.3.10-1.")
    parser.add_argument('--save_dicom_std_not_in_tcia', action='store_true', help="Save DICOM table rows not in TCIA private tags.")
    parser.add_argument('--save_reformatted', action='store_true', help="Save the reformatted CSV.")
    args = parser.parse_args()

    tcia_df = read_tcia_csv(args.input_tcia, args.tcia_url)
    reorder_and_save(tcia_df, args.reformatted, save_reformatted=args.save_reformatted)
    dicom_std = fetch_dicom_table(url=args.dicom_url, dicom_csv_path=args.dicom_table_csv)
    dicom_std = normalize_dicom_std(dicom_std)
    annotate_tcia_df(tcia_df, dicom_std, args.annotated, save_dicom_std_not_in_tcia=args.save_dicom_std_not_in_tcia)

if __name__ == "__main__":
    main()