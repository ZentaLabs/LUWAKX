import pandas as pd
import requests
from bs4 import BeautifulSoup
import argparse
import os

def read_local_csv(local_csv_path):
    """
    Read the local CSV file into a pandas DataFrame. If the file does not exist locally,
    download it from the TCIA link.
    Args:
        local_csv_path (str): Path to the local CSV file.
    Returns:
        pd.DataFrame: DataFrame containing the CSV data.
    """
    if not os.path.exists(local_csv_path):
        
        tcia_url = "https://wiki.cancerimagingarchive.net/download/attachments/3539047/TCIAPrivateTagKB-02-01-2024-formatted.csv?version=2&modificationDate=1707174689263&api=v2"
        print(f"Downloading {local_csv_path}")
        response = requests.get(tcia_url)
        response.raise_for_status()
        with open(local_csv_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {local_csv_path}.")
    local_df = pd.read_csv(local_csv_path)
    original_col = local_df.columns[0]
    local_df.insert(1, "Private_Creator", "")
    return local_df

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

def reorder_and_save(local_df, output_path):
    """
    Apply transformation to DataFrame and save reordered CSV.
    Args:
        local_df (pd.DataFrame): DataFrame to transform.
        output_path (str): Path to save the reordered CSV.
    """
    local_df[['element_sig_pattern', 'Private_Creator','tag_name', 'vr','private_disposition']] = local_df.apply(transform_row, axis=1, result_type='expand')
    local_df.to_csv(output_path, index=False, header=True)

def fetch_dicom_table(url):
    """
    Download and parse the DICOM Table E.3.10-1 from the given URL.
    Args:
        url (str): URL to fetch the DICOM table from.
    Returns:
        pd.DataFrame: DataFrame containing the table data.
    """
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
    web_df = pd.DataFrame(rows, columns=headers)
    return web_df

def normalize_web_df(web_df):
    """
    Normalize columns in the web DataFrame for comparison.
    Args:
        web_df (pd.DataFrame): DataFrame to normalize.
    Returns:
        pd.DataFrame: Normalized DataFrame.
    """
    web_df["Data Element"] = web_df["Data Element"].str.strip().str.lower()
    web_df["Private Creator"] = web_df["Private Creator"].str.strip()
    web_df["VR"] = web_df["VR"].str.strip()
    web_df.rename(columns={"Data Element": "element_sig_pattern", "Private Creator": "Private_Creator", "VR": "vr"}, inplace=True)
    return web_df


def extract_last_paren(s):
    """
    Extracts the last element inside parentheses from a string.

    If the input string contains parentheses, the function returns the content of the last pair of parentheses.
    If there are no parentheses, it returns the string as is.
    If the input is NaN (as detected by pandas), it returns the input unchanged.

    Args:
        s (str or pandas.NA): The input string to process.

    Returns:
        str or pandas.NA: The last element inside parentheses, or the original string if no parentheses are present, or NaN if input is NaN.
    """
    if pd.isna(s):
        return s
    parts = [p for p in s.split('(') if p]
    if len(parts) > 1:
        last = parts[-1].split(')')[0]
        return last.strip()
    elif len(parts) == 1 and ')' in parts[0]:
        return parts[0].split(')')[0].strip()
    return s.strip()

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

def annotate_local_df(local_df, web_df, output_path):
    """
    Annotate local DataFrame with DICOM Safe Private Attribute and save to CSV.
    For comparison, if 'element_sig_pattern' or 'Private_Creator' have multiple elements separated by (),
    only the last element is considered. Also prints the count of True values and compares with web_df row count.
    Additionally, saves web_df rows not in local_df to another file.
    Args:
        local_df (pd.DataFrame): Local DataFrame to annotate.
        web_df (pd.DataFrame): Web DataFrame for comparison.
        output_path (str): Path to save the annotated CSV.
    """
    
    # Create new columns for comparison
    local_df['element_sig_pattern_cmp'] = local_df['element_sig_pattern'].apply(extract_last_paren)
    local_df['Private_Creator_cmp'] = local_df['Private_Creator'].apply(extract_last_paren)
    # Use these columns for comparison
    cmp_cols = ['element_sig_pattern_cmp', 'Private_Creator_cmp', 'vr']
    web_df['element_sig_pattern_cmp'] = web_df['element_sig_pattern'].apply(extract_last_paren)
    web_df['Private_Creator_cmp'] = web_df['Private_Creator'].apply(extract_last_paren)
    local_df['DICOM Safe Private Attribute'] = local_df[cmp_cols].apply(tuple, axis=1).isin(web_df[cmp_cols].apply(tuple, axis=1))
    true_count = local_df['DICOM Safe Private Attribute'].sum()
    web_count = len(web_df)
    # Add 'Basic Prof.' column
    local_df['Basic Prof.'] = local_df['DICOM Safe Private Attribute'].apply(lambda v: 'k' if v else 'x')
    # Add 'Rtn. Safe Priv. Opt.' column
    
    local_df['Rtn. Safe Priv. Opt.'] = local_df.apply(rtn_safe_priv_opt, axis=1)
    # Only save the requested columns for the annotated file
    save_cols = ['element_sig_pattern', 'Private_Creator', 'tag_name', 'vr', 'private_disposition', 'DICOM Safe Private Attribute', 'Basic Prof.', 'Rtn. Safe Priv. Opt.']
    local_df.to_csv(output_path, index=False, columns=save_cols)
    print(f"Saved to {output_path} with DICOM Safe Private Attribute column.")
    print(f"Number of True in 'DICOM Safe Private Attribute from TCIA private tags list': {true_count}")
    print(f"Number of rows in DICOM Safe Private Attribute: {web_count}")
    if true_count == web_count:
        print("The counts match.")
    else:
        print("The counts do NOT match.")
    # Save web_df rows not in local_df
    local_tuples = set(local_df[cmp_cols].apply(tuple, axis=1))
    web_not_in_local = web_df[~web_df[cmp_cols].apply(tuple, axis=1).isin(local_tuples)]
    web_not_in_local_path = output_path.replace('.csv', '_DICOM_SAFE_PRIVATE_TAGS_not_in_TCIA_PRIVATE_TAGS.csv')
    web_not_in_local.to_csv(web_not_in_local_path, index=False)
    print(f"Saved web_df rows not in local_df to {web_not_in_local_path}.")

def main():
    """
    Main function to parse command-line arguments and execute processing steps.
    """
    parser = argparse.ArgumentParser(description="Format and annotate DICOM private tags.")
    parser.add_argument('--input', type=str, default="TCIAPrivateTagKB-02-01-2024-formatted.csv", help="Input CSV file path.")
    parser.add_argument('--reordered', type=str, default="TCIAPrivateTagKB-reordered.csv", help="Output reordered CSV file path.")
    parser.add_argument('--annotated', type=str, default="TCIAPrivateTagKB-annotated.csv", help="Output annotated CSV file path.")
    parser.add_argument('--dicom_url', type=str, default="https://dicom.nema.org/medical/dicom/current/output/chtml/part15/sect_E.3.10.html", help="URL for DICOM Table E.3.10-1.")
    args = parser.parse_args()

    local_df = read_local_csv(args.input)
    reorder_and_save(local_df, args.reordered)
    web_df = fetch_dicom_table(args.dicom_url)
    web_df = normalize_web_df(web_df)
    annotate_local_df(local_df, web_df, args.annotated)

if __name__ == "__main__":
    main()