import os
import argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pydicom.datadict import get_entry
import sys
# Add the parent directory of luwakx to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from luwakx.utils import download_github_asset_by_tag


def read_tcia_csv(tcia_csv_path, tcia_url=None):
    foldername = os.path.dirname(tcia_csv_path) or '.'
    filename = os.path.basename(tcia_csv_path)
    token = os.environ.get("TEST_DATA_TOKEN")
    if os.path.exists(tcia_csv_path):
        print(f"File {filename} already in {foldername}.")
    else:
        if tcia_url is None:
            raise ValueError("No URL provided to download the CSV file.")
        print(f"Downloading {tcia_csv_path}")
        #these new lines are necessary as the url is no longer available for download and the file is hosted on GitHub
        archive_path = os.path.join(foldername,filename)
        download_github_asset_by_tag(
            "ZentaLabs", "luwak", "standard-private-tags", filename, archive_path, token
        )
        #response = requests.get(tcia_url)
        #response.raise_for_status()
        #with open(tcia_csv_path, "wb") as f:
        #    f.write(response.content)
        #print(f"Downloaded {tcia_csv_path}.")
    tcia_df = pd.read_csv(tcia_csv_path)
    tcia_df.insert(1, "Private_Creator", "")
    # Remove the CSV file after reading
    if os.path.exists(tcia_csv_path):
        os.remove(tcia_csv_path)
        print(f"Removed temporary file: {tcia_csv_path}")
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

def is_all_caps(s):
    return isinstance(s, str) and s.isupper() and s != s.lower()

def dedup_rows(df):
    # Group by element_sig_pattern, vr, and lower-case Private_Creator
    df['pc_lower'] = df['Private_Creator'].str.lower()
    df = df.sort_values(by=['Private_Creator'])  # so all-caps come after
    keep_rows = []
    for _, group in df.groupby(['element_sig_pattern', 'vr', 'pc_lower']):
        # If any row is not all caps, keep only those
        non_caps = group[~group['Private_Creator'].apply(is_all_caps)]
        if not non_caps.empty:
            keep_rows.append(non_caps)
        else:
            keep_rows.append(group)
    result = pd.concat(keep_rows, ignore_index=True)
    result = result.drop(columns=['pc_lower'])
    return result

def reorder_and_save(tcia_df, output_path, save_reformatted=False):
    """
    Apply transformation to DataFrame and save reformatted CSV if requested.
    Args:
        tcia_df (pd.DataFrame): DataFrame to transform.
        output_path (str): Path to save the reformatted CSV.
        save_reformatted (bool): Whether to save the reformatted CSV.
    """
    # Transform rows
    tcia_df[['element_sig_pattern', 'Private_Creator','tag_name', 'vr','private_disposition']] = tcia_df.apply(transform_row, axis=1, result_type='expand')
    # Keep only rows where private_disposition is 'k', 'h', or 'o'
    tcia_df = tcia_df[tcia_df['private_disposition'].isin(['k', 'h', 'o'])].copy()
    # Exclude rows with Private_Creator == 'Unnamed Private Block - 10'
    tcia_df = tcia_df[tcia_df['Private_Creator'] != 'Unnamed Private Block - 10'].copy()
    # Remove rows with all-caps Private_Creator if a non-all-caps duplicate exists
    tcia_df = dedup_rows(tcia_df)
    if save_reformatted:
        tcia_df.to_csv(output_path, index=False, header=True)
        print(f"Saved reformatted CSV to {output_path}.")
    return tcia_df

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
    dicom_std.rename(columns={"Data Element": "element_sig_pattern", "Private Creator": "Private_Creator", "VR": "vr", "Meaning": "tag_name"}, inplace=True)
    return dicom_std

def extract_last_paren(s, private_creator=False):
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
        if private_creator:
            return last.strip()
        return last.strip().lower()
    elif len(parts) == 1 and ')' in parts[0]:
        if private_creator:
            return parts[0].split(')')[0].strip()
        return parts[0].split(')')[0].strip().lower()
    if private_creator:
        return s.strip()
    return s.strip().lower()

def rtn_safe_priv_opt(row):
    """
    Determine the safe private attribute disposition for a DICOM row based on private_disposition value.
    Maps disposition codes to retention options:
    - 'k' -> 'keep'
    - 'o' -> 'func:hash_increment_date'
    - 'h' -> 'func:generate_hashuid'
    - 'd' and IsInDICOMRetainSafePrivateTags -> 'keep'
    
    Args:
        row (dict): A dictionary representing a DICOM row, expected to contain 'IsInDICOMRetainSafePrivateTags' and 'private_disposition' keys.
    Returns:
        str: The safe private retention option.
    """
    disposition = str(row['private_disposition']).strip().lower()
    
    if disposition == 'k':
        return 'keep'
    elif disposition == 'o':
        return 'func:hash_increment_date'
    elif disposition == 'h':
        return 'func:generate_hashuid'
    elif row['IsInDICOMRetainSafePrivateTags'] and disposition == 'd':
        return 'keep'

def split_group_element(val):
    """Split element_sig_pattern_cmp into Group and Element"""
    if isinstance(val, str) and ',' in val:
        parts = val.split(',')
        return parts[0].strip(), parts[1].strip()
    return '', ''

def merge_tcia_df(tcia_df, dicom_std, output_path, save_dicom_std_not_in_tcia=False):
    tcia_df['element_sig_pattern_cmp'] = tcia_df['element_sig_pattern'].apply(extract_last_paren)
    tcia_df['Private_Creator_cmp'] = tcia_df['Private_Creator'].apply(extract_last_paren, private_creator=True)
    dicom_std['element_sig_pattern_cmp'] = dicom_std['element_sig_pattern'].apply(extract_last_paren)
    dicom_std['Private_Creator_cmp'] = dicom_std['Private_Creator'].apply(extract_last_paren, private_creator=True)
    cmp_cols = ['element_sig_pattern_cmp', 'Private_Creator_cmp', 'vr']
    merged = pd.merge(
        tcia_df,
        dicom_std[['element_sig_pattern_cmp', 'Private_Creator_cmp', 'vr', 'VM', 'tag_name']],
        on=cmp_cols,
        how='left',
        suffixes=('', '_dicom')
    )
    merged['IsInDICOMRetainSafePrivateTags'] = merged[cmp_cols].apply(tuple, axis=1).isin(dicom_std[cmp_cols].apply(tuple, axis=1))
    merged['tag_name'] = merged['tag_name_dicom'].combine_first(merged['tag_name'])
    merged['Rtn. Safe Priv. Opt.'] = merged.apply(rtn_safe_priv_opt, axis=1)
    local_tuples = set(merged[cmp_cols].apply(tuple, axis=1))
    dicom_std_not_in_tcia = dicom_std[~dicom_std[cmp_cols].apply(tuple, axis=1).isin(local_tuples)].copy()
    dicom_std_not_in_tcia['private_disposition'] = ''
    dicom_std_not_in_tcia['IsInDICOMRetainSafePrivateTags'] = False
    dicom_std_not_in_tcia['Rtn. Safe Priv. Opt.'] = 'keep'
    save_cols = ['element_sig_pattern', 'Private_Creator', 'tag_name', 'vr', 'private_disposition', 'IsInDICOMRetainSafePrivateTags', 'VM', 'Rtn. Safe Priv. Opt.', 'element_sig_pattern_cmp','Private_Creator_cmp']
    merged_out = merged[save_cols]
    dicom_std_not_in_tcia_out = dicom_std_not_in_tcia[save_cols]
    final_out = pd.concat([merged_out, dicom_std_not_in_tcia_out], ignore_index=True)
    final_out['Group'], final_out['Element'] = zip(*final_out['element_sig_pattern_cmp'].map(split_group_element))
    # Remove rows where Group is even
    final_out = final_out[final_out['Group'].apply(lambda g: int(g, 16) % 2 == 1 if g else False)].copy()
    # Exclude rows where Element == 'xxinc.' or 'xxinc'
    final_out = final_out[final_out['Element'] != 'xxinc.'].copy()
    final_out = final_out[final_out['Element'] != 'xxinc'].copy()
   
    rename_map = {
        'Private_Creator': 'TCIA Private_Creator',
        'Private_Creator_cmp': 'Private Creator',
        'tag_name': 'Meaning',
        'vr': 'VR',
        'element_sig_pattern': 'TCIA element_sig_pattern',
        'private_disposition': 'TCIA private_disposition'
    }
    final_out = final_out.rename(columns=rename_map)
    requested_cols = [
        'Group', 'Element', 'Private Creator', 'VR', 'VM', 'Meaning', 'Rtn. Safe Priv. Opt.', 'IsInDICOMRetainSafePrivateTags',
        'TCIA element_sig_pattern'
    ]
     # Remove rows where Group, Element, and Private Creator are all equal (duplicates)
    final_out = final_out.drop_duplicates(subset=['Group', 'Element', 'Private Creator'])

    for col in requested_cols:
        if col not in final_out.columns:
            final_out[col] = ''
    final_out = final_out[requested_cols]
    final_out.to_csv(output_path, index=False, columns=requested_cols)
    print(f"Saved to {output_path}.")
    if save_dicom_std_not_in_tcia:
        print(f"Number of True in 'IsInDICOMRetainSafePrivateTags' from TCIA private tags list: {merged['IsInDICOMRetainSafePrivateTags'].sum()}")
        print(f"Number of rows in IsInDICOMRetainSafePrivateTags: {len(dicom_std)}")
        dicom_std_not_in_tcia_path = output_path.replace('.csv', '_DICOM_SAFE_PRIVATE_TAGS_not_in_TCIA_PRIVATE_TAGS.csv')
        dicom_std_not_in_tcia_out.to_csv(dicom_std_not_in_tcia_path, index=False)
        print(f"Saved dicom_std rows not in tcia_df to {dicom_std_not_in_tcia_path}.")

def fetch_tcia_table1(url, output_csv=None, save_csv=False):
    """
    Scrape Table 1 from TCIA Submission and De-identification Overview or load from CSV if present.

    Args:
        url (str): URL of the TCIA wiki page.
        output_csv (str, optional): Path to save or load the CSV file.
        save_csv (bool): If True, save the scraped data to CSV.

    Returns:
        pd.DataFrame: DataFrame containing the table data.
    """
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise ValueError("No tables found on the page.")
    table = tables[0]
    # Try to get headers from <th>, fallback to first row if needed
    headers = [th.text.strip() for th in table.find_all("th")]
    all_rows = table.find_all("tr")
    rows = []
    for tr in all_rows:
        cells = [td.text.strip() for td in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)
    if not headers and rows:
        headers = rows[0]
        rows = rows[1:]
    df = pd.DataFrame(rows, columns=headers)
    if save_csv:
        df.to_csv(output_csv, index=False)
        print(f"Saved TCIA Table 1 to {output_csv}")
    return df

def fetch_dicom_table_e1(url, output_csv=None, save_csv=False):
    """
    Scrape Table E.1-1 from DICOM part 15 or load from CSV if present.

    Args:
        url (str): URL of the DICOM part 15 page.
        output_csv (str, optional): Path to save or load the CSV file.
        save_csv (bool): If True, save the scraped data to CSV.

    Returns:
        pd.DataFrame: DataFrame containing the table data.
    """
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_anchor = soup.find(attrs={"id": "table_E.1-1"})
    if not table_anchor:
        raise ValueError("Could not find Table E.1-1 by id.")
    table = table_anchor.parent.find("table")
    if not table:
        raise ValueError("Could not find table element for Table E.1-1.")

    # Get headers
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
    else:
        # fallback: use first row as headers
        first_row = table.find("tr")
        headers = [td.get_text(strip=True) for td in first_row.find_all(["td", "th"])]

    # Get all rows
    data = []
    tbody = table.find("tbody")
    if not tbody:
        raise ValueError("Could not find tbody in Table E.1-1.")
    for tr in tbody.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) == len(headers):
            data.append(cells)
        else:
            # skip malformed rows
            continue

    df = pd.DataFrame(data, columns=headers)
    if save_csv:
        df.to_csv(output_csv, index=False)
        print(f"Saved DICOM Table E.1-1 to {output_csv}")
    return df


def extract_group_element(tag_str):
    """
    Extract Group and Element from tag string like (0010,0010) or 0010,0010.
    Args:
        tag_str (str): Tag string.
    Returns:
        tuple: (group, element) as strings.
    """
    tag_str = tag_str.replace('(', '').replace(')', '').replace(' ', '')
    if ',' in tag_str:
        group, element = tag_str.split(',', 1)
        return group, element
    return '', ''

def normalize_name(name):
    """
    Normalize attribute name for comparison.
    Args:
        name (str): Attribute name.
    Returns:
        str: Normalized name (lowercase, stripped).
    """
    return name.strip().lower() if isinstance(name, str) else ''

def build_final_df(tcia_df, dicom_df, requested_cols, output_csv):
    """
    Merge and compare TCIA and DICOM tables, mark matches, and save as CSV.
    Args:
        tcia_df (pd.DataFrame): TCIA DataFrame.
        dicom_df (pd.DataFrame): DICOM DataFrame.
        requested_cols (list): List of columns for final output.
        output_csv (str): Path to save the merged CSV.
    Returns:
        None
    """
    tcia_df['Tag_norm'] = tcia_df['Tag'].apply(lambda x: x.lower().replace('(', '').replace(')', '').replace(' ', ''))
    tcia_df['Name_norm'] = tcia_df['Attribute Name'].apply(normalize_name)
    dicom_df['Tag_norm'] = dicom_df['Tag'].apply(lambda x: x.lower().replace('(', '').replace(')', '').replace(' ', ''))
    dicom_df['Name_norm'] = dicom_df['Attribute Name'].apply(normalize_name)
    for col in ["VR", "VM"]:
        if col not in tcia_df.columns:
            tcia_df[col] = ""
        if col not in dicom_df.columns:
            dicom_df[col] = ""
    merged = pd.merge(
        tcia_df, dicom_df,
        on=['Tag_norm', 'Name_norm'],
        how='outer',
        indicator=True,
        suffixes=('_tcia', '_dicom')
    )
    final = pd.DataFrame()
    final['Group'], final['Element'] = zip(*merged['Tag_norm'].map(lambda x: extract_group_element(x) if pd.notna(x) else ('','')))
    final['Name'] = merged['Attribute Name_tcia'].combine_first(merged['Attribute Name_dicom'])
    final['VR'] = merged['VR_tcia'].combine_first(merged['VR_dicom'])
    final['VM'] = merged['VM_tcia'].combine_first(merged['VM_dicom'])
    final['TCIA element_sig_pattern'] = merged['Tag_tcia'].fillna('')
    final['MatchTciaStandardAttributes'] = merged['_merge'].apply(lambda x: 'Yes' if x == 'both' else '')
    for col in [
    'Basic Prof.', 'Rtn. Safe Priv. Opt.', 'Rtn. UIDs Opt.', 'Rtn. Dev. Id. Opt.', 'Rtn. Inst. Id. Opt.',
    'Rtn. Pat. Chars. Opt.', 'Rtn. Long. Full Dates Opt.', 'Rtn. Long. Modif. Dates Opt.',
    'Clean Desc. Opt.', 'Clean Struct. Cont. Opt.', 'Clean Graph. Opt.', 'Final CTP Script'
    ]:
        tcia_col = col + '_tcia'
        dicom_col = col + '_dicom'
        if tcia_col in merged.columns and dicom_col in merged.columns:
            final[col] = merged[tcia_col].combine_first(merged[dicom_col]).fillna('')
        elif tcia_col in merged.columns:
            final[col] = merged[tcia_col].fillna('')
        elif dicom_col in merged.columns:
            final[col] = merged[dicom_col].fillna('')
        elif col in merged.columns:
            final[col] = merged[col].fillna('')
        else:
            final[col] = ''
    for col in requested_cols:
        if col not in final.columns:
            final[col] = ''
    final = final[requested_cols]
    #print(merged.columns)
    #final.to_csv(output_csv, index=False)
    #print(f"Saved merged standard tags to {output_csv}")
    return final

def fill_missing_vr(df):
    """
    Fill in the VR column for specific DICOM tags that are missing from the pydicom dictionary.
    Args:
        df (pd.DataFrame): DataFrame with DICOM tags and a 'VR' column.
    Returns:
        pd.DataFrame: DataFrame with updated 'VR' values for known missing tags.
    """
    # Map of tag (as string "gggg,eeee") to VR
    missing_vr_map = {
        "0008,1301": "SQ",  # Principal Diagnosis Code Sequence
        "0008,1302": "SQ",  # Primary Diagnosis Code Sequence
        "0008,1303": "SQ",  # Secondary Diagnoses Code Sequence
        "0008,1304": "SQ",  # Histological Diagnoses Code Sequence
        "0010,0011": "SQ",  # Person Names to Use Sequence
        "0010,0012": "LT",  # Name to Use
        "0010,0013": "UT",  # Name to Use Comment
        "0010,0014": "SQ",  # Third Person Pronouns Sequence
        "0010,0015": "SQ",  # Pronoun Code Sequence
        "0010,0016": "UT",  # Pronoun Comment
        "0010,0041": "SQ",  # Gender Identity Sequence
        "0010,0042": "UT",  # Sex Parameters for Clinical Use Category Comment
        "0010,0043": "SQ",  # Sex Parameters for Clinical Use Category Sequence
        "0010,0044": "SQ",  # Gender Identity Code Sequence
        "0010,0045": "UT",  # Gender Identity Comment
        "0010,0046": "SQ",  # Sex Parameters for Clinical Use Category Code Sequence
        "0010,0047": "UR",  # Sex Parameters for Clinical Use Category Reference
        "0010,2162": "UC",  # Ethnic Groups
        "0040,a034": "DT",  # Effective Start DateTime
        "0040,a035": "DT",  # Effective Stop DateTime
        "0040,b034": "DT",  # Annotation DateTime
        "0040,b036": "DT",  # Segment Definition DateTime
        "0040,b03b": "LT",  # Montage Name
        "0040,b03f": "LO",  # Montage Channel Label
    }
    # Normalize tag format for matching
    def normalize_tag(row):
        group = str(row['Group']).lower().zfill(4)
        element = str(row['Element']).lower().zfill(4)
        return f"{group},{element}"

    for idx, row in df.iterrows():
        tag_str = normalize_tag(row)
        if tag_str in missing_vr_map:
            df.at[idx, 'VR'] = missing_vr_map[tag_str]
    return df

def get_vr_for_tag(tag_str, row):
    """
    Get the VR (Value Representation) for a DICOM tag.
    If the VR column value for the row exists (not empty/NaN), return it.
    Otherwise, try to get it from pydicom's dictionary.
    If the group is 50xx, 60xx, or 'gggg', do nothing (return None).
    Args:
        tag_str (str): DICOM tag string, e.g., '(0008,1301)' or '0008,1301'
        row (pd.Series): The DataFrame row containing the 'VR' column
    Returns:
        str or None: The VR value, or None if not found
    """
    vr_value = row.get('VR', None)
    if pd.notna(vr_value) and str(vr_value).strip() != '':
        return vr_value
    try:
        tag_str = tag_str.strip('()')
        group_str, elem_str = tag_str.split(',')
        group = group_str.strip().lower()
        # Check for special cases
        if group == 'gggg' or group == '50xx' or group == '60xx':
            return None
        group_int = int(group, 16)
        elem = int(elem_str, 16)
        entry = get_entry((group_int, elem))
        if entry:
            return entry[0]  # VR is the first element in the tuple
    except Exception as e:
        print(f"Could not determine VR for tag {tag_str}: {e}")
    return None

def generate_basic_profile(final_df):
    """
    Process a DataFrame and update the 'Basic Prof.' column based on DICOM anonymization rules.
    
    Args:
        final_df: DataFrame with DICOM standard tags
    
    Returns:
        DataFrame: Updated DataFrame with modified 'Basic Prof.' column
    """
    # Make a copy to avoid modifying the original
    df = final_df.copy()
    
    # Ensure required columns exist
    required_cols = ['Basic Prof.', 'Name', 'VR', 'TCIA element_sig_pattern', 'Group', 'Element']
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in DataFrame")
            return df
    
    for idx, row in df.iterrows():
        basic_profile = str(row['Basic Prof.']).strip()
        name = str(row['Name']) if pd.notna(row['Name']) else ""
        tag = str(row['TCIA element_sig_pattern']).strip() if pd.notna(row['TCIA element_sig_pattern']) else ""
        # Set empty tags
        if not tag:
            tag = '(' + str(row['Group']) + ',' + str(row['Element']) + ')'
        vr = get_vr_for_tag(tag, row)
        df.at[idx, 'VR'] = vr
        # Set empty tags
        if basic_profile == '':
            continue
            
        if basic_profile == 'X':
            if name == "Digital Signatures Sequence":
                df.at[idx, 'Basic Prof.'] = 'remove'
            elif name == "Curve Data":
                df.at[idx, 'Basic Prof.'] = 'remove'
            elif name == "Overlay Data":
                df.at[idx, 'Basic Prof.'] = 'remove'
            elif name == "Overlay Comments":
                df.at[idx, 'Basic Prof.'] = 'remove'
            elif name == "Data Set Trailing Padding":
                df.at[idx, 'Basic Prof.'] = 'remove'
            elif name == "Private Attributes":
                df.at[idx, 'Basic Prof.'] = 'remove'
            else:
                df.at[idx, 'Basic Prof.'] = 'remove'
                
        elif basic_profile == 'K':
            df.at[idx, 'Basic Prof.'] = 'keep'
            
        elif basic_profile == 'U':
            # Check if VR is UI for UID replacement
            if vr == 'UI':
                df.at[idx, 'Basic Prof.'] = 'func:generate_hashuid'
                
        elif basic_profile == 'D':
            replace_tag = ["AE", "LO", "LT", "SH", "PN", "CS", "ST", "UT", "UC", "UR", "DS", "IS", 
                           "FD", "FL", "SS", "US", "SL", "UL", 'AS', 'SQ', 'OD', 'OL', 'OV', 'SV', 'UV']
            # Check if VR is date/time related for date replacement   
            if vr in ['DA', 'DT', 'TM']:
                df.at[idx, 'Basic Prof.'] = 'func:set_fixed_datetime'
            elif vr in ['UI']:
                df.at[idx, 'Basic Prof.'] = 'func:generate_hashuid'
            elif vr in replace_tag:
                df.at[idx, 'Basic Prof.'] = 'replace'
            elif vr in ['OB', 'OW', 'OF', 'UN']:
                df.at[idx, 'Basic Prof.'] = 'remove'
            else:
                print(f"Tag {tag} with Basic Profile 'D' has unhandled VR '{vr}'. Keeping original value.")
                
        elif basic_profile == 'Z':
            # Check if VR is date/time related for date replacement
            if vr in ['DA', 'DT', 'TM']:
                df.at[idx, 'Basic Prof.'] = 'func:set_fixed_datetime'
            else:
                df.at[idx, 'Basic Prof.'] = 'blank'
                
        elif basic_profile in ['Z/D','X/Z','X/D','X/Z/D','X/Z/U*']:
            if vr in ['DA', 'DT', 'TM']:
                df.at[idx, 'Basic Prof.'] = 'func:set_fixed_datetime'
            elif basic_profile in ['X/Z/U*','X/D', 'Z/D', 'X/Z/D'] and vr == 'UI':
                df.at[idx, 'Basic Prof.'] = 'func:generate_hashuid'
            elif basic_profile in ['X/Z/U*', 'X/Z']:
                df.at[idx, 'Basic Prof.'] = 'blank'
            elif basic_profile in ['X/D', 'Z/D', 'X/Z/D']:
                if vr in ['OB', 'OW', 'OF', 'UN']:
                    df.at[idx, 'Basic Prof.'] = 'remove'
                else:
                    df.at[idx, 'Basic Prof.'] = 'replace'
            else:
                df.at[idx, 'Basic Prof.'] = 'manual_review'
    
    return df

def generate_retain_uid_profile(final_df):
    """
    Process a DataFrame and update the 'Rtn. UIDs Opt.' column based on DICOM UID retention rules.
    
    Args:
        final_df: DataFrame with DICOM standard tags
    
    Returns:
        DataFrame: Updated DataFrame with modified 'Rtn. UIDs Opt.' column
    """
    # Make a copy to avoid modifying the original
    df = final_df.copy()
    
    # Ensure required columns exist
    required_cols = ['Rtn. UIDs Opt.', 'Name', 'TCIA element_sig_pattern', 'Group', 'Element']
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in DataFrame")
            return df
    
    for idx, row in df.iterrows():
        profile = str(row['Rtn. UIDs Opt.']).strip()
        name = str(row['Name']) if pd.notna(row['Name']) else ""
        tag = str(row['TCIA element_sig_pattern']).strip() if pd.notna(row['TCIA element_sig_pattern']) else ""
        
        # Set empty tags
        if not tag:
            tag = '(' + str(row['Group']) + ',' + str(row['Element']) + ')'
        
        if profile == 'K':
            df.at[idx, 'Rtn. UIDs Opt.'] = 'keep'
        else:
            # For any other values, keep them as is or set to default
            continue
    
    return df
                    
def generate_retain_patient_characteristics_profile(final_df):
    """
    Process a DataFrame and update the 'Rtn. Pat. Chars. Opt.' column based on patient characteristics retention rules.
    
    Args:
        final_df: DataFrame with DICOM standard tags
    
    Returns:
        DataFrame: Updated DataFrame with modified 'Rtn. Pat. Chars. Opt.' column
    """
    # Make a copy to avoid modifying the original
    df = final_df.copy()
    
    # Ensure required columns exist
    required_cols = ['Rtn. Pat. Chars. Opt.', 'Name', 'TCIA element_sig_pattern', 'Group', 'Element']
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in DataFrame")
            return df
    
    for idx, row in df.iterrows():
        profile = str(row['Rtn. Pat. Chars. Opt.']).strip()
        name = str(row['Name']) if pd.notna(row['Name']) else ""
        tag = str(row['TCIA element_sig_pattern']).strip() if pd.notna(row['TCIA element_sig_pattern']) else ""
        
        # Set empty tags
        if not tag:
            tag = '(' + str(row['Group']) + ',' + str(row['Element']) + ')'
        
        if profile == 'K':
            df.at[idx, 'Rtn. Pat. Chars. Opt.'] = 'keep'
        else:
            # For any other values, keep them as is or set to default
            continue
    
    return df

def generate_retain_long_full_dates_profile(final_df):
    """
    Process a DataFrame and update the 'Rtn. Long. Full Dates Opt.' column based on date retention rules.
    
    Args:
        final_df: DataFrame with DICOM standard tags
    
    Returns:
        DataFrame: Updated DataFrame with modified 'Rtn. Long. Full Dates Opt.' column
    """
    # Make a copy to avoid modifying the original
    df = final_df.copy()
    
    # Ensure required columns exist
    required_cols = ['Rtn. Long. Full Dates Opt.', 'Name', 'TCIA element_sig_pattern', 'Group', 'Element']
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in DataFrame")
            return df
    
    for idx, row in df.iterrows():
        profile = str(row['Rtn. Long. Full Dates Opt.']).strip()
        name = str(row['Name']) if pd.notna(row['Name']) else ""
        tag = str(row['TCIA element_sig_pattern']).strip() if pd.notna(row['TCIA element_sig_pattern']) else ""
        # Set empty tags
        if not tag:
            tag = '(' + str(row['Group']) + ',' + str(row['Element']) + ')'
        if profile == 'K':
            df.at[idx, 'Rtn. Long. Full Dates Opt.'] = 'keep'
        elif profile == '':
            continue
        else:
            # For any other values, keep them as is or set to default
            pass
    
    return df

def generate_retain_long_modified_dates_profile(final_df):
    """
    Process a DataFrame and update the 'Rtn. Long. Modif. Dates Opt.' column based on modified date retention rules.
    
    Args:
        final_df: DataFrame with DICOM standard tags
    
    Returns:
        DataFrame: Updated DataFrame with modified 'Rtn. Long. Modif. Dates Opt.' column
    """
    # Make a copy to avoid modifying the original
    df = final_df.copy()
    
    # Ensure required columns exist
    required_cols = ['Rtn. Long. Modif. Dates Opt.', 'Name', 'TCIA element_sig_pattern', 'Group', 'Element']
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in DataFrame")
            return df
    
    for idx, row in df.iterrows():
        profile = str(row['Rtn. Long. Modif. Dates Opt.']).strip()
        name = str(row['Name']) if pd.notna(row['Name']) else ""
        tag = str(row['TCIA element_sig_pattern']).strip() if pd.notna(row['TCIA element_sig_pattern']) else ""
        
        # Set empty tags
        if not tag:
            tag = '(' + str(row['Group']) + ',' + str(row['Element']) + ')'
        
        if profile == 'C':
            if str(row['VR']) == 'TM':
                df.at[idx, 'Rtn. Long. Modif. Dates Opt.'] = 'keep'
            else:
                df.at[idx, 'Rtn. Long. Modif. Dates Opt.'] = 'func:hash_increment_date'
        else:
            # For any other values, keep them as is or set to default
            continue
    
    return df

def retain_device_id_option(df):
    """
    Process a DataFrame and update the 'Rtn. Device ID Opt.' column based on device ID retention rules.

    Args:
        df: DataFrame with DICOM standard tags

    Returns:
        DataFrame: Updated DataFrame with modified 'Rtn. Device ID Opt.' column
    """
    # Make a copy to avoid modifying the original
    df = df.copy()

    # Ensure required columns exist
    required_cols = ['Rtn. Dev. Id. Opt.', 'Name', 'TCIA element_sig_pattern', 'Group', 'Element']
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in DataFrame")
            return df

    for idx, row in df.iterrows():
        profile = str(row['Rtn. Dev. Id. Opt.']).strip()
        name = str(row['Name']) if pd.notna(row['Name']) else ""
        tag = str(row['TCIA element_sig_pattern']).strip() if pd.notna(row['TCIA element_sig_pattern']) else ""

        # Set empty tags
        if not tag:
            tag = '(' + str(row['Group']) + ',' + str(row['Element']) + ')'

        if profile == 'C':
            df.at[idx, 'Rtn. Dev. Id. Opt.'] = 'clean_manually'
        elif profile == 'K':
            df.at[idx, 'Rtn. Dev. Id. Opt.'] = 'keep'
        else:
            # For any other values, keep them as is or set to default
            continue

    return df

def retain_institution_id_option(df):
    """
    Process a DataFrame and update the 'Rtn. Inst. Id. Opt.' column based on institution ID retention rules.

    Args:
        df: DataFrame with DICOM standard tags
    Returns:
        DataFrame: Updated DataFrame with modified 'Rtn. Inst. Id. Opt.' column
    """
    # Make a copy to avoid modifying the original
    df = df.copy()

    # Ensure required columns exist
    required_cols = ['Rtn. Inst. Id. Opt.', 'Name', 'TCIA element_sig_pattern', 'Group', 'Element']
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in DataFrame")
            return df

    for idx, row in df.iterrows():
        profile = str(row['Rtn. Inst. Id. Opt.']).strip()
        name = str(row['Name']) if pd.notna(row['Name']) else ""
        tag = str(row['TCIA element_sig_pattern']).strip() if pd.notna(row['TCIA element_sig_pattern']) else ""

        # Set empty tags
        if not tag:
            tag = '(' + str(row['Group']) + ',' + str(row['Element']) + ')'

        if profile == 'K':
            df.at[idx, 'Rtn. Inst. Id. Opt.'] = 'keep'
        else:
            # For any other values, keep them as is or set to default
            continue

    return df

def clean_profiles(df):
    """
    Process a DataFrame and update the 'Clean Desc. Opt.', 'Clean Struct. Cont. Opt.', and 'Clean Graph. Opt.' columns based on institution ID retention rules.

    Args:
        df: DataFrame with DICOM standard tags
    Returns:
        DataFrame: Updated DataFrame with modified 'Clean Desc. Opt.', 'Clean Struct. Cont. Opt.', and 'Clean Graph. Opt.' columns
    """
    # Make a copy to avoid modifying the original
    df = df.copy()

    # Ensure required columns exist
    required_cols = ['Clean Desc. Opt.', 'Clean Struct. Cont. Opt.', 'Clean Graph. Opt.', 'Name', 'TCIA element_sig_pattern', 'Group', 'Element']
    for col in required_cols:
        if col not in df.columns:
            print(f"Warning: Required column '{col}' not found in DataFrame")
            return df

    for idx, row in df.iterrows():
        profile1 = str(row['Clean Desc. Opt.']).strip()
        profile2 = str(row['Clean Struct. Cont. Opt.']).strip()
        profile3 = str(row['Clean Graph. Opt.']).strip()
        name = str(row['Name']) if pd.notna(row['Name']) else ""
        tag = str(row['TCIA element_sig_pattern']).strip() if pd.notna(row['TCIA element_sig_pattern']) else ""

        # Set empty tags
        if not tag:
            tag = '(' + str(row['Group']) + ',' + str(row['Element']) + ')'

        if profile1 == 'C':
            df.at[idx, 'Clean Desc. Opt.'] = 'func:clean_descriptors_with_llm'
        if profile2 == 'C':
            df.at[idx, 'Clean Struct. Cont. Opt.'] = 'clean_manually'
        if profile3 == 'C':
            df.at[idx, 'Clean Graph. Opt.'] = 'clean_manually'
        else:
            # For any other values, keep them as is or set to default
            continue

    return df

def create_final_file(final_df, output_csv):
    """
    Apply all profile cleaning functions to the DataFrame.
    
    Args:
        final_df: DataFrame with DICOM standard tags
    
    Returns:
        DataFrame: Updated DataFrame with all profiles cleaned
    """
    df = final_df.copy()
    df = fill_missing_vr(df)
    df = generate_basic_profile(df)
    df = generate_retain_uid_profile(df)
    df = generate_retain_patient_characteristics_profile(df)
    df = generate_retain_long_full_dates_profile(df)
    df = generate_retain_long_modified_dates_profile(df)
    df = retain_device_id_option(df)
    df = retain_institution_id_option(df)
    df = clean_profiles(df)
    df.to_csv(output_csv, index=False)
    print(f"Saved merged standard tags to {output_csv}")

def main():
    parser = argparse.ArgumentParser(description="Retrieve and process DICOM private or standard tags.")
    # Private tag arguments
    parser.add_argument('--create_private_tag_template', action='store_true', help="Create private tag template.")
    parser.add_argument('--input_tcia', type=str, default="TCIAPrivateTagKB-02-01-2024-formatted.csv", help="Input CSV file path for TCIA private tags.")
    parser.add_argument('--tcia_url', type=str, default="https://wiki.cancerimagingarchive.net/download/attachments/3539047/TCIAPrivateTagKB-02-01-2024-formatted.csv?version=2&modificationDate=1707174689263&api=v2", help="URL to download CSV if not present.")
    parser.add_argument('--reformatted', type=str, default="TCIAPrivateTagKB-reformatted.csv", help="Output formatted TCIA private tags CSV file path.")
    parser.add_argument('--merged_private_tags', type=str, default="../data/TagsArchive/private_tags_template.csv", help="Default ../data/TagsArchive/private_tags_template.csv")
    parser.add_argument('--dicom_url', type=str, default="https://dicom.nema.org/medical/dicom/current/output/chtml/part15/sect_E.3.10.html", help="URL for DICOM safe private tags Table E.3.10-1.")
    parser.add_argument('--dicom_table_csv', type=str, help="Local CSV file path for DICOM safe private tags Table E.3.10-1.")
    parser.add_argument('--save_dicom_std_not_in_tcia', action='store_true', help="Save DICOM table rows not in TCIA private tags.")
    parser.add_argument('--save_reformatted', action='store_true', help="Save the reformatted CSV.")
    # Standard tag arguments
    parser.add_argument('--create_standard_tag_template', action='store_true', help="Create standard tag template.")
    parser.add_argument('--standard_tcia_url', type=str, default="https://wiki.cancerimagingarchive.net/display/Public/Submission+and+De-identification+Overview", help="URL for TCIA standard tags table.")
    parser.add_argument('--standard_dicom_url', type=str, default="https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E", help="URL for DICOM standard tags table.")
    parser.add_argument('--standard_tcia_csv', type=str, default="tcia_standard_tags.csv", help="Output CSV for TCIA standard tags.")
    parser.add_argument('--standard_dicom_csv', type=str, default="dicom_standard_tags.csv", help="Output CSV for DICOM standard tags.")
    parser.add_argument('--merged_standard_tags', type=str, default="../data/TagsArchive/standard_tags_template.csv", help="Output merged standard tags CSV.")
    parser.add_argument('--save_standard_tcia_csv', action='store_true', help="Use existing TCIA CSV if present.")
    parser.add_argument('--save_standard_dicom_csv', action='store_true', help="Use existing DICOM CSV if present.")
    args = parser.parse_args()

    temp_files = []

    # Auto-detect workflow based on arguments
    if args.create_private_tag_template:
        print("Creating private tag template...")
        # Private tag workflow
        tcia_df = read_tcia_csv(args.input_tcia, args.tcia_url)
        tcia_df = reorder_and_save(tcia_df, args.reformatted, save_reformatted=args.save_reformatted)
        dicom_std = fetch_dicom_table(url=args.dicom_url, dicom_csv_path=args.dicom_table_csv)
        dicom_std = normalize_dicom_std(dicom_std)
        merge_tcia_df(tcia_df, dicom_std, args.merged_private_tags, save_dicom_std_not_in_tcia=args.save_dicom_std_not_in_tcia)
        # Track temp files for removal
        if args.tcia_url:
            temp_files.append(args.input_tcia)
        if args.dicom_url:
            temp_files.append(args.dicom_table_csv)
    if args.create_standard_tag_template:
        # Standard tag workflow
        tcia_df = fetch_tcia_table1(args.standard_tcia_url, args.standard_tcia_csv, save_csv=args.save_standard_tcia_csv)
        dicom_df = fetch_dicom_table_e1(args.standard_dicom_url, args.standard_dicom_csv, save_csv=args.save_standard_dicom_csv)
        requested_cols = [
            'Group', 'Element', 'Name', 'VR', 'VM', 'Basic Prof.', 'Rtn. UIDs Opt.', 'Rtn. Dev. Id. Opt.', 'Rtn. Inst. Id. Opt.', 'Rtn. Pat. Chars. Opt.',
            'Rtn. Long. Full Dates Opt.', 'Rtn. Long. Modif. Dates Opt.', 'Clean Desc. Opt.',
            'Clean Struct. Cont. Opt.', 'Clean Graph. Opt.','TCIA element_sig_pattern', 'Final CTP Script'
        ]
        final_df = build_final_df(tcia_df, dicom_df, requested_cols, args.merged_standard_tags)
        create_final_file(final_df, args.merged_standard_tags)
        # Track temp files for removal
        if args.standard_tcia_url:
            temp_files.append(args.standard_tcia_csv)
        if args.standard_dicom_url:
            temp_files.append(args.standard_dicom_csv)
    elif not args.create_private_tag_template and not args.create_standard_tag_template:
        print("No action specified. Use --create_private_tag_template or --create_standard_tag_template.")
        parser.print_help()
        return

if __name__ == "__main__":
    main()
