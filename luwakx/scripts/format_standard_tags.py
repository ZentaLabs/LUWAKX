import os
import argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup

def fetch_tcia_table1(url, output_csv=None, use_existing=False):
    """
    Scrape Table 1 from TCIA Submission and De-identification Overview or load from CSV if present.

    Args:
        url (str): URL of the TCIA wiki page.
        output_csv (str, optional): Path to save or load the CSV file.
        use_existing (bool): If True and file exists, load from CSV instead of scraping.

    Returns:
        pd.DataFrame: DataFrame containing the table data.
    """
    if use_existing and output_csv and os.path.exists(output_csv):
        print(f"File {os.path.basename(output_csv)} already in {os.path.dirname(output_csv) or '.'}.")
        return pd.read_csv(output_csv)
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
    if output_csv:
        df.to_csv(output_csv, index=False)
        print(f"Saved TCIA Table 1 to {output_csv}")
    return df

def fetch_dicom_table_e1(url, output_csv=None, use_existing=False):
    """
    Scrape Table E.1-1 from DICOM part 15 or load from CSV if present.

    Args:
        url (str): URL of the DICOM part 15 page.
        output_csv (str, optional): Path to save or load the CSV file.
        use_existing (bool): If True and file exists, load from CSV instead of scraping.

    Returns:
        pd.DataFrame: DataFrame containing the table data.
    """
    if use_existing and output_csv and os.path.exists(output_csv):
        print(f"File {os.path.basename(output_csv)} already in {os.path.dirname(output_csv) or '.'}.")
        return pd.read_csv(output_csv)

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
    if output_csv:
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
    final['PrivateCreator'] = ''
    final['Name'] = merged['Attribute Name_tcia'].combine_first(merged['Attribute Name_dicom'])
    final['VR'] = merged['VR_tcia'].combine_first(merged['VR_dicom'])
    final['VM'] = merged['VM_tcia'].combine_first(merged['VM_dicom'])
    final['MatchTciaPrivateTags'] = ''
    final['TCIA element_sig_pattern'] = merged['Tag_tcia'].fillna('')
    final['TCIA tag_name'] = merged['Attribute Name_tcia'].fillna('')
    final['TCIA Private_Creator'] = ''
    final['TCIA private_disposition'] = ''
    final['MatchTciaStandardAttributes'] = merged['_merge'].apply(lambda x: 'Yes' if x == 'both' else '')
    for col in [
    'Basic Prof.', 'Rtn. Safe Priv. Opt.', 'Rtn. UIDs Opt.', 'Rtn. Dev. Id. Opt.', 'Rtn. Inst. Id. Opt.',
    'Rtn. Pat. Chars. Opt.', 'Rtn. Long. Full Dates Opt.', 'Rtn. Long. Modif. Dates Opt.',
    'Clean Desc. Opt.', 'Clean Struct. Cont. Opt.', 'Clean Graph. Opt.',
    'TCIA Profile', 'TCIA Implementation', 'Final CTP Script'
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
    print(merged.columns)
    final.to_csv(output_csv, index=False)
    print(f"Saved merged standard tags to {output_csv}")

def main():
    """
    Main function to orchestrate scraping, loading, merging, and saving Dicom standard tag tables.
    """
    parser = argparse.ArgumentParser(description="Scrape and compare standard DICOM tags from TCIA and DICOM tables.")
    parser.add_argument('--tcia_url', type=str, default="https://wiki.cancerimagingarchive.net/display/Public/Submission+and+De-identification+Overview")
    parser.add_argument('--dicom_url', type=str, default="https://dicom.nema.org/medical/dicom/current/output/html/part15.html#chapter_E")
    parser.add_argument('--tcia_csv', type=str, default="tcia_standard_tags.csv")
    parser.add_argument('--dicom_csv', type=str, default="dicom_standard_tags.csv")
    parser.add_argument('--output_csv', type=str, default="merged_standard_tags.csv")
    parser.add_argument('--use_existing_tcia_csv', action='store_true', help="Use existing TCIA CSV if present.")
    parser.add_argument('--use_existing_dicom_csv', action='store_true', help="Use existing DICOM CSV if present.")
    args = parser.parse_args()

    requested_cols = [
        'Group', 'Element', 'PrivateCreator', 'Name', 'VR', 'VM', 'Basic Prof.', 'Rtn. Safe Priv. Opt.',
        'Rtn. UIDs Opt.', 'Rtn. Dev. Id. Opt.', 'Rtn. Inst. Id. Opt.', 'Rtn. Pat. Chars. Opt.',
        'Rtn. Long. Full Dates Opt.', 'Rtn. Long. Modif. Dates Opt.', 'Clean Desc. Opt.',
        'Clean Struct. Cont. Opt.', 'Clean Graph. Opt.', 'MatchTciaPrivateTags',
        'TCIA element_sig_pattern', 'TCIA Private_Creator', 'TCIA tag_name', 'TCIA private_disposition',
        'MatchTciaStandardAttributes', 'TCIA Profile', 'TCIA Implementation', 'Final CTP Script'
    ]

    tcia_df = fetch_tcia_table1(args.tcia_url, args.tcia_csv, use_existing=args.use_existing_tcia_csv)
    dicom_df = fetch_dicom_table_e1(args.dicom_url, args.dicom_csv, use_existing=args.use_existing_dicom_csv)
    build_final_df(tcia_df, dicom_df, requested_cols, args.output_csv)

if __name__ == "__main__":
    main()