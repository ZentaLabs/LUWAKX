import csv

def generate_keep_statements(input_csv, output_file):
    with open(input_csv, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        with open(output_file, 'w') as outfile:
            outfile.write("FORMAT dicom\n\n%header\n\n")
            for row in reader:
                data_element = row['Data Element']
                private_creator = row['Private Creator']

                # Extract group and element from Data Element
                group = data_element[1:5]  # Extracts 7053 from (7053,xx00)
                element = data_element[8:10]  # Extracts 00 from (7053,xx00)

                # Format the output line
                line = f"KEEP ({group},\"{private_creator}\",{element})\n"
                outfile.write(line)

            # Add the final line to remove all other private tags
            line = f"REMOVE ALL func:is_tag_private\n"
            outfile.write(line)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate deid safe private tag recipe from CSV.")
    parser.add_argument('--input', type=str, default="DICOM_SAFE_PRIVATE_TAGS.csv", help='Input CSV file (default: DICOM_SAFE_PRIVATE_TAGS.csv)')
    parser.add_argument('--output', type=str, default="anonymization_recipes/deid.dicom.safe_private_tags", help='Output recipe file (default: anonymization_recipes/deid.dicom.safe_private_tags)')
    args = parser.parse_args()
    generate_keep_statements(args.input, args.output)

