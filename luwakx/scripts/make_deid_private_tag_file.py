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

if __name__ == "__main__":
    input_csv = "DICOM_SAFE_PRIVATE_TAGS.csv"
    output_file = "deid.dicom.safe_private_tags"
    generate_keep_statements(input_csv, output_file)

