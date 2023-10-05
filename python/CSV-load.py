import csv

def proper_case(text):
    """Converts text to proper case (title case)."""
    return text.title()

def convert_to_proper_case(input_path, output_path):
    """
    Convert columns A to G to proper case.
    
    Args:
    - input_path (str): Path to the input CSV file.
    - output_path (str): Path to the output CSV file.
    """
    
    with open(input_path, 'r', encoding='utf-8-sig') as infile, open(output_path, 'w', encoding='utf-8-sig', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Write the header row as is
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            # Convert the text in columns A to G (0-based index 0 to 6) to proper case, update as required.
            for i in range(7):  
                row[i] = proper_case(row[i])
            writer.writerow(row)

if __name__ == "__main__":
    # Specify the input CSV path
    input_csv_path = "companies.csv"
    
    # Create an output file path
    output_csv_path = input_csv_path.rsplit('.', 1)[0] + "_proper_case.csv"

    # Convert the text in columns A to G to proper case
    convert_to_proper_case(input_csv_path, output_csv_path)
    print(f"Text converted to proper case! New file saved as {output_csv_path}")
