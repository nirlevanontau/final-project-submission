import os
import pandas as pd
import re

def sanitize_sheet_name(sheet_name):
    # Remove invalid characters from sheet name
    sanitized_name = re.sub(r'[\/:*?[\]]', '', sheet_name)
    return sanitized_name[:31]  # Limit sheet name length to 31 characters

def merge_excel_sheets(directory):
    # Create an empty DataFrame to store the merged sheets
    merged_data = pd.DataFrame()

    # Get a list of all Excel files in the directory
    excel_files = [file for file in os.listdir(directory) if file.endswith('.xlsx')]

    for file in excel_files:
        print(f"Processing file: {file}")

        # Load the Excel file
        file_path = os.path.join(directory, file)
        excel_data = pd.read_excel(file_path, sheet_name=None)

        # Iterate over each sheet in the file
        for sheet_name, sheet_data in excel_data.items():
            print(f"Processing sheet: {sheet_name}")

            # Check if the sheet already exists in the merged data
            if sheet_name in merged_data:
                # Merge the sheet based on the date column
                merged_data = pd.merge(merged_data, sheet_data, on="Date", how="outer")
            else:
                # Add the sheet to the merged data
                merged_data = pd.concat([merged_data, sheet_data], axis=1)

    # Save the merged data to a new Excel file
    merged_file_path = os.path.join(directory, "merged_sheets.xlsx")
    merged_data.to_excel(merged_file_path, index=False)
    print(f"Merged sheets saved to: {merged_file_path}")

def main():
    directory = "sheets"
    merge_excel_sheets(directory)

if __name__ == "__main__":
    main()
