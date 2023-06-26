import pandas as pd
import sys
import re
from datetime import datetime

def sanitize_sheet_name(sheet_name):
    # Remove invalid characters from sheet name
    sanitized_name = re.sub(r'[\/:*?[\]]', '', sheet_name)
    return sanitized_name[:31]  # Limit sheet name length to 31 characters

def process_excel_file(input_path, output_path):
    # Read the Excel file and load "Sheet1" into a DataFrame
    df = pd.read_excel(input_path, sheet_name="Sheet1", header=None)
    
    # Extract unique identifiers from the first column
    identifiers = df.iloc[:, 0].dropna().unique()
    
    # Initialize a dictionary to store data for each unique value in the second row
    sheet_names = df.iloc[1, 1:3].values
    data_dict = {sanitize_sheet_name(name): {"id": identifiers} for name in sheet_names}
    
    # Iterate over the remaining columns
    total_columns = len(df.columns[1:])
    current_column = 0
    
    for column in df.columns[1:]:
        # Update progress
        current_column += 1
        print(f"Processing column {current_column}/{total_columns}")
        
        # Extract the date from the first row of the current column
        date = df[column].iloc[0]
        string_date = datetime.strftime(date, "%Y-%m-%d")
        # Extract the data type from the second row
        data_type = sanitize_sheet_name(df[column].iloc[1])
        
        # Skip the column if the data type is empty
        if pd.isna(data_type):
            continue
        
        # Add the data from the column to the respective dictionary
        records = df[column].iloc[2:]#.values
        if data_type not in data_dict:
            data_dict[data_type] = {string_date: records}
        else:
            data_dict[data_type][string_date] = records
    
    # Create a new Excel file with a sheet for each unique value in the second row
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for sheet_name, data in data_dict.items():
            data_frame = pd.DataFrame(data)
            data_frame.to_excel(writer, sheet_name=str(sheet_name), index=False)
            worksheet = writer.sheets[str(sheet_name)]
            worksheet.autofilter(0, 0, data_frame.shape[0], data_frame.shape[1]-1)
        # writer.save(worksheet)
    
    print("Processing complete!")

def main():
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_file_path> <output_file_path>")
        return
    
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    
    process_excel_file(input_file_path, output_file_path)

if __name__ == '__main__':
    main()
