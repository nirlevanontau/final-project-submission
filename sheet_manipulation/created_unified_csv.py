import os
import pandas as pd
import sys
from datetime import datetime
from functools import reduce

def merge_excel_sheets(directory):
    excel_files = [file for file in os.listdir(directory) if file.endswith('.xlsx')]

    initial_dfs = []
    final_dfs = []

    for file in excel_files:
        print(f"Processing file: {file}")

        # Load the Excel file
        file_path = os.path.join(directory, file)
        # Read the Excel file and load "Sheet1" into a DataFrame
        excel_data = pd.read_excel(file_path, sheet_name="Sheet1", header=None)
        initial_dfs.append(excel_data)
    
    
    for df in initial_dfs:
      # Extract unique identifiers from the first column
      identifiers = df.iloc[:, 0].dropna().unique()
      
      # Initialize a dictionary to store data for each unique value in the second row
      parameters = df.iloc[1, 1:3].values
      data_dict = {name: {"id": identifiers} for name in parameters}
      
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
          data_type = (df[column].iloc[1])
          
          # Skip the column if the data type is empty
          if pd.isna(data_type):
              continue
          
          # Add the data from the column to the respective dictionary
          records = df[column].iloc[2:]#.values
          if data_type not in data_dict:
              data_dict[data_type] = {string_date: records}
          else:
              data_dict[data_type][string_date] = records
    
      for parameter, data in data_dict.items():
          data_frame = pd.DataFrame(data)
          date_cols = data_frame.columns[1:].tolist()
          action_rows_df = pd.melt(
              data_frame,
              id_vars=['id'],
              value_vars=date_cols,
              var_name='date',
              value_name=parameter
          ).dropna()
          final_dfs.append(action_rows_df)

    united_df = reduce(lambda df1,df2: pd.merge(df1,df2,on=['id', 'date'], how='outer'), final_dfs).fillna(0)

    united_df.to_csv(f'{directory}/united_data.csv', index=False)

    print("Processing complete!")

def main():
    directory = "sheets"
    merge_excel_sheets(directory)

if __name__ == '__main__':
    main()
