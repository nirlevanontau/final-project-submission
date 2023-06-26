import pytest
import os
import pandas as pd

# taking into account we're currently inside the 'data' folder,
# create a test checking that all .csv files are read correctly
def test_read_csv():
    # get current path
    current_path = os.getcwd()
    # get path to data folder
    # get list of files in data folder
    files = os.listdir(current_path)
    # get list of .csv files
    csv_files = [file for file in files if file.endswith('.csv')]
    # create empty list to store dataframes
    df_list = []
    # loop over csv files
    for file in csv_files:
        # read csv file
        df = pd.read_csv(os.path.join(current_path, file))
        # append dataframe to list
        df_list.append(df)
    # check that all dataframes have been read
    assert len(df_list) == len(csv_files)

# from the './positions.csv' file, take all unique values for the 'location' column, and make sure that all of them exist in the './cells.csv' file 'location' column:
def test_check_locations():
    # get current path
    current_path = os.getcwd()
    # read positions.csv
    positions = pd.read_csv(os.path.join(current_path, 'positions.csv'))
    # read cells.csv
    cells = pd.read_csv(os.path.join(current_path, 'cells.csv'))
    # get unique values for 'location' column in positions.csv
    unique_locations = positions['location'].unique()
    # get unique values for 'location' column in cells.csv
    unique_cells = cells['location'].unique()
    # check that all unique locations in positions.csv exist in cells.csv, is missing, print the missing values
    missing_locations = [location for location in unique_locations if location not in unique_cells]
    assert len(missing_locations) == 0, f"Missing locations: {missing_locations}"

# from the './positions.csv', 'orders.csv', and 'shipments.csv' files, take all unique values for the 'uuid' column, and make sure that all of them exist in the './items.csv' file 'uuid' column, if missing, print the missing values and in which file they are missing:
def test_check_uuids():
    # get current path
    current_path = os.getcwd()
    # read positions.csv
    positions = pd.read_csv(os.path.join(current_path, 'positions.csv'))
    # read orders.csv
    orders = pd.read_csv(os.path.join(current_path, 'orders.csv'))
    # read shipments.csv
    shipments = pd.read_csv(os.path.join(current_path, 'shipments.csv'))
    # read items.csv
    items = pd.read_csv(os.path.join(current_path, 'items.csv'))
    # get unique values for 'uuid' column in positions.csv
    unique_positions = positions['uuid'].unique()
    # get unique values for 'uuid' column in orders.csv
    unique_orders = orders['uuid'].unique()
    # get unique values for 'uuid' column in shipments.csv
    unique_shipments = shipments['uuid'].unique()
    # get unique values for 'uuid' column in items.csv
    unique_items = items['uuid'].unique()
    # check that all unique uuids in positions.csv exist in items.csv, if missing, print the missing values
    missing_positions = [uuid for uuid in unique_positions if uuid not in unique_items]
    assert len(missing_positions) == 0, f"Missing uuids in positions.csv: {missing_positions}"
    # check that all unique uuids in orders.csv exist in items.csv, if missing, print the missing values
    missing_orders = [uuid for uuid in unique_orders if uuid not in unique_items]
    assert len(missing_orders) == 0, f"Missing uuids in orders.csv: {missing_orders}"
    # check that all unique uuids in shipments.csv exist in items.csv, if missing, print the missing values
    missing_shipments = [uuid for uuid in unique_shipments if uuid not in unique_items]
    assert len(missing_shipments) == 0, f"Missing uuids in shipments.csv: {missing_shipments}"

# check that all of the .csv files don't have any missing values:
def test_check_missing_values():
    # get current path
    current_path = os.getcwd()
    # get path to data folder
    # get list of files in data folder
    files = os.listdir(current_path)
    # get list of .csv files
    csv_files = [file for file in files if file.endswith('.csv')]
    # loop over csv files
    for file in csv_files:
        # read csv file
        df = pd.read_csv(os.path.join(current_path, file))
        # check that there are no missing values
        assert df.isnull().sum().sum() == 0, f"Missing values in {file}: {df.isnull().sum().sum()}"
