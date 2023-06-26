import pandas as pd
from datetime import datetime

DATA_PATH = 'src/data'

PLACEMENT = 'original' # 'placement#'

# create an import function for each csv file
def import_dates_data(max_date: datetime or None = None) -> pd.DataFrame:
    df = pd.read_csv(f'{DATA_PATH}/dates.csv')
    # define the date column as datetime
    df['date'] = pd.to_datetime(df['date'])
    if max_date is not None:
        df = df[df['date'] < max_date]
    return df

def import_warehouse_data(path: str = f'{DATA_PATH}/{PLACEMENT}/cells.csv') -> pd.DataFrame:
    df = pd.read_csv(path)
    # drop the 'fetch_zone' column
    df = df.drop(columns=['fetch_zone'])
    # replace all column which are objects with the relevant ids from 'src/data/ids/
    location_df = pd.read_csv( f'{DATA_PATH}/ids/location_ids.csv')
    # replace the values under the 'location' column with the corresponding value from the location_df 'location_id' column
    df = df.merge(location_df, on='location').drop(columns=['location'])
    putaway_zone_df = pd.read_csv( f'{DATA_PATH}/ids/putaway_zone_ids.csv')
    df = df.merge(putaway_zone_df[['putaway_zone', 'putaway_zone_id']], on='putaway_zone').drop(columns=['putaway_zone'])
    tool_df = pd.read_csv( f'{DATA_PATH}/ids/tool_ids.csv', usecols=['fetch_tool', 'fetch_tool_id'])
    df = df.merge(tool_df, on='fetch_tool').drop(columns=['fetch_tool'])
    # remove all _id from the column names
    df.columns = [col.replace('_id', '') for col in df.columns]
    # change the order of the columns so location will be first
    cols = df.columns.tolist()
    # remove the location column from the list
    cols.remove('location')
    cols = ['location'] + cols
    df = df[cols]
    # sort by location and reset index
    df = df.sort_values(by='location').reset_index(drop=True)
    return df


def import_warehouse_positions(path: str = f'{DATA_PATH}/{PLACEMENT}/positions.csv') -> pd.DataFrame:
    df = pd.read_csv(path)
    # replace all column which are objects with the relevant ids from 'src/data/ids/
    location_df = pd.read_csv(f'{DATA_PATH}/ids/location_ids.csv')
    # replace the values under the 'location' column with the corresponding value from the location_df 'location_id' column
    df = df.merge(location_df, on='location').drop(columns=['location'])
    item_df = pd.read_csv(f'{DATA_PATH}/ids/item_ids.csv')
    df = df.merge(item_df[['uuid', 'item_id']], on='uuid').drop(columns=['uuid'])
    df = df.rename(columns={'location_id': 'location', 'item_id': 'uuid'})
    df.sort_values(by=['location', 'uuid'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    # change the order of the columns
    df = df[['location', 'uuid', 'quantity']]
    
    return df

def import_items_data(path: str = f'{DATA_PATH}/items.csv') -> pd.DataFrame:
    df = pd.read_csv(path)
    # replace all column which are objects with the relevant ids from 'src/data/ids/
    item_df = pd.read_csv(f'{DATA_PATH}/ids/item_ids.csv')
    # merge the df with the item_df dataframe on the 'uuid' column, keep only the 'item_id' column
    df = df.merge(item_df[['uuid', 'item_id']], on='uuid').drop(columns=['uuid'])
    putaway_zone_df = pd.read_csv(f'{DATA_PATH}/ids/putaway_zone_ids.csv')
    # do the same for the 'putaway_zone' column
    df = df.merge(putaway_zone_df[['putaway_zone', 'putaway_zone_id']], on='putaway_zone').drop(columns=['putaway_zone'])
    # remove the _id from the column names
    df = df.rename(columns={'putaway_zone_id': 'putaway_zone', 'item_id': 'uuid'})
    df.sort_values(by=['uuid'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    # change the order of the columns to match the original dataframe
    df = df[['uuid', 'putaway_zone', 'item_volume', 'item_attractiveness', 'initial_stock']]
    return df

def import_shipments_data(path: str = f'{DATA_PATH}/shipments.csv', max_date: datetime or None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    # change the 'date' column to datetime
    df['date'] = pd.to_datetime(df['date'])
    if max_date is not None:
        df = df[df['date'] < max_date]
    # replace all column which are objects with the relevant ids from 'src/data/ids/
    item_df = pd.read_csv(f'{DATA_PATH}/ids/item_ids.csv')
    # merge the df with the item_df dataframe on the 'uuid' column, keep only the 'item_id' column
    df = df.merge(item_df[['uuid', 'item_id']], on='uuid').drop(columns=['uuid'])
    # remove the _id from the column names
    df = df.rename(columns={'item_id': 'uuid'})
    df.sort_values(by=['date', 'uuid'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    # change the order of the columns to match the original dataframe
    df = df[['date', 'uuid', 'quantity']]
    return df

def import_orders_data(path: str = f'{DATA_PATH}/orders.csv', shipments_df: pd.DataFrame = pd.DataFrame(), items_df: pd.DataFrame = pd.DataFrame(), max_date: datetime or None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    # change the 'date' column to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    if max_date is not None:
        df = df[df['timestamp'] < max_date]
    
    # first_shipments_df = shipments_df.groupby('uuid').agg({'date': 'min'}).sort_values(by=['date', 'uuid']).reset_index()
    
    # # starting_stock_items will be the items that are not in the first_shipments_df dataframe and have initial_stock > 0, rename the 'item_id' column to 'uuid' and keep only the 'uuid' column
    # starting_stock_items = items_df[~items_df['uuid'].isin(first_shipments_df['uuid']) & (items_df['initial_stock'] > 0)].rename(columns={'item_id': 'uuid'})[['uuid']]
    # # add a date column with the value of 2021-01-01
    # starting_stock_items['date'] = pd.to_datetime('2021-01-01')
    # # concat the in_stock_items to the first_shipments_df, sort by date and uuid, drop duplicates and reset the index
    # existing_items = pd.concat([first_shipments_df, starting_stock_items]).sort_values(by=['date', 'uuid']).drop_duplicates(subset=['uuid'], keep='first').reset_index(drop=True)
    
    # replace all column which are objects with the relevant ids from 'src/data/ids/
    item_ids_df = pd.read_csv(f'{DATA_PATH}/ids/item_ids.csv')
    # merge the df with the item_df dataframe on the 'uuid' column, keep only the 'item_id' column
    df = df.merge(item_ids_df[['uuid', 'item_id']], on='uuid').drop(columns=['uuid'])
    df = df.rename(columns={'item_id': 'uuid'})
    # remove each row which has a 'uuid' which isn't in the existing_items dataframe
    # df = df[df['uuid'].isin(existing_items['uuid'])]
    # remove each row which has a corresponding 'date' which is before the 'date' in the existing_items dataframe
    # df = df.merge(existing_items, on='uuid')
    # df = df[df['timestamp'] >= df['date']]
    # remove the date column
    # df.drop(columns=['date'], inplace=True)
    df.sort_values(by=['timestamp', 'uuid'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    # make order_id equal to the index
    df['order_id'] = df.index
    # change the order of the columns to match the original dataframe
    df = df[['order_id', 'timestamp', 'uuid', 'quantity']]
    return df

def import_tools_data(path: str = f'{DATA_PATH}/fetch_tools_speeds_mean_and_std.csv') -> pd.DataFrame:
    df = pd.read_csv(path)
    # replace all column which are objects with the relevant ids from 'src/data/ids/
    tool_df = pd.read_csv(f'{DATA_PATH}/ids/tool_ids.csv')
    # merge the df with the tool_df dataframe on the 'uuid' column, keep only the 'fetch_tool_id' column
    df = df.merge(tool_df, on='fetch_tool').drop(columns=['fetch_tool'])
    # remove the _id from the column names
    df = df.rename(columns={'fetch_tool_id': 'fetch_tool'})
    df.sort_values(by=['fetch_tool'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    # put the last column first in order
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    
    return df

def import_tool_capacity_data(path: str = f'{DATA_PATH}/tool_capacity.csv') -> pd.DataFrame:
    df = pd.read_csv(path)
    # replace all column which are objects with the relevant ids from 'src/data/ids/
    tool_df = pd.read_csv(f'{DATA_PATH}/ids/tool_ids.csv')
    # merge the df with the tool_df dataframe on the 'uuid' column, keep only the 'fetch_tool_id' column
    df = df.merge(tool_df, on='fetch_tool').drop(columns=['fetch_tool'])
    # remove the _id from the column names
    df = df.rename(columns={'fetch_tool_id': 'fetch_tool'})
    df.sort_values(by=['fetch_tool'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    # put the last column first in order
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    
    return df