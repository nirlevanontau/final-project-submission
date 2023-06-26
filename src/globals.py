import pandas as pd
from data_imports import *

# Global Parameters - Shown here to save code repetition
MAX_DATETIME = None  # Set to None for full simulation
PLACEMENT = 'original' # Set to 'original' or 'placement#' ( # = (1,7) )
DATES_DATA = import_dates_data(MAX_DATETIME)
DATES_DATA_list = DATES_DATA['date'].tolist()
EMPLOYEE_INDEX = 1  # Setting counter for employee id
WAREHOUSE_DATA = import_warehouse_data()
WAREHOUSE_POSITIONS = import_warehouse_positions()
# find the 'location' value of the first row with aisle == 0 in the WAREHOUSE_DATA dataframe, assign as SORT_AREA_LOCATION
SORT_AREA_LOCATION = WAREHOUSE_DATA.loc[WAREHOUSE_DATA['aisle']
                                        == 0, 'location'].iloc[0]
ITEMS_DATA = import_items_data()
SHIPMENTS_DATA = import_shipments_data(max_date=MAX_DATETIME)
ORDERS_DATA = import_orders_data(
    shipments_df=SHIPMENTS_DATA, items_df=ITEMS_DATA, max_date=MAX_DATETIME)
TIMES_DATA = import_tools_data()
TOOL_CAPACITY_DATA = import_tool_capacity_data()
ORDERS_ON_TIME = 0  # Measure for Service Level
ORDERS_ON_TIME_TODAY = 0


ORDERS_LATE = 0  # Measure for Service Level
RETURN_ORDERS = 0
IMPOSSIBLE_ORDERS = 0
# Dictionary for fetch from floor
# FETCH_FROM_FLOOR = {date.date(): 0 for date in DATES_DATA_list}
# Dictionary for fetch from high
# FETCH_FROM_HIGH = {date.date(): 0 for date in DATES_DATA_list}
# Dictionary for fetch from sort
# FETCH_FROM_SORT = {date.date(): 0 for date in DATES_DATA_list}
FETCH_TASK_TIMES = []  # List for fetch task times
WAITING_FOR_DELIVERY = []  # List for waiting for delivery

FETCH_TASKS_columns = ['t_now', 'employee', 'tool', 'type_tool',
                        'task_time', 'num_of_items', 'num_of_aisles']
# DataFrame for fetch tasks
FETCH_TASKS = pd.DataFrame(columns=FETCH_TASKS_columns)


EVENTS_SIM_columns = ['t_now', 'event_type',
                      'employee', 'tool_id', 'tool_type', 'item_in_shipment', 'order_id', 'item_in_order']
# DataFrame for events simulation
EVENTS_SIM = pd.DataFrame(columns=EVENTS_SIM_columns)
