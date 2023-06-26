from globals import *
from classes import *
import pandas as pd
import numpy as np
from datetime import timedelta

# Intialization of the simulation
# Step 1: Set up the initial state of the simulation

# QUEUES:
fetching_queue_columns = ['fetch_tool', 'location', 'aisle', 'cell_attractiveness',
                          'order_id', 'arrival_date', 'uuid', 'units_to_fetch', 'volume_to_fetch', 'time_to_fetch']
# DataFrame for fetching queue
FETCHING_QUEUE_DF = pd.DataFrame(columns=fetching_queue_columns)

fetching_task_queue_columns = [
    'employee', 'order_id', 'location', 'aisle', 'uuid', 'units_to_fetch', 'time_to_fetch']
# DataFrame for fetching task queue
FETCHING_TASK_QUEUE_DF = pd.DataFrame(columns=fetching_task_queue_columns)

# initialize the employees
employee0 = Employee(employee_id=0, tools=[
                     ToolType.CROSS_DOCK.value], employee_rest=1)  # The cross dock employee
# The PALLET JACK employee
employee1 = Employee(employee_id=1, tools=[ToolType.PALLET_JACK.value])
# The PALLET JACK employee
employee2 = Employee(employee_id=2, tools=[ToolType.PALLET_JACK.value])
# The PALLET JACK employee
employee3 = Employee(employee_id=3, tools=[ToolType.PALLET_JACK.value])
# The ORDER PICKER, REACH FORK, PALLET JACK employee
employee4 = Employee(employee_id=4, tools=[
                     ToolType.ORDER_PICKER.value, ToolType.REACH_FORK.value, ToolType.PALLET_JACK.value])
# The ORDER PICKER, REACH FORK, PALLET JACK employee
employee5 = Employee(employee_id=5, tools=[
                     ToolType.ORDER_PICKER.value, ToolType.REACH_FORK.value, ToolType.PALLET_JACK.value])
EMPLOYEES = [employee0, employee1, employee2, employee3,
             employee4, employee5]           # List of employees

# Initialize the tools:


# Function to create a dictionary of speed for the tool
def create_dict_for_tool(tool_type, mean, std):
    current_dict = {}
    mean_vaule = TIMES_DATA.loc[TIMES_DATA['fetch_tool'] == tool_type][mean].iloc[0]
    std_vaule = TIMES_DATA.loc[TIMES_DATA['fetch_tool'] == tool_type][std].iloc[0]
    current_dict["mean"] = mean_vaule
    current_dict["std"] = std_vaule
    return current_dict


# Creation of tools:
PALLET_JACK_horizontal_speed = create_dict_for_tool(
    ToolType.PALLET_JACK.value, 'horizontal_speed_mean', 'horizontal_speed_std')
PALLET_JACK_vertical_speed = create_dict_for_tool(
    ToolType.PALLET_JACK.value, 'vertical_speed_mean', 'vertical_speed_std')
PALLET_JACK_remove_from_shelf_time = create_dict_for_tool(
    ToolType.PALLET_JACK.value, 'remove_from_shelf_time_mean', 'remove_from_shelf_time_std')
REACH_FORK_horizontal_speed = create_dict_for_tool(
    ToolType.REACH_FORK.value, 'horizontal_speed_mean', 'horizontal_speed_std')
REACH_FORK_vertical_speed = create_dict_for_tool(
    ToolType.REACH_FORK.value, 'vertical_speed_mean', 'vertical_speed_std')
REACH_FORK_remove_from_shelf_time = create_dict_for_tool(
    ToolType.REACH_FORK.value, 'remove_from_shelf_time_mean', 'remove_from_shelf_time_std')
ORDER_PICKER_horizontal_speed = create_dict_for_tool(
    ToolType.ORDER_PICKER.value, 'horizontal_speed_mean', 'horizontal_speed_std')
ORDER_PICKER_vertical_speed = create_dict_for_tool(
    ToolType.ORDER_PICKER.value, 'vertical_speed_mean', 'vertical_speed_std')
ORDER_PICKER_remove_from_shelf_time = create_dict_for_tool(
    ToolType.ORDER_PICKER.value, 'remove_from_shelf_time_mean', 'remove_from_shelf_time_std')
PALLET_JACK_capacity = TOOL_CAPACITY_DATA.loc[TOOL_CAPACITY_DATA['fetch_tool'] == ToolType.PALLET_JACK.value]['max_volume'].iloc[0]
REACH_FORK_capacity = TOOL_CAPACITY_DATA.loc[TOOL_CAPACITY_DATA['fetch_tool'] == ToolType.REACH_FORK.value]['max_volume'].iloc[0]
ORDER_PICKER_capacity = TOOL_CAPACITY_DATA.loc[TOOL_CAPACITY_DATA['fetch_tool'] == ToolType.ORDER_PICKER.value]['max_volume'].iloc[0]
tool0 = Tool(tool_id=0, type=ToolType.CROSS_DOCK.value, horizontal_speed={'mean': 0, 'std': 0}, vertical_speed={
             'mean': 0, 'std': 0}, remove_from_shelf_time={'mean': 0, 'std': 0}, capacity=np.inf, left_capacity=np.inf, is_height=False)
tool1 = Tool(tool_id=1, type=ToolType.PALLET_JACK.value, horizontal_speed=PALLET_JACK_horizontal_speed, vertical_speed=PALLET_JACK_vertical_speed,
             remove_from_shelf_time=PALLET_JACK_remove_from_shelf_time, capacity=PALLET_JACK_capacity, left_capacity=PALLET_JACK_capacity, is_height=False)
tool2 = Tool(tool_id=2, type=ToolType.PALLET_JACK.value, horizontal_speed=PALLET_JACK_horizontal_speed, vertical_speed=PALLET_JACK_vertical_speed,
             remove_from_shelf_time=PALLET_JACK_remove_from_shelf_time, capacity=PALLET_JACK_capacity, left_capacity=PALLET_JACK_capacity, is_height=False)
tool3 = Tool(tool_id=3, type=ToolType.PALLET_JACK.value, horizontal_speed=PALLET_JACK_horizontal_speed, vertical_speed=PALLET_JACK_vertical_speed,
             remove_from_shelf_time=PALLET_JACK_remove_from_shelf_time, capacity=PALLET_JACK_capacity, left_capacity=PALLET_JACK_capacity, is_height=False)
tool4 = Tool(tool_id=4, type=ToolType.REACH_FORK.value, horizontal_speed=REACH_FORK_horizontal_speed, vertical_speed=REACH_FORK_vertical_speed,
             remove_from_shelf_time=REACH_FORK_remove_from_shelf_time, capacity=REACH_FORK_capacity, left_capacity=REACH_FORK_capacity, is_height=True)
tool5 = Tool(tool_id=5, type=ToolType.REACH_FORK.value, horizontal_speed=REACH_FORK_horizontal_speed, vertical_speed=REACH_FORK_vertical_speed,
             remove_from_shelf_time=REACH_FORK_remove_from_shelf_time, capacity=REACH_FORK_capacity, left_capacity=REACH_FORK_capacity, is_height=True)
tool6 = Tool(tool_id=6, type=ToolType.ORDER_PICKER.value, horizontal_speed=ORDER_PICKER_horizontal_speed, vertical_speed=ORDER_PICKER_vertical_speed,
             remove_from_shelf_time=ORDER_PICKER_remove_from_shelf_time, capacity=ORDER_PICKER_capacity, left_capacity=ORDER_PICKER_capacity, is_height=True)
tool7 = Tool(tool_id=7, type=ToolType.ORDER_PICKER.value, horizontal_speed=ORDER_PICKER_horizontal_speed, vertical_speed=ORDER_PICKER_vertical_speed,
             remove_from_shelf_time=ORDER_PICKER_remove_from_shelf_time, capacity=ORDER_PICKER_capacity, left_capacity=ORDER_PICKER_capacity, is_height=True)

TOOLS = [tool0, tool1, tool2, tool3, tool4,
         tool5, tool6, tool7]  # List of tools


P = []                                  # Events heap


print("started making orders")
ORDERS = []
# Generate or trigger events
# Creating the orders events


def process_order(row):
    order_id = int(row['order_id'])
    timestamp = row['timestamp']
    items_dict = {row['uuid']: int(row['quantity'])}
    order = Order(order_id, timestamp, items_dict)
    ORDERS.append(order)
    Event(time=timestamp,
          type=EventType.ORDERS_FROM_CUSTOMERS, order=order, P=P)


ORDERS_DATA.apply(process_order, axis=1)
print("finished making orders")

print("strated making shipments")
# Creating the shipments:
SHIPMENTS = []


def process_shipment(row):
    # add 9 hours to the timestamp
    timestamp = row['date'] + timedelta(hours=9)
    items_dict = {row['uuid']: int(row['quantity'])}
    shipment = Shipment(timestamp, items_dict)
    SHIPMENTS.append(shipment)
    Event(time=timestamp,
          type=EventType.DELIVERIES_FROM_SUPPLIERS, shipment=shipment, P=P)


SHIPMENTS_DATA.apply(process_shipment, axis=1)
print("finished making shipments")


print("strated making placing")
shipment_dates_lst = SHIPMENTS_DATA['date'].unique().tolist()
for shipment_date in shipment_dates_lst:
    # If it is Friday
    if DATES_DATA.loc[DATES_DATA['date'] == shipment_date]['short_day'].any():
        timestamp = shipment_date + timedelta(hours=12, minutes=15)
    else:
        timestamp = shipment_date + timedelta(hours=17)
    # Add the event to the event list for the same day
    Event(time=timestamp, type=EventType.PLACING, P=P)
print("finished making placing")


# Creating the first 12:00 event:
# The first date in the dates data
start_date = DATES_DATA.iloc[0]['date'] + timedelta(hours=12)
Event(time=start_date, type=EventType.TWELVE_PM, P=P)
print("finished making 12 pm at", start_date)
