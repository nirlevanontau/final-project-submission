import numpy as np
import heapq
from datetime import datetime, time, timedelta
from globals import *
from classes import *
from setting_sim import *

""" ------- Helper Functions ------- """


def calc_time_to_transfer() -> int:
    """
    Calculate the time to transfer from tool to tool.

    Returns:
        float: Time to transfer from tool to tool.
    """
    return int(np.random.normal(60, 10))


def get_next_day(t_now: datetime) -> datetime:
    """
    Get the next day based on the current date.

    Args:
        t_now (datetime): Current datetime object.

    Returns:
        datetime: Next day as a datetime object.
    """
    current_date = t_now.date()  # Extract the current date
    # from the DATES_DATA dataframe get the next date after current_date as next_day
    try:
        next_day = DATES_DATA.loc[DATES_DATA['date'] >
                                  pd.to_datetime(current_date), 'date'].iloc[0]
    except:
        next_day = t_now + timedelta(days=1)
    # mask = DATES_DATA['date'] == pd.to_datetime(current_date)  # Get the current day index
    # current_day_index = DATES_DATA.index[mask]  # Get the current day index
    # # If the current day index is not empty and the current day index is not the last day
    # if not current_day_index.empty and current_day_index[0] < len(DATES_DATA) - 1:
    #     next_day = DATES_DATA.loc[current_day_index[0] + 1, 'date']
    # else:  # If the current day index is empty or the current day index is the last day
    #     next_day = t_now + timedelta(days=1)  # Get the next day
    return next_day  # Return the next day


def revelant_shipment_in_next_days(t_now: datetime, order: Order):
    """
    Check if there is a relevant shipment in the next 4 days.

    Args:
        t_now (datetime): Current datetime object.
        order (Order): Order object.

    Returns:
        datetime: The date of the relevant shipment if there is one, False otherwise.
    """
    arrival_date = order.arrival_date.date()  # Get the arrival date of the order
    current_date = t_now.date()  # Get the current date
    # Get the number of days between the current date and the arrival date
    count_days_between = len(DATES_DATA.loc[(DATES_DATA['date'] > pd.to_datetime(arrival_date)) &
                                            (DATES_DATA['date'] <= pd.to_datetime(current_date))])
    # Get the number of days until the next shipment
    days_until_supply = 4 - count_days_between

    if days_until_supply > 0:  # If there is a relevant shipment in the next 4 days
        item_id = next(iter(order.items_dict))  # Get the item id
        # Get the number of units needed
        units_needed = order.items_dict[item_id]
        # Get the relevant dates for the next shipment
        relevant_next_dates = [get_next_day(t_now + timedelta(days=i)).date().strftime("%Y-%m-%d")
                               for i in range(days_until_supply)]
        # Get the relevant shipments
        filtered_shipments_df = SHIPMENTS_DATA.loc[(SHIPMENTS_DATA['date'].isin(relevant_next_dates)) &
                                                   (SHIPMENTS_DATA['uuid'] == item_id)]
        # Get the total units in the relevant shipments
        total_units = filtered_shipments_df['quantity'].sum()

        if total_units >= units_needed:  # If the relevant shipments have enough units
            global RETURN_ORDERS  # Get the global variable
            RETURN_ORDERS += 1  # Update the global variable
            # Get the date of the relevant shipment
            relevant_shipment_date = filtered_shipments_df['date'].iloc[0]
            return relevant_shipment_date

    return False


def creation_fetching(t_now: datetime, employee: Employee, tool: Tool, P) -> None:
    """
    This function creates the fetching events.

    Args:
        t_now (datetime): The current time
        employee (Employee): The employee that is fetching the item
        tool (Tool): The tool that is fetching the item
        P : The heap of the events
    """
    if employee.employee_id != 0:  # If the employee is not the sort employee
        employee.employee_status = 1  # The employee is busy
    if tool.tool_id != 0:  # If the tool is not the sort tool
        tool.status = 1  # The tool is busy
    # Get the time to fetch the item
    y = prioritize_items_for_fetching(t_now, employee, tool)
    if y:  # If the time to fetch the item is not 0
        # Create the fetching event
        Event(time=t_now+timedelta(seconds=y), type=EventType.FETCHING, employee=employee,
              tool=tool, P=P)  # Add the event to the event list


def checking_next_fetching_task(t_now: datetime, employee: Employee, current_tool: Tool) -> None:
    """
    Check for the next fetching task or available tools for the employee.

    Args:
        t_now (datetime): Current datetime object.
        employee (Employee): Employee object.
        tool (Tool): Tool object.
    """
    employee.employee_status = 0  # The employee is available
    # Check if there are pending fetching tasks for the tool
    mask = FETCHING_QUEUE_DF['fetch_tool'] == current_tool.type
    if mask.any():  # If there are pending fetching tasks for the tool
        creation_fetching(t_now, employee, current_tool, P)
    else:  # If there are no pending fetching tasks for the tool
        current_tool.status = 0  # The tool is available
        # Check if there are tools to be picked
        next_tool = prioritize_tools_for_queues(employee)
        if next_tool:  # If there are tools to be picked
            if employee.employee_id != 0:  # If the employee is not the sort employee
                employee.employee_status = 1  # The employee is busy
            if next_tool.tool_id != 0:   # If the tool is not the sort tool
                next_tool.status = 1  # The tool is busy
            y = calc_time_to_transfer()  # Get the time to transfer the tools
            # Create the transfer tools event
            Event(time=t_now + timedelta(seconds=y), type=EventType.TRANSFER_TOOLS,
                  employee=employee, tool=next_tool, P=P)


def get_order(order_id):
    """
    This function returns the order according to the order_id

    Args:
        order_id (int): The id of the order

    Returns:
        order (Order): The order with the order_id
    """
    return next((order for order in ORDERS if order.order_id == order_id), False)


def update_FETCHING_TASK_QUEUE_DF(index: int, row: pd.Series, employee: Employee, tool: Tool, relevant_items: pd.DataFrame) -> pd.DataFrame:
    """
    Update the fetching task queue DataFrame with new items for fetching.

    Args:
        index (int): Index of the row to be updated.
        row: Row containing the item details.
        employee (Employee): Employee object.
        tool (Tool): Tool object.
        relevant_items: DataFrame containing relevant items for fetching.

    Returns:
        DataFrame: Updated relevant_items DataFrame after removing the specified index.
    """
    global FETCHING_QUEUE_DF, FETCHING_TASK_QUEUE_DF
    # Create a dictionary with the new item details
    new_items_for_fetching_task_df = {
        'employee': employee.employee_id,
        'order_id': row['order_id'],
        'location': row['location'],
        'aisle': row['aisle'],
        'uuid': row['uuid'],
        'units_to_fetch': row['units_to_fetch'],
        'time_to_fetch': row['time_to_fetch']
    }
    # Append the new item to the fetching task queue DataFrame
    index_to_add = (max(FETCHING_TASK_QUEUE_DF.index) +
                    1) if (len(FETCHING_TASK_QUEUE_DF) > 0) else 0
    FETCHING_TASK_QUEUE_DF.loc[index_to_add] = new_items_for_fetching_task_df
    # Update the left capacity of the tool based on the volume of the fetched item
    tool.left_capacity -= row['volume_to_fetch']
    # Remove the item from the relevant_items DataFrame
    relevant_items.drop(index, inplace=True)
    # Remove the item from the FETCHING_QUEUE_DF DataFrame
    FETCHING_QUEUE_DF = FETCHING_QUEUE_DF.drop(index)
    return relevant_items


def calculate_time_in_aisle(current_row: pd.Series, next_row: pd.Series, tool: Tool) -> float:
    """
    Calculate the time it takes to move between two locations in the aisle.

    Args:
        current_row (pd.Series): Current row containing the location details.
        next_row (pd.Series): Next row containing the location details.
        tool (Tool): Tool object.

    Returns:
        float: Time in seconds.
    """
    # Get the x-coordinate of the current and next locations
    from_loc_x = current_row['x_length']
    to_loc_x = next_row['x_length']
    # Get the horizontal speed of the tool using a normal distribution
    speed_x = np.random.normal(
        tool.horizontal_speed['mean'], tool.horizontal_speed['std'])
    # Get the z-coordinate of the current and next locations
    from_loc_z = current_row['z_height']
    to_loc_z = next_row['z_height']
    # Get the vertical speed of the tool using a normal distribution
    speed_z = np.random.normal(
        tool.vertical_speed['mean'], tool.vertical_speed['std'])
    # Calculate the time it takes to move between locations based on the speeds
    if not pd.isnull(from_loc_x):  # If the from_loc_x is not null
        if speed_x > 0 and speed_z > 0:  # If the speeds are positive
            # Return the maximum time between the horizontal and vertical movements
            return max(((abs(to_loc_x-from_loc_x))/speed_x), ((abs(to_loc_z-from_loc_z))/speed_z))
    return 0


""" ------- Huristic functions ------- """


def prioritize_locations_for_fetching(t_now: datetime, order: Order) -> None:
    """
    Prioritize locations for fetching based on order requirements and warehouse data.

    Args:
        t_now (datetime): Current datetime.
        order (Order): Order object.
    """
    item_id = next(iter(order.items_dict))  # Get the first item in the order
    # Get the number of units left to fetch
    units_left_to_fetch = order.items_dict[item_id]
    # Get the relevant positions of the item
    # try:
    relevant_positions = WAREHOUSE_POSITIONS.loc[WAREHOUSE_POSITIONS['uuid'] == item_id, [
        'location', 'uuid', 'quantity']]
    possible_positions = pd.merge(relevant_positions, WAREHOUSE_DATA[['location', 'aisle', 'cell_attractiveness', 'fetch_tool']],
                                  on='location', how='left')
    # Sort the possible positions by cell attractiveness
    possible_positions.sort_values(
        by=['cell_attractiveness'], ascending=False, inplace=True)
    # Get the item volume
    unit_volume = ITEMS_DATA.loc[ITEMS_DATA['uuid']
                                 == item_id, 'item_volume'].iloc[0]
    # Calculate the number of units that can be fetched by the tool
    fetch_time_data = TIMES_DATA.loc[TIMES_DATA['fetch_tool']
                                     == possible_positions['fetch_tool'].values[0]]
    fetch_time_mean = fetch_time_data['remove_from_shelf_time_mean'].values[0]
    fetch_time_std = fetch_time_data['remove_from_shelf_time_std'].values[0]
    # Calculate the time it takes to fetch the item
    fetch_time_array = np.random.normal(
        fetch_time_mean, fetch_time_std, size=len(possible_positions))
    # Create a DataFrame to store the items to be fetched
    index_to_add = len(FETCHING_QUEUE_DF)
    current_date = t_now.date()
    # Iterate over the possible positions
    for i, row in possible_positions.iterrows():  # Iterate over the possible positions
        # Get the number of units to fetch
        units_to_fetch = min(row['quantity'], units_left_to_fetch)
        # Update the number of units left to fetch
        units_left_to_fetch -= units_to_fetch
        # Update the quantity of the item in the warehouse
        WAREHOUSE_POSITIONS.loc[(WAREHOUSE_POSITIONS['location'] == row['location']) &
                                (WAREHOUSE_POSITIONS['uuid'] == row['uuid']), 'quantity'] -= units_to_fetch
        # Update the available volume of the item in the warehouse
        volume_to_fetch = unit_volume * units_to_fetch
        WAREHOUSE_DATA.loc[WAREHOUSE_DATA['location'] ==
                           row['location'], 'available_volume'] += volume_to_fetch
        # Calculate the time it takes to fetch the item
        time_to_fetch = 0 if row['fetch_tool'] == ToolType.CROSS_DOCK.value else fetch_time_array[i]
        # Add the item to the FETCHING_QUEUE_DF DataFrame
        FETCHING_QUEUE_DF.loc[index_to_add] = {
            'fetch_tool': row['fetch_tool'],
            'location': row['location'],
            'aisle': row['aisle'],
            'cell_attractiveness': row['cell_attractiveness'],
            'order_id': order.order_id,
            'arrival_date': order.arrival_date,
            'uuid': row['uuid'],
            'units_to_fetch': units_to_fetch,
            'volume_to_fetch': volume_to_fetch,
            'time_to_fetch': time_to_fetch
        }
        index_to_add += 1  # Update the index to add
        # Update the number of units fetched by the tool
        # if row['fetch_tool'] == ToolType.PALLET_JACK.value:
        #     FETCH_FROM_FLOOR[current_date] += 1
        # elif row['fetch_tool'] == ToolType.CROSS_DOCK.value:
        #     FETCH_FROM_SORT[current_date] += 1
        # else:
        #     FETCH_FROM_HIGH[current_date] += 1
        # Check if all units were fetched
        if units_left_to_fetch == 0:
            break
    # Sort the fetching queue by cell attractiveness
    FETCHING_QUEUE_DF.sort_values(
        'cell_attractiveness', ascending=False, inplace=True)


def prioritize_tools_for_queues(employee: Employee):
    """
    Prioritize tools for fetching queues based on employee's available tools.

    Args:
        employee (Employee): Employee object.

    Returns:
        Tool or False: The prioritized tool for the fetching queues, or False if no available tool is found.
    """
    employee_tool_types = set(
        employee.tools)  # Get the employee's available tools
    # Get the available tools
    available_tools = {tool.type: tool for tool in TOOLS if tool.status == 0}
    # Iterate over the employee's available tools
    for tool_type in employee_tool_types:
        # Check if the tool is available and in the fetching queue
        if tool_type in available_tools and any(FETCHING_QUEUE_DF['fetch_tool'] == tool_type):
            return available_tools[tool_type]  # Return the tool
    # Return False if no available tool is found
    return False


def prioritize_items_for_fetching(t_now: datetime, employee: Employee, tool: Tool) -> float:
    """
    Prioritizes items for fetching based on employee and tool constraints.

    Args:
        t_now (datetime): Current datetime.
        employee (Employee): Employee object.
        tool (Tool): Tool object.

    Returns:
        float: Total time required for fetching tasks.
    """
    # Initialize tool's left capacity
    tool.left_capacity = tool.capacity
    # Filter relevant items for the tool type
    relevant_items = FETCHING_QUEUE_DF[FETCHING_QUEUE_DF['fetch_tool'] == tool.type]
    # Loop until no more relevant items or tool capacity is reached
    while not relevant_items.empty:  # Loop until no more relevant items or tool capacity is reached
        # Filter relevant items that fit within the tool's capacity
        relevant_items = relevant_items[relevant_items['volume_to_fetch']
                                        <= tool.left_capacity]
        # Check if there are relevant items to fetch
        if not relevant_items.empty:  # If there are relevant items to fetch
            # Find the earliest arrival item
            # Get the index of the item with the earliest arrival date
            earliest_index = relevant_items['arrival_date'].idxmin()
            # Fetch the current item
            current_item = relevant_items.loc[earliest_index]
            current_aisle = current_item['aisle']  # Fetch the current aisle
            # Update relevant items and task queue
            relevant_items = update_FETCHING_TASK_QUEUE_DF(
                earliest_index, current_item, employee, tool, relevant_items)
            # Get relevant items in the same aisle
            relevant_items_in_same_aisle = relevant_items.loc[relevant_items['aisle'] == current_aisle]
            # Iterate over relevant items in the same aisle
            for index, row in relevant_items_in_same_aisle.iterrows():
                if (row['volume_to_fetch'] <= tool.left_capacity).any():
                    relevant_items = update_FETCHING_TASK_QUEUE_DF(
                        index, row, employee, tool, relevant_items)
                else:  # This item is not relevant to the current fetch task
                    relevant_items.drop(index, inplace=True)

    # Calculate the total time for the fetch task
    fetching_task_queue_of_employee = FETCHING_TASK_QUEUE_DF.loc[
        FETCHING_TASK_QUEUE_DF['employee'] == employee.employee_id]
    if not fetching_task_queue_of_employee.empty:
        number_items_in_fetch_task = len(fetching_task_queue_of_employee)
        time_fetch_task = fetching_task_queue_of_employee['time_to_fetch'].sum(
        )

        # Sort aisles and calculate time between them
        aisles_lst = fetching_task_queue_of_employee['aisle'].unique().tolist()
        number_aisles_in_fetch_task = len(aisles_lst)
        aisles_lst.sort()
        time_to_first_aisle_from_io = (
            WAREHOUSE_DATA.loc[WAREHOUSE_DATA['aisle'] == aisles_lst[0]]['distance_from_io_to_aisle']).unique()
        time_from_last_aisle_to_io = (
            WAREHOUSE_DATA.loc[WAREHOUSE_DATA['aisle'] == aisles_lst[-1]]['distance_from_io_to_aisle']).unique()
        time_fetch_task += time_to_first_aisle_from_io
        time_fetch_task += time_from_last_aisle_to_io

        # Calculate aisle travel times
        fetching_task_queue_with_cells = pd.merge(fetching_task_queue_of_employee, WAREHOUSE_DATA,
                                                  on=['location', 'aisle'],
                                                  how='left')[["aisle", 'x_length', 'y_width', 'z_height', 'fetch_tool']]
        fetching_task_queue_with_cells.sort_values(
            'x_length', ascending=True, inplace=True)
        aisles_y = []
        for aisle_number in aisles_lst:
            aisles_y.append(
                fetching_task_queue_with_cells['y_width'].iloc[0])
            current_aisle_task = fetching_task_queue_with_cells.loc[
                fetching_task_queue_with_cells['aisle'] == int(aisle_number)].reset_index(drop=True)
            io_aisle = pd.DataFrame({"aisle": pd.NA, 'x_length': [0], 'y_width': pd.NA, 'z_height': [0],
                                     'fetch_tool': pd.NA})
            current_aisle_task = pd.concat(
                [io_aisle, current_aisle_task, io_aisle], ignore_index=True)
            for current_row, next_row in zip(current_aisle_task.iterrows(), current_aisle_task.iloc[1:].iterrows()):
                current_index, current_data = current_row
                next_index, next_data = next_row
                current_aisle_task['aisle_travel_time'] = calculate_time_in_aisle(
                    current_data, next_data, tool)
            time_fetch_task += current_aisle_task['aisle_travel_time'].sum()

        # Calculate time between aisles
        speed_y = np.random.normal(
            tool.horizontal_speed['mean'], tool.horizontal_speed['std'])
        time_between_aisles = [(((abs(from_y - to_y)) / speed_y) if speed_y > 0 else 0)
                               for from_y, to_y in zip(aisles_y[:-1], aisles_y[1:])]
        time_fetch_task += sum(time_between_aisles)
        # Append fetch task time to FETCH_TASK_TIMES list
        time_fetch_task[0] = round(time_fetch_task[0])
        FETCH_TASK_TIMES.append(time_fetch_task[0])
        new_FETCH_TASKE_row = {
            't_now': t_now,
            'employee': employee.employee_id,
                               'tool': tool.tool_id,
                               'type_tool': tool.type,
                               'task_time': time_fetch_task[0],
                               'num_of_items': number_items_in_fetch_task,
                               'num_of_aisles': number_aisles_in_fetch_task}
        # Get the index to add the new row
        index_to_add = (max(FETCH_TASKS.index) +
                        1) if (len(FETCH_TASKS) > 0) else 0
        # Add the new row to the FETCH_TASKS dataframe
        FETCH_TASKS.loc[index_to_add] = new_FETCH_TASKE_row
        return time_fetch_task[0]  # Return the time of the fetch task
    else:
        employee.employee_status = 0  # Update employee status to 0 (available)
        tool.tool_status = 0  # Update tool status to 0 (available)
        return 0  # Return 0 (no fetch task)


def placing_huristic(item_id: int, amount_to_place: int) -> None:
    """
    Apply the placing heuristic to determine the optimal placement of an item.

    Args:
        item_id (int): The ID of the item to be placed.
        amount_to_place (int): The number of units of the item to be placed.
    """
    # Get unit putaway zone and unit volume for the item
    unit_putaway_zone = ITEMS_DATA.loc[ITEMS_DATA['uuid']
                                       == item_id, 'putaway_zone'].values[0]
    unit_volume = ITEMS_DATA.loc[ITEMS_DATA['uuid']
                                 == item_id, 'item_volume'].values[0]
    global WAREHOUSE_POSITIONS, WAREHOUSE_POSITIONS
    while amount_to_place > 0:  # While there are still units to place
        # Get all available cells with enough volume to place the unit
        positions_cells_without_sort = WAREHOUSE_DATA.loc[WAREHOUSE_DATA['location']
                                                          != SORT_AREA_LOCATION]
        # Calculate the volume left to place
        volume_left_to_place = unit_volume * amount_to_place
        # Get all available cells with enough volume to place the unit
        positions_cells_available = positions_cells_without_sort[
            positions_cells_without_sort['available_volume'] >= unit_volume]
        positions_cells_merged_available = pd.merge(
            WAREHOUSE_POSITIONS[['location', 'uuid']],
            positions_cells_available[[
                'location', 'putaway_zone', 'available_volume', 'cell_attractiveness']],
            on='location',
            how='right'
        )
        if item_id in positions_cells_merged_available['uuid'].values:
            # Cells which include the item
            possible_cells = positions_cells_merged_available.loc[
                positions_cells_merged_available['uuid'] == item_id]
        elif (unit_putaway_zone) in positions_cells_merged_available['putaway_zone'].values:
            # Cells with the same putaway zone
            possible_cells = positions_cells_merged_available.loc[
                positions_cells_merged_available['putaway_zone'] == unit_putaway_zone]
        else:
            # All other possible cells
            possible_cells = positions_cells_merged_available

        # Check if there is a cell with enough volume to place all the units left to place
        is_cell_available_for_all_volume_left_to_place = any(
            possible_cells['available_volume'] >= volume_left_to_place)
        # If there is a cell with enough volume to place all the units left to place
        if is_cell_available_for_all_volume_left_to_place:
            # Get all possible cells with enough volume to place all the units left to place
            current_possible_cells = possible_cells[possible_cells['available_volume']
                                                    >= volume_left_to_place]
        else:
            # Get all possible cells with enough volume to place the unit
            current_possible_cells = possible_cells
        # Get the most attractive cell
        most_attractive_cell_index = current_possible_cells['cell_attractiveness'].idxmax(
        )
        most_attractive_cell = current_possible_cells.loc[most_attractive_cell_index]
        max_amount_to_place = most_attractive_cell['available_volume'] // unit_volume
        # If there is a cell with enough volume to place all the units left to place
        amount_to_place_here = min(amount_to_place, max_amount_to_place)
        # Calculate the volume to place in the cell
        volume_to_place_here = unit_volume * amount_to_place_here
        # Update the available volume of the cell
        filtered_df = WAREHOUSE_POSITIONS[
            (WAREHOUSE_POSITIONS['location'] == most_attractive_cell['location']) &
            (WAREHOUSE_POSITIONS['uuid'] == item_id)
        ]
        # If the cell already has the item
        if not filtered_df.empty:
            WAREHOUSE_POSITIONS.loc[
                (WAREHOUSE_POSITIONS['uuid'] == item_id) &
                (WAREHOUSE_POSITIONS['location'] ==
                 most_attractive_cell['location']),
                'quantity'
            ] += amount_to_place  # Update the quantity of the item in the cell
        else:
            # Add the item to the cell
            # create a new df row based on the WAREHOUSE_POSITIONS df columns,
            # with uuid = item_id, location = most_attractive_cell['location'] and quantity = amount_to_place
            new_position = pd.DataFrame(
                [[most_attractive_cell['location'], item_id, amount_to_place]],
                columns=WAREHOUSE_POSITIONS.columns
            )
            # concat the new row to the WAREHOUSE_POSITIONS df
            WAREHOUSE_POSITIONS = pd.concat(
                [WAREHOUSE_POSITIONS, new_position], ignore_index=True)
        # Update the available volume of the cell
        WAREHOUSE_DATA.loc[WAREHOUSE_DATA['location'] ==
                           most_attractive_cell['location'], 'available_volume'] -= volume_to_place_here

        amount_to_place -= amount_to_place_here
        volume_left_to_place -= volume_to_place_here

# ------------- Event functions -------------:


def deliveries_from_suppliers(t_now: datetime, shipment: Shipment, P) -> None:
    """
    This function handels the deliveries from suppliers events.

    Args:
        t_now (datetime): The current time.
        P: The heap of the events.
        shipment (Shipment): The shipment that is being delivered.

    Iterates over the items in the shipment and adds them to the warehouse with the location named 'Sort'.
    Creates placing events based on the day of the week.

    - `item`: Item ID from the shipment.
    - `amount`: Amount of the item in the shipment.
    - `row_to_add`: A dictionary representing the row to add to the warehouse DataFrame.
      - 'uuid': Item ID.
      - 'quantity': Amount of the item.
      - 'location': "Sort".
    - Appends `row_to_add` to the `WAREHOUSE_POSITIONS` DataFrame.
    """
    for item, amount in shipment.items_dict.items():
        # Create a row to add to the warehouse
        row_to_add = {'uuid': item, 'quantity': amount,
                      'location': SORT_AREA_LOCATION}
        index_to_add = (max(WAREHOUSE_POSITIONS.index) +
                        1) if (len(WAREHOUSE_POSITIONS) > 0) else 0
        WAREHOUSE_POSITIONS.loc[index_to_add] = row_to_add


def placing() -> None:
    """
    This function handels the placing events.

    - `warhouse_sort_area`: Filtered DataFrame representing the warehouse positions with the location named 'Sort'.
    - `merged_df`: DataFrame obtained by merging the warehouse sort area with the items data based on the 'uuid' column.
    - `rearranged_df`: DataFrame sorted by 'location' (ascending) and 'item_attractiveness' (descending).

    Iterates over the rows of `rearranged_df` and calls the `placing_huristic` function for each row.
    - `index`: Index of the current row.
    - `row`: A row of `rearranged_df` containing the 'uuid' and 'quantity' values.
    """
    global WAREHOUSE_POSITIONS, SORT_AREA_LOCATION, ITEMS_DATA
    # Filter the items to place from SORT01
    warehouse_sort_area = WAREHOUSE_POSITIONS[WAREHOUSE_POSITIONS['location']
                                              == SORT_AREA_LOCATION]
    # Merge with ITEMS_DATA to get additional item information
    merged_df = pd.merge(warehouse_sort_area, ITEMS_DATA,
                         on='uuid', how='left').sort_values(
        by='item_attractiveness',
        ascending=False
    )
    # Update the warehouse positions and drop from SORT01
    # mask = (WAREHOUSE_POSITIONS['location'] == SORT_AREA_LOCATION) & (
    #     WAREHOUSE_POSITIONS['uuid'].isin(merged_df['uuid']))
    # WAREHOUSE_POSITIONS.loc[mask,
    #                         'quantity'] -= merged_df['quantity'].values
    # WAREHOUSE_POSITIONS = WAREHOUSE_POSITIONS.dropna(subset=['quantity'])
    # Perform the placing heuristic for each item
    merged_df.apply(lambda row: placing_huristic(
        row['uuid'], row['quantity']), axis=1)
    # remove from WAREHOUSE_POSITIONS every row with location == SORT_AREA_LOCATION and quantity == 0
    WAREHOUSE_POSITIONS = WAREHOUSE_POSITIONS[~(
        (WAREHOUSE_POSITIONS['location'] == SORT_AREA_LOCATION) & (WAREHOUSE_POSITIONS['quantity'] == 0))]


def orders_from_customers(t_now: datetime, order: Order, P) -> None:
    """
    Process orders from customers.

    Args:
        t_now (datetime): The current time.
        next_day (datetime): The next day.
        order (Order): The order to process.
        P: Placeholder for an unknown parameter.
    """
    global IMPOSSIBLE_ORDERS, RETURN_ORDERS
    item = order.items_dict.keys()  # Get the items in the order as a list
    # Get the quantities of the items as a list
    quantities = order.items_dict.values()

    # Check if the items are in the warehouse
    mask = WAREHOUSE_POSITIONS['uuid'].isin(item)
    available_items = WAREHOUSE_POSITIONS[mask]

    if not available_items.empty:  # If the items are in the warehouse
        # Get the total quantities of the items in the warehouse
        total_quantities_in_warehouse = available_items.groupby('uuid')[
            'quantity'].sum()
        order_quantities_available = pd.Series(quantities, index=item).reindex(
            total_quantities_in_warehouse.index, fill_value=0)
        # Check if the order quantities are less than or equal to the warehouse quantities
        mask_available = order_quantities_available <= total_quantities_in_warehouse
        order_quantities_satisfied = order_quantities_available[mask_available]
        if not order_quantities_satisfied.empty:
            # We have all units of these items
            if order.order_id in WAITING_FOR_DELIVERY:
                # Remove the order from the waiting for delivery list
                WAITING_FOR_DELIVERY.remove(order.order_id)
            # Prioritize locations for fetching
            prioritize_locations_for_fetching(t_now, order)
            for employee in EMPLOYEES:  # Iterate over the employees
                if employee.employee_status == 0:  # If the employee is available
                    tool = prioritize_tools_for_queues(
                        employee)  # Prioritize tools for queues
                    if tool:  # If the employee has a tool
                        # Create a fetching event
                        creation_fetching(t_now, employee, tool, P)

        # We do not have all units of these items
        else:
            relevant_next_event_date = revelant_shipment_in_next_days(
                t_now, order)  # Get the relevant next event date
            # If there is a relevant next event date
            if relevant_next_event_date:
                if order.order_id not in WAITING_FOR_DELIVERY:
                    WAITING_FOR_DELIVERY.append(order.order_id)
                # Create an orders from customers event
                Event(time=t_now.replace(year=relevant_next_event_date.year, month=relevant_next_event_date.month,
                                         day=relevant_next_event_date.day, hour=9, minute=1),
                      type=EventType.ORDERS_FROM_CUSTOMERS, order=order, P=P)
                RETURN_ORDERS += 1
            else:
                IMPOSSIBLE_ORDERS += 1
    # We do not have any units of these items
    else:
        relevant_next_event_date = revelant_shipment_in_next_days(t_now, order)
        # If there is a relevant next event date
        if relevant_next_event_date:
            if order.order_id not in WAITING_FOR_DELIVERY:
                # Add the order to the waiting for delivery list
                WAITING_FOR_DELIVERY.append(order.order_id)
            # Create an orders from customers event
            Event(time=t_now.replace(year=relevant_next_event_date.year, month=relevant_next_event_date.month,
                                     day=relevant_next_event_date.day, hour=9, minute=1),
                  type=EventType.ORDERS_FROM_CUSTOMERS, order=order, P=P)
            RETURN_ORDERS += 1
        else:
                IMPOSSIBLE_ORDERS += 1


def rest(t_now: datetime, employee: Employee, P) -> None:
    """
    This function handels the rest events.

    Args:
        t_now (datetime): The current time
        P : The heap of the events
        employee (Employee): The employee that is resting
    """
    employee.employee_status = 0  # The employee is available
    # Check if there are tools to be picked
    tool = prioritize_tools_for_queues(employee)
    if tool:  # If there are tools to be picked
        creation_fetching(t_now=t_now, employee=employee, tool=tool, P=P)

def handle_employee_fetching_tasks(row: pd.Series) -> None:
    order_id = row['order_id']  # Get the order
    order = get_order(order_id)  # Get the order
    uuid = row['uuid']  # Get the UUID
    # update the quantity left to fetch
    order.items_dict[uuid] -= row['units_to_fetch']
    quantity_left_to_fetch = order.items_dict[uuid]
    if quantity_left_to_fetch == 0:  # If there is no quantity left to fetch
        arrival_date = order.arrival_date.date()
        current_date = t_now.date()
        count_days_between = len(DATES_DATA[(DATES_DATA['date'] > pd.to_datetime(arrival_date)) & (DATES_DATA['date'] <=
                                                                                                    pd.to_datetime(current_date))])
        order.waiting_days_for_delivery = count_days_between
        # Update Service Level
        if order.waiting_days_for_delivery > 4:
            global ORDERS_LATE
            ORDERS_LATE += 1
        else:
            global ORDERS_ON_TIME, ORDERS_ON_TIME_TODAY
            ORDERS_ON_TIME += 1
            ORDERS_ON_TIME_TODAY += 1

def fetching(t_now: datetime, employee: Employee, tool: Tool, P) -> None:
    """
    This function handels the fetching events.

    Args:
        t_now (datetime): The current time
        P : The heap of the events
        employee (Employee): The employee that is fetching
        tool (Tool): The tool that is being fetched
    """
    # Get the last fetching task assigned to the employee from the fetching task queue DataFrame.
    global FETCHING_TASK_QUEUE_DF
    last_employee_fetching_task = FETCHING_TASK_QUEUE_DF.loc[
        FETCHING_TASK_QUEUE_DF['employee'] == employee.employee_id]
    # apply handle_employee_fetching_tasks on last_employee_fetching_task
    last_employee_fetching_task.apply(handle_employee_fetching_tasks, axis=1)
    # remove all the rows from FETCHING_TASK_QUEUE_DF with employee_id = employee.employee_id
    FETCHING_TASK_QUEUE_DF = FETCHING_TASK_QUEUE_DF.drop(FETCHING_TASK_QUEUE_DF[FETCHING_TASK_QUEUE_DF['employee'] == employee.employee_id].index).reset_index(drop=True)

    current_date = t_now.date()
    # If it is Friday
    if DATES_DATA.loc[DATES_DATA['date'] == pd.to_datetime(current_date)]['short_day'].any():
        if 8 <= t_now.hour <= 12:  # work hours
            checking_next_fetching_task(t_now, employee, tool)

    else:  # If it is not Friday
        if 8 <= t_now.hour <= 17:
            if employee.employee_rest == 1:  # If the employee rested
                checking_next_fetching_task(t_now, employee, tool)

            else:  # If the employee did not rest
                if t_now.hour > 12:  # employee needs rest
                    employee.employee_rest = 1  # The employee is resting
                    tool.status = 0  # The tool is available
                    # Add the event to the event list
                    Event(time=t_now+timedelta(hours=1),
                          type=EventType.REST, employee=employee, P=P)
                else:  # employee does not need rest
                    checking_next_fetching_task(t_now, employee, tool)


def tranfer_tools(t_now: datetime, employee: Employee, tool: Tool, P) -> None:
    """
    This function creates the transfer tools events.

    Args:
        t_now (datetime): The current time
        P : The heap of the events
        tool (Tool): The tool that is being transferred
        employee (Employee): The employee that is transferred to the tool
    """
    creation_fetching(t_now, employee, tool, P)


def twelve_pm(t_now: datetime, next_day: datetime, P) -> None:
    """
    This function creates the twelve pm events.
    Args:
        t_now (datetime): The current time
        next_day (datetime): The next day
        P : The heap of the events
    """
    current_date = t_now.date()
    # If it is not  Friday
    if not DATES_DATA.loc[DATES_DATA['date'] == pd.to_datetime(current_date)]['short_day'].any():
        available_employees = [
            employee for employee in EMPLOYEES if employee.employee_status == 0]
        for employee in available_employees:
            if employee.employee_id != 0:
                employee.employee_rest = 1  # The employee is resting
                # Add the event to the event list
                Event(time=t_now + timedelta(hours=1),
                      type=EventType.REST, employee=employee, P=P)

    # Add the event to the event list
    next_day = next_day.replace(hour=12, minute=0)
    # Step 2: Define the simulation logic
    Event(time=next_day, type=EventType.TWELVE_PM, P=P)


# last date for stopping the simulation
last_date = DATES_DATA['date'].max()

event = heapq.heappop(P)
t_now = event.time
current_date = t_now.date()
# Step 3: Run the simulation
print("Starting simulation...")

while (P) and (t_now <= last_date):
    if current_date != t_now.date():
        # from the DAILY_SERVICE_RATE list, find the dictionary with the date of the current day, and add the number of orders on time today
        # DAILY_SERVICE_RATE.loc[DAILY_SERVICE_RATE['date'] == pd.to_datetime(current_date), 'orders_on_time'] = ORDERS_ON_TIME_TODAY
        ORDERS_ON_TIME_TODAY = 0

    if event.type == EventType.DELIVERIES_FROM_SUPPLIERS:
        # print("entring deliveries from suppliers")
        shipment = event.shipment   # Get the shipment
        deliveries_from_suppliers(t_now, shipment, P)

    elif event.type == EventType.TWELVE_PM:
        # print("entering twelve_pm event")
        next_day = get_next_day(t_now)
        twelve_pm(t_now, next_day, P)

    elif event.type == EventType.FETCHING:
        # print("entering fetching event")
        employee = event.employee  # Get the employee
        tool = event.tool          # Get the tool
        items_to_fetch = event.items_to_fetch  # Get the items to fetch
        fetching(t_now, employee, tool, P)  # add fetching_task_queue?

    elif event.type == EventType.TRANSFER_TOOLS:
        # print("entering transfering tools event")
        employee = event.employee  # Get the employee
        tool = event.tool          # Get the tool
        tranfer_tools(t_now, employee, tool, P)

    elif event.type == EventType.REST:
        # print("entering rest event")
        employee = event.employee  # Get the employee
        rest(t_now, employee, P)

    elif event.type == EventType.ORDERS_FROM_CUSTOMERS:
        # print("entering orders_from_costumers event")
        order = event.order        # Get the order
        orders_from_customers(t_now, order, P)

    elif event.type == EventType.PLACING:
        # print("entering placing event")
        # placing(t_now, P)
        placing()
    
    
    current_date = t_now.date()
    # create a string comprised of event.type.name and pad it to 20 characters
    last_event_type = event.type
    # Check in order to reduce the amount of useless prints
    if last_event_type != EventType.DELIVERIES_FROM_SUPPLIERS:
        orders_so_far_count = len(
            ORDERS_DATA.loc[ORDERS_DATA['timestamp'] <= t_now])
        # count the number of orders which are on this current date (including the current time)
        orders_on_this_date_so_far = len(
            ORDERS_DATA.loc[(ORDERS_DATA['timestamp'] <= t_now) & (ORDERS_DATA['timestamp'].dt.date == current_date)])
        print(
            t_now,
            f"{event.type.name:<25}",
            "Today:", f"{ORDERS_ON_TIME_TODAY:<6}/{orders_on_this_date_so_far:<6}",
            "Orders:", f"{orders_so_far_count-IMPOSSIBLE_ORDERS:<6} ({orders_so_far_count:<6})",
            "OT:", f"{ORDERS_ON_TIME:<6}",
            "SR:", f"{round(ORDERS_ON_TIME/max(orders_so_far_count-IMPOSSIBLE_ORDERS,1), 2):<6}",
            "FTQ:", f"{len(FETCHING_TASK_QUEUE_DF):<6}",
            "FQ:", f"{len(FETCHING_QUEUE_DF):<6}",
            "R:", f"{RETURN_ORDERS:<6}",
            "IM:", f"{IMPOSSIBLE_ORDERS}"
        )
    new_event = {'t_now': t_now,
                 'event_type': event.type,
                 'employee': event.employee.employee_id if event.employee else pd.NA,
                 'tool_id': event.tool.tool_id if event.tool else pd.NA,
                 'tool_type': event.tool.type if event.tool else pd.NA,
                 'item_in_shipment': next(iter(event.shipment.items_dict)) if event.shipment else pd.NA,
                 'order_id': event.order.order_id if event.order else pd.NA,
                 'item_in_order': next(iter(event.order.items_dict)) if event.order else pd.NA}
    index_to_add = (max(EVENTS_SIM.index) + 1) if (len(EVENTS_SIM) > 0) else 0
    EVENTS_SIM.loc[index_to_add] = new_event

    previous_event_date = t_now.date()  # Extract the current date
    event = heapq.heappop(P)
    t_now = event.time

    if t_now.date() != previous_event_date:
        # print("RESET")
        FETCHING_TASK_QUEUE_DF = FETCHING_TASK_QUEUE_DF[0:0]
        for employee in EMPLOYEES:
            employee.employee_status = 0  # The employee is available
            if employee.employee_id != 0:
                employee.employee_rest = 0  # The employee is not resting
        for tool in TOOLS:
            tool.status = 0  # The tool is available
print("end simulation")

global PLACEMENT

# create the 'src/{PLACEMENT}/results'  directory if it doesn't exist
if not os.path.exists(f"src/{PLACEMENT}/results"):
    os.makedirs(f"src/{PLACEMENT}/results")

measures_df = pd.DataFrame(columns=['last_date', 'ORDERS_ON_TIME', 'ORDERS_LATE', 'RETURN_ORDERS',
                           'WAITING_FOR_DELIVERY_len', 'FETCHING_QUEUE_DF', 'FETCHING_TASK_QUEUE_DF', 'IMPOSSIBLE_ORDERS'])
measures_df["last_date"] = [last_date]
measures_df["ORDERS_ON_TIME"] = [ORDERS_ON_TIME]
measures_df["ORDERS_LATE"] = [ORDERS_LATE]
measures_df["RETURN_ORDERS"] = [RETURN_ORDERS]
measures_df["WAITING_FOR_DELIVERY_len"] = [len(WAITING_FOR_DELIVERY)]
measures_df["FETCHING_QUEUE_DF"] = [len(FETCHING_QUEUE_DF)]
measures_df["FETCHING_TASK_QUEUE_DF"] = [len(FETCHING_TASK_QUEUE_DF)]
measures_df["IMPOSSIBLE_ORDERS"] = [IMPOSSIBLE_ORDERS]
measures_df.to_csv(f'src/data/{PLACEMENT}/results/measures.csv', index=False)


waiting_df = pd.DataFrame(WAITING_FOR_DELIVERY)
waiting_df.to_csv(
    f'src/data/{PLACEMENT}/results/waiting_for_supply.csv', index=False)

FETCH_TASKS.to_csv(
    f'src/data/{PLACEMENT}/results/fetch_tasks_results.csv', index=False)

EVENTS_SIM.to_csv(
    f'src/data/{PLACEMENT}/results/events_sim_results.csv', index=False)
