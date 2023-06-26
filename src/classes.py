import heapq
from datetime import datetime
from enum import Enum

class Employee:
    """
    Represents an employee that collects orders from the warehouse,
    with various attributes and methods.
    """
    def __init__(self, employee_id:int, tools:list, employee_status=0, employee_rest=False) -> None:
        """
        Initializes a new instance of the Employee class.

        Args:
            employee_id (int): The unique identifier for the employee.
            qualification (list): The qualification or skill set of the employee.
            tools (list): The tools that the employee can use.
            employee_status (int, optional): The status of the employee. There are two conditions: 0-available, 1-busy. Defaults to 0.
            employee_rest (bool, optional): Indicates if the employee took a rest. Defaults to False.
        
        Raises:
            TypeError: If the provided tool_id is not an integer.
        """
        if not isinstance(employee_id, int):
            raise TypeError("employee_id must be an integer.")

        self.employee_id = employee_id 
        self.tools = tools
        self.employee_status = employee_status
        self.employee_rest = employee_rest
        self.d_work_hours = {i : [] for i in range(1,8)} # A dictionary that contains daliy work hours of the worker
    
    def __repr__(self) -> str:
        """
        Returns:
            str: A string representation of the Employee object.
        """
        return (f"Employee no. (employee_id={self.employee_id}, employee_status={self.employee_status}, employee_rest={self.employee_rest})")
    
    def add_work_hours(self,day_of_week:int,total_hours:float) -> None:
        self.d_work_hours[day_of_week].append(total_hours)

class ToolType(Enum):
    """
    Represents the type of an event in a system.
    """

    REACH_FORK = 0
    PALLET_JACK = 1
    ORDER_PICKER = 2
    CROSS_DOCK = 3

class Tool:
    """
    Represents a tool used in the warehouse with various attributes and methods.
    """
    def __init__(self, tool_id: int, type: int, horizontal_speed: dict, vertical_speed: dict, remove_from_shelf_time: dict, capacity: float, left_capacity: float, is_height: bool = False, status: int = 0):
        """
        Initializes a new instance of the Tool class.

        Args:
            tool_id (int): The unique identifier for the tool.
            type (int): The type of the tool.
            speed (float): The speed of the tool.
            capacity (float): The capacity of the tool.
            left_capacity (float): The left capacity of the tool.
            is_height (bool, optional): Indicates if the tool requires a height qualification. Defaults to False.
            status (int, optional): The status of the tool. Defaults to 0 (available).

        Raises:
            TypeError: If the provided tool_id is not an integer.
        """
        if not isinstance(tool_id, int):
            raise TypeError("tool_id must be an integer.")

        self.tool_id = tool_id
        self.type = type
        self.horizontal_speed = horizontal_speed
        self.vertical_speed = vertical_speed
        self.remove_from_shelf_time = remove_from_shelf_time
        self.capacity = capacity
        self.left_capacity = left_capacity
        self.is_height = is_height
        self.status = status
    
    def __repr__(self):
        """
        Returns:
            str: A string representation of the Tool object.
        """
        return (f"Tool(tool_id={self.tool_id}, horizontal_speed={self.horizontal_speed}, vertical_speed=={self.vertical_speed}, capacity={self.capacity}, is_height={self.is_height}, status={self.status})")

class Order:
    """
    Represents an order from a client.
    """

    def __init__(self, order_id: int, arrival_date: datetime, items_dict: dict, delivery_date: datetime = None):
        """
        Initializes a new instance of the Order class.

        Args:
            order_id (int): The identifier for the order.
            arrival_date (datetime): The date and time when the order arrived from the call center.
            items_dict (dict): A dictionary representing the items in the order and their respective amounts.
            delivery_date (datetime, optional): The date and time when the order is delivered to the client.
                                                 Defaults to None.
        
        Raises:
            TypeError: If the provided order_id is not an integer, arrival_date is not a datetime object,
                       items_dict is not a dictionary, or delivery_date is not a datetime object or None.
        """
        if not isinstance(order_id, int):
            raise TypeError("order_id must be an integer.")
        if not isinstance(arrival_date, datetime):
            raise TypeError("arrival_date must be a datetime object.")
        if not isinstance(items_dict, dict):
            raise TypeError("items_dict must be a dictionary.")
        if delivery_date is not None and not isinstance(delivery_date, datetime):
            raise TypeError("delivery_date must be a datetime object or None.")

        self.order_id = order_id
        self.arrival_date = arrival_date
        self.items_dict = items_dict
        self.delivery_date = delivery_date
        self.waiting_days_for_delivery = 0

    def __repr__(self):
        """
        Returns:
            str: A string representation of the Order object.
        """
        return (f"Order(order_id={self.order_id}, arrival_date={self.arrival_date}, " \
               f"items_dict={self.items_dict})")
    

# TODO: Do we need to add an id?
class Shipment:
    """
    Represents a shipment from suppliers.
    """

    def __init__(self, arrival_date: datetime, items_dict: dict):
        """
        Initializes a new instance of the Shipment class.

        Args:
            arrival_date (datetime): The date and time when the shipment arrived.
            items_dict (dict): A dictionary representing the items in the shipment and their respective amounts.

        Raises:
            TypeError: If the provided arrival_date is not a datetime object or items_dict is not a dictionary.
        """
        if not isinstance(arrival_date, datetime):
            raise TypeError("arrival_date must be a datetime object.")
        if not isinstance(items_dict, dict):
            raise TypeError("items_dict must be a dictionary.")

        self.arrival_date = arrival_date
        self.items_dict = items_dict

    def __repr__(self):
        """
        Returns:
            str: A string representation of the Shipment object.
        """
        return (f"Shipment(arrival_date={self.arrival_date}, items_dict={self.items_dict})")

class EventType(Enum):
    """
    Represents the type of an event in a system.
    """

    DELIVERIES_FROM_SUPPLIERS = 0
    PLACING = 1
    ORDERS_FROM_CUSTOMERS = 2
    REST = 3
    FETCHING = 4
    TRANSFER_TOOLS = 5
    TWELVE_PM = 12    


class Event:
    """
    Represents an event in a system.
    """

    def __init__(self, time, type:EventType, employee:Employee=None, tool:Tool=None, shipment:Shipment=None, order:Order=None,items_to_fetch=None,P=[]):
        """
        Initializes a new instance of the Event class.

        Args:
            time: The time of the event.
            event_type: The type of the event.
            employee: An Employee-type object associated with the event. Defaults to None.
            tool: A Tool-type object associated with the event. Defaults to None.
            shipment: A Shipment-type object associated with the event. Defaults to None.
            order: An Order-type object associated with the event. Defaults to None.
            items_to_fetch: A dictionary representing the items to fetch and their amounts. Defaults to None.
            P: Heap used for events. Defaults to an empty list.

            TODO: Decide how time is represented (a hour, a day, a week)
        """
        self.time = time
        self.type = type
        self.employee = employee
        self.tool = tool
        self.shipment = shipment
        self.order = order
        self.items_to_fetch = items_to_fetch
        # Pushing to the heap
        self.P = P
        heapq.heappush(P, self)

    def __lt__(self, event2):
        """
        Compares the current event with another event based on their time.

        Args:
            event2: The event to compare with.

        Returns:
            bool: True if the current event's time is less than the other event's time, False otherwise.
        """
        return self.time < event2.time

    def __repr__(self):
        """
        Returns:
            str: A string representation of the Event object.
        """
        return (f"Event(time={self.time}, type={self.type.name}")
    