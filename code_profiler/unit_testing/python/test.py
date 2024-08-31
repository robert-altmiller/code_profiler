from ace_tools import *


def sum_of_list(lst):
    """ Calculate the sum of elements in a list. """
    # Convert the list to a Spark DataFrame column
    total = 0
    for itm in lst:
        total += itm
    return total
