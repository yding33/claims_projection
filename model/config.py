import pandas
import numpy
import math
from datetime import datetime
from datetime import timedelta
from dateuntil.relativedelta import relativedelta
from itertools import product

def expand_grid(x):
    '''
    This function perform cross join with the given inputs and expands the grid of the data frame
    '''
    new_data_frame = pandas.DataFrame([row for row in product(*x.values())],
        columns=x.keys())
    return new_data_frame

def numEmergenceMonths(x):
    '''
    Each peril has a specific loss emergence pattern. This function is used to hold the mapping of
    max emgergence months that we consider
    '''
    peril_emgergence_months = {
        'Water' = 12}
    if x in peril_emgergence_months:
        return peril_emgergence_months[x]
    else return 18


