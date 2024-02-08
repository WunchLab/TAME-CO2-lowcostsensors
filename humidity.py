# This work is subject to Canadian Crown Copyright.
# © His Majesty the King in Right of Canada, as represented by the Minister of Environment and Climate Change, 2024.
# For more information, please consult the Canadian Intellectual Property Office.

# This work was authored by Environment and Climate Change Canada.
# Point of contact: Felix Vogel

##########

'''
This Python script provides the function to calculate the absolute humidity
'''


import math

def absolute_humidity(temperature, relative_humidity):
    T =  temperature + 273.15
    theta = 1 - T / 647.096

    C1 = -7.85951783
    C2 = 1.84408259
    C3 = -11.7866497
    C4 = 22.6807411
    C5 = -15.9618719
    C6 = 1.80122502

    pw = relative_humidity * 220640 * math.e ** ((647.096 / T) * (C1 * theta + C2 * theta**1.5 + C3 * theta**3 + C4 * theta**3.5 + C5 * theta**4 + C6 * theta**7.5))

    abs_humidity = 2.16679 * pw / T

    return abs_humidity





