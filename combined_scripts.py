#!/usr/bin/env python

# This work is subject to Canadian Crown Copyright.
# © His Majesty the King in Right of Canada, as represented by the Minister of Environment and Climate Change, 2024.
# For more information, please consult the Canadian Intellectual Property Office.

# This work was authored by Environment and Climate Change Canada.
# Point of contact: Felix Vogel

##########

# NB: The modbus_protocol, serial_port, and utilities packages are not publicly available.
# Contact the repository maintainer for access to those scripts.

import collections
import csv
import sys
from datetime import datetime as dt
from datetime import timedelta
from time import localtime, sleep, strftime


#import Adafruit_DHT as DHT
import modbus_protocol
import numpy as np
import serial_port
import utilities
from humidity import absolute_humidity
import DHT22

# def DHT_sensor(pin):
# #'''
# #This Python script provides the function that returns the temperature and humidity from the DHT sensor
# #'''
#     humidity, temperature = DHT.read_retry(DHT.DHT22, pin)
#     return [temperature, humidity]

# *****************************************************************************


"""
# @brief    The function performs request of the frame to sensor and parses
#           its answer if received
# @param    portname string source name like '/dev/ttyUSB0' or 'COM3'
"""
def get_init_sensor_data(portname): 
    # a dictionary with sensor identification data { name: (address, type, size) }
    parameters = utilities.load_parameters_from_file('/home/pi/Desktop/Python3Scripts/hpp_identification.config')
    if not parameters:
        return

    # try to open communication line
    port = serial_port.open_port(portname)
    if port == None:
        print('Cannot open specified port')
        return


    # read out each identification parameter
    for p, (address, type, size) in list(parameters.items()):
        # create request frame body
        request_frame = modbus_protocol.create_read_ram_frame(address, size)
        
        # perform communication with sensor
        # 5 is a standard Modbus header size
        (ok, err, data) = modbus_protocol.process_modbus_request_internal(
            port, 
            bytearray(request_frame), 
            5 + size)
        if ok:
                    #result = p + '=' + str(data[0])
            result = utilities.parse_parameter(size, type, data) + ' '
        else:
            result = p + '=' + err + ' '
        sleep(0.001) # without a little pause some timeouts may occur
        return(result)
    
    # close communication after all
    serial_port.close_port(port)


# *****************************************************************************
"""
# @brief    The function performs request of the frame to sensor and parses
#           its answer if received
# @param    portname string source name like '/dev/ttyUSB0' or 'COM3'
# @param    times integer number of repeating the command. If 0, read
#           continuously with given interval
# @param    interval float number with seconds between readings (if times >= 0)
#           0 if not used
"""
def get_logg_sensor_data(portname, config):
    parameters = utilities.load_parameters_from_file(config)
    if not parameters:
        return

    # try to open communication line
    port = serial_port.open_port(portname)
    if port == None:
            print('Cannot open specified port')
            return

    result = []

    # read out each parameter
    for p, (address, type, size) in list(parameters.items()):
        # create request frame body
        request_frame = modbus_protocol.create_read_ram_frame(address, size)
        
        # perform communication with sensor
        # 5 is a standard Modbus header size
        (ok, err, data) = modbus_protocol.process_modbus_request_internal(
                port, 
                bytearray(request_frame), 
                5 + size)
        if ok:
            #result += p + '=' + utilities.parse_parameter(size, type, data) + ' '
            result.append(utilities.parse_parameter(size, type, data))
        else:
            result += p + '=' + err + ' '
        sleep(0.001)
    # result into std output
    return result
            
# *****************************************************************************
def get_parameter_names(filename):

    with open(filename, 'r') as f:
        lines = f.readlines()
    
    names = []
    for i in lines:
        names.append(i.split(' =')[0])
        
    return names



'''
This Python script reads the data from the HPP and logs it onto a CSV file.
It creates two CSV files, one that is 10 second averages and the other is 1 minute averaged
'''


'''
This function returns an array that is organized so that it can be written to the 10 second average CSV file
'''
def organize(sample_number, timestamp, numbers):
    array = [sample_number, timestamp] 
    
    for i in range(len(numbers)):
        data = np.array(numbers[i])
        array.extend([len(numbers[i]), np.mean(data), np.std(data, ddof=0)]) # Sample count, Mean and standard deviation of data in array
        
    return array

'''
This function returns an array that is processed so that it can be written to the 1 minute average CSV file
'''
def process(array, length, DHT):
    new_array = [(dt.now() - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')] # Subtract 1 minute from current time for time delay
    data = np.array([0 for i in range(length * 2)], dtype=float)
    
    for sample in array:
        for j in range(length * 2):
            data[j] += sample[j]
    
    data[:2] /= 10 # Divide CO2 concentration and standard deviation by 10
    data[2:6] /= 100 # Divide pressure, temperature and their standard deviation by 100
    data /= len(array) # Get average
    
    new_array.extend(data)
    
    if DHT:
        new_array.append(absolute_humidity(data[-4], data[-2])) # Returns absolute humidity from DHT's temperature and relative humidity

    return new_array
    
def start_logging(interval):
    port = '/dev/ttyUSB0' # Port for Raspberry Pi 3
    config = '/home/pi/Desktop/Python3Scripts/my_parameters.config' # Path where to read which parameters to log

    csv_header = [['', ''], ['Sample #', 'Timestamp']] # Headers for CSV file
    csv_header_processed = ['Timestamp']
    
    parameters = get_parameter_names(config)
    parameter_length = len(parameters) + 2
    
    for name in parameters:
        csv_header[0].extend(['', name, ''])
        csv_header[1].extend(['Sample count', 'Average', 'StdDev'])
        
        csv_header_processed.extend([name, 'StdDev'])
        
    csv_header[1].extend(['Sample count', 'Average', 'StdDev', 'Sample count', 'Average', 'StdDev'])
    csv_header[0].extend(['', 'DHT_Temperature', '', '', 'DHT_Relative_Humidity', ''])
    csv_header_processed.extend(['DHT_Temperature', 'StdDev', 'DHT_Relative_Humidity', 'StdDev', 'Calculated Absolute Humidity'])
    
    hpp_name = get_init_sensor_data(port)[-5:] # Meter ID of HPP
    current_time = dt.now().strftime('_%Y%m%d_%H%M%S')
    
    path  = '/home/pi/Desktop/Logging_reports/HPP_' + hpp_name + current_time + '.csv' # create path for CSV files
    path_processed = '/home/pi/Desktop/Processed/HPP_' + hpp_name + current_time + '_processed_PERC5.csv'     

    file = open(path, 'w') # Write the headers to the CSV files
    writer = csv.writer(file)
    writer.writerows(csv_header)

    file_processed = open(path_processed, 'w')
    writer_processed = csv.writer(file_processed)
    writer_processed.writerows([csv_header_processed])
    
    sample_number = 1
    
    start_minute = dt.now().strftime('%M')
    processed_array = []
    
    DHT_works = True #Boolean to control if DHT works or not
    
    while True: # Never ending loop
        numbers = [[] for i in range(parameter_length)]
        start_second = dt.now().strftime('%S')[0]
        
        while True:
            raw_data = get_logg_sensor_data(port, config) # Recieve data from HPP sensor
            if DHT_works:
                raw_DHT = DHT22.DHT_sensor(9) # Recieve temperature and relative humidity from DHT
                if str(raw_DHT[0]) == 'None': #If DHT returns None then it has stopped working
                    DHT_works = False
                    parameter_length -= 2
                    numbers = [[] for i in range(parameter_length)]
                else:
                    raw_data += raw_DHT
       
            for i in range(parameter_length):
                numbers[i].append(raw_data[i])                
                
            for i in range(int(interval/0.1)): # Break down interval into multiple pieces to read if the time has changed in between
                if start_second == dt.now().strftime('%S')[0]:
                    sleep(0.1)
                else:
                    break
                
            else:
                continue
            break # If the time by 10 seconds has changed during sleep then code goes out of inner while loop
                
        data_array = organize(sample_number, dt.now().strftime('%Y-%m-%d %H:%M:%S'), numbers)
          
        file = open(path, 'a') # Write to 10 second average CSV file
        writer = csv.writer(file)
        writer.writerows([data_array])
        sample_number += 1
        
        if start_minute != dt.now().strftime('%M'): # If the minute has changed write to 1 minute CSV file
            if len(processed_array) != 0:
                file_processed = open(path_processed, 'a')
                writer_processed = csv.writer(file_processed)
                writer_processed.writerows([process(processed_array, parameter_length, DHT_works)])

                processed_array = []
            
            start_minute = dt.now().strftime('%M')
        
        val = True # If the standard deviation is greater than 10, don't log the data 
        for std in data_array[4::3]:
            if std > 10:
                val = False
                break
            
        if val:
            processed_array.append([num for index, num in enumerate(data_array[2:]) if index % 3 != 0]) # Only process data and standard deviation not sample count
            
start_logging(2)
