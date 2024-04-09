#!/usr/bin/env python

# This work is subject to Canadian Crown Copyright.
# © His Majesty the King in Right of Canada, as represented by the Minister of Environment and Climate Change, 2024.
# For more information, please consult the Canadian Intellectual Property Office.

# This work was authored by Environment and Climate Change Canada.
# Point of contact: Felix Vogel

##########

# NB: The modbus_protocol, serial_port, and utilities packages are not publicly available.
# Contact the repository maintainer for access to those scripts.

import csv
import os
import subprocess
from datetime import datetime as dt
from datetime import timedelta
from time import sleep
import gps
import glob
import sys


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
        print("Error with hpp id config file.")
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
        
        print("HPP device initialized.")
        
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
            try:
                result.append(utilities.parse_parameter(size, type, data))
            except Exception as e:
                print("Exception: %s" % e)
                print("Error occurred parsing: %s" % str(parameters[p]))
                result.append(-9999)
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
        
    print("Loaded parameter names.")
        
    return names



# '''
# This Python script reads the data from the HPP and logs it onto a CSV file.
# It creates two CSV files, one that is 10 second averages and the other is 1 minute averaged
# '''


# '''
# This function returns an array that is organized so that it can be written to the 10 second average CSV file
# '''
# def organize(sample_number, timestamp, numbers):
#     array = [sample_number, timestamp] 
    
#     for i in range(len(numbers)):
#         data = np.array(numbers[i])
#         array.extend([len(numbers[i]), np.mean(data), np.std(data, ddof=0)]) # Sample count, Mean and standard deviation of data in array
        
#     return array

# '''
# This function returns an array that is processed so that it can be written to the 1 minute average CSV file
# '''
# def process(array, length, DHT):
#     new_array = [(dt.now() - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')] # Subtract 1 minute from current time for time delay
#     data = np.array([0 for i in range(length * 2)], dtype=float)
    
#     for sample in array:
#         for j in range(length * 2):
#             data[j] += sample[j]
    
#     data[:2] /= 10 # Divide CO2 concentration and standard deviation by 10
#     data[2:6] /= 100 # Divide pressure, temperature and their standard deviation by 100
#     data /= len(array) # Get average
    
#     new_array.extend(data)
    
#     if DHT:
#         new_array.append(absolute_humidity(data[-4], data[-2])) # Returns absolute humidity from DHT's temperature and relative humidity

#     return new_array

def poll_gps(gps_session, timeout):
    
    ## This function reads the gps data via the gpsd client
    ## The code is drawn from:
    ## https://gpsd.io/gpsd-client-example-code.html#_example_2python
    
    gps_dict = {
            "fix" : "Invalid",
            "lat" : -9999,
            "lon" : -9999,
            "alt" : -9999
    }
    
    poll_start = dt.now()
    gps_read_try_n = 0
    
    while True:
                
        ## check for time out
        if (dt.now() - poll_start) > timedelta(seconds = timeout):
            print("GPS signal acquisition timed out (%d s)." % timeout)
            gps_session.close()
            gps_session = gps.gps(mode=gps.WATCH_ENABLE)
            print("GPS session was restarted.")
            break
        
        try:
            ## check the read is OK. The read returns negative if not OK.
            if gps_session.read() != 0:
                gps_read_try_n += 1
                if gps_read_try_n >= 2:
                    print("GPS session read failed multiple times.")
                    gps_session.close()
                    gps_session = gps.gps(mode=gps.WATCH_ENABLE)
                    print("GPS session was restarted.")
                    gps_read_try_n = 0
                sleep(1)
                continue
            
            ## check that the packet contains time, position, velocity (TPV) data
            if not (gps.MODE_SET & gps_session.valid):
                gps_read_try_n += 1
                if gps_read_try_n >= 2:
                    print("GPS session read packet was invalid multiple times.")
                    gps_session.close()
                    gps_session = gps.gps(mode=gps.WATCH_ENABLE)
                    print("GPS session was restarted.")
                    gps_read_try_n = 0
                sleep(1)
                continue
                
            ## check the GPS fix. 2 is 2D fix and 3 is 3D fix
            if gps_session.fix.mode >= 2 and gps.isfinite(gps_session.fix.latitude):
                
                gps_dict["fix"] = ("Invalid", "NO_FIX", "2D", "3D")[gps_session.fix.mode]
                gps_dict["lat"] = gps_session.fix.latitude
                gps_dict["lon"] = gps_session.fix.longitude
                gps_dict["alt"] = gps_session.fix.altitude
                
                print("GPS data acquired:\n%s" % gps_dict)
                
                break
            
        except ConnectionResetError as e:
            print("GPS error: "+str(e))
            gps_session.close()
            gps_session = gps.gps(mode=gps.WATCH_ENABLE)
            print("GPS session was restarted.")
            break
        
    return gps_dict, gps_session
    
def check_time_sync(printing=True):
    
    ## check if system time is sync'd with ntp
    chrony_last_sync = subprocess.run(["chronyc sources | grep NMEA"], 
                                shell=True, capture_output=True, 
                                text=True).stdout.split()[5]
    
    chrony_offset = subprocess.run(["chronyc sources | grep NMEA"], 
                                shell=True, capture_output=True, 
                                text=True).stdout.split()[6].split('[')[0]

    if printing:
        try:
            if int(chrony_last_sync) <= 13:
                print("Time is synchronized")
                
            else:
                print("Time is not synchronized...")
                
        except:
            print("Time is not synchronized...")
    
    return chrony_last_sync, chrony_offset
            
def setup_paths_filenames(hpp_name):
    ## check whether the current date's directory and files exist
    ## if they don't, set them up
    
    daily_file_counter = 0
    
    time_file = dt.now()
    # TODO check if local or UTC on pi
    
    ## set up new year and month directories
    if os.path.exists(os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"))) == False:
        
        subprocess.run(["mkdir " + os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"))], shell=True)
        print("Created new year directory.")
        subprocess.run(["mkdir " + os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m"))], shell=True)
        print("Created new month directory.")

    ## set up new month directory
    elif os.path.exists(os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m"))) == False:
            
        subprocess.run(["mkdir " + os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m"))], shell=True)
        print("Created new month directory.")

    ## set path and filename variables
    path = os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m"))
    filename = hpp_name + time_file.strftime("-%Y-%m-%d") + "--" + str(daily_file_counter) +".csv"
    
    ## tick up the end of the filename if a file for the day already exists
    if os.path.exists(os.path.join(path, filename)) == True:
        daily_files_list = glob.glob(os.path.join(path, filename.split('--')[0]+'*.csv'))
        daily_file_counter = len(daily_files_list)
        filename = hpp_name + time_file.strftime("-%Y-%m-%d") + "--" + str(daily_file_counter) +".csv"
    
    ## run the SD card check on mondays
    if time_file.weekday() == 0:
        print("Happy Monday!")
        #check_sd_health()
    
    return path, filename, time_file
    
def setup_csv_headers(path, filename, csv_header, csv_col_names):
    
    ## check for an existing file, and write header to file if not
    if os.path.exists(os.path.join(path, filename)) == False:
        with open(os.path.join(path, filename),'w') as f:
            wr = csv.writer(f, dialect='excel')
            wr.writerow(csv_header)
            wr.writerow(["#"])
            wr.writerow(csv_col_names)
        print("New file written.\n\tPath = %s\n\tFilename = %s" % (path, filename))

def load_location_config(file_path):
    
    location_config =[]
    
    with open(file_path, 'r') as f:
        raw = f.readlines()
    
    for i in range(len(raw)):
        print(raw[i].strip('\n'))
        location_config.extend(raw[i].strip('\n').replace(' ', '').split('='))
    
    return location_config

def make_parameter_formats(csv_col_names):
    
    ## set up formats for the csv for particular measured parameters
    ## set up as a dictionary where the keys are the index in the row
    ## and the values are a list containing information in index 0 and
    ## the fstrings format code in index 1
    
    parameter_formats = {}
    
    for i in range(len(csv_col_names)):
        
        if "Error" in csv_col_names[i]:
            parameter_formats[i] = ['hex',"08b"]
            
    return parameter_formats

def start_logging(interval):
    ## This is the main sensor function
    ## Initializes the sensor and then takes measurements every interval seconds
    
    ## set paths for important script parts
    port = subprocess.run(["ls /dev/serial/by-id/usb-FTDI*"], # TODO check operation on pi
                   shell=True, capture_output=True, text=True
                   ).stdout.strip("\n")
    
    # port = '/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_A600PJOV-if00-port0' # Port for Raspberry Pi 3
    config = '/home/pi/Desktop/Python3Scripts/my_parameters.config' # Path where to read which parameters to log

    hpp_serial = get_init_sensor_data(port).strip(' ')
    hpp_name = "HPP"+hpp_serial[-4:] # Last 4 digits of meter ID of HPP

    ## set up GPS and record location
    gps_session = gps.gps(mode=gps.WATCH_ENABLE)
    gps_dict, gps_session = poll_gps(gps_session, 10)   # 10 s time out

    ## set up header and read information from location config file
    csv_header = ["#","HPP_serial", hpp_serial]
    location_config = load_location_config("/home/pi/Desktop/Python3Scripts/location.config")
    
    ## prefer GPS lat lon over config file lat lon
    if gps_dict["fix"] == '2D' or gps_dict["fix"] == '3D':
        location_config[5] = gps_dict["lat"]
        location_config[7] = gps_dict["lon"]
        location_config[9] = gps_dict["alt"]
    
    csv_header.extend(location_config)

    ## get parameter names and set up csv column names header
    csv_col_names = ['Timestamp']
    parameters = get_parameter_names(config)
    csv_col_names.extend(parameters)
    csv_col_names.extend(["DHT_Temperature", "DHT_Relative_Humidity", "CPU_Temperature"])
    parameter_formats = make_parameter_formats(csv_col_names)
    
    ## run important initialization steps
    ## see individual functions for more info
    chrony_last_sync, chrony_offset = check_time_sync()
    path, filename, time_file = setup_paths_filenames(hpp_name)
    setup_csv_headers(path, filename, csv_header, csv_col_names)
    
    ## initialize variables for the measurement loop
    DHT_works = True #Boolean to control if DHT works or not
    time_prev = False
    counter = 0
    
    print("Beginning measurement loop...")
    
    while True: # Never ending loop for measurement
    
        row = []
        time0 = dt.now()
        
        if counter == 100:
            
            gps_dict, gps_session = poll_gps(gps_session, 10)
            chrony_last_sync, chrony_offset = check_time_sync(printing=False)
            
            with open("/home/pi/Desktop/Logging_reports/GPS_log.txt", 'a') as f:
                f.write(time0.isoformat(timespec='milliseconds'))
                for v in gps_dict.values():
                    f.write(' '+str(v))
                f.write('\n')
                
            with open("/home/pi/Desktop/Logging_reports/chrony_log.txt", 'a') as f:
                f.write(time0.isoformat(timespec='milliseconds'))
                f.write(' '+str(chrony_last_sync))
                f.write(' '+str(chrony_offset))
                f.write('\n')
            
            counter = 0
        
        row.extend(get_logg_sensor_data(port, config)) # Recieve data from HPP sensor
        
        if DHT_works:
            
            raw_DHT = DHT22.DHT_sensor(9) # Recieve temperature and relative humidity from DHT
            if str(raw_DHT[0]) == 'None': #If DHT returns None then it has stopped working
                DHT_works = False
                row.extend([-9999,-9999])
            else:
                row.append(int(round(raw_DHT[0],1)*10))
                row.append(int(round(raw_DHT[1],1)*10))
                
        else:
            row.extend([-9999,-9999])
   
        ## read in the CPU temperature
        row.append(round(int(subprocess.run(["cat /sys/class/thermal/thermal_zone0/temp"], # TODO check operation on pi
                       shell=True, capture_output=True, text=True
                       ).stdout.strip("\n")),-2) // 100)
            
        time1 = dt.now()
        
        time_row = time0 + (time1 - time0)/2
        
        row.insert(0, time_row.isoformat(timespec='milliseconds'))
        
        ## adjust formats for certain parameters in row
        for k,v in parameter_formats.items():
            
            if v[0] == "hex":
                try:
                    row[k] = f"{int(row[k],0):{v[1]}}"
                except Exception as e:
                    print("Parameter exception: %s" % e)
                    print(row[k])
                    row[k]=-9999
            else:
                row[k] = f"{row[k]:{v[1]}}"
        
        ## check if the day changed during the measurement
        if time_row.day != time_file.day:
            chrony_last_sync, chrony_offset = check_time_sync()
            path, filename, time_file = setup_paths_filenames(hpp_name)
                
        ## check for an existing file, and write header to file if not
        setup_csv_headers(path, filename, csv_header, csv_col_names)
        
        ## append measured values to csv
        with open(os.path.join(path, filename),'a') as f:
            wr = csv.writer(f, dialect='excel')
            wr.writerow(row)

        ## control the timing of the measurement loop based on the interval
        if time_prev:
            time_now = dt.now()
            if (time_now - time_prev).total_seconds() < interval:
                try:
                    sleep(interval - (time_now - time_prev).total_seconds())
                except:
                    pass
        else:
            sleep(interval)
        time_prev = time_row
        counter += 1
        
if __name__ == "__main__":
    
    try:
        initial_sleep = int(sys.argv[1])
    except IndexError:
        initial_sleep = 60
        
    sleep(initial_sleep)   # initial sleep to allow for user interrupt and gps cold start
    
    start_logging(2.5)