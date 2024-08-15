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
from RPi import GPIO
from threading import Lock, Thread
from queue import Queue
import paramiko
import struct
import pandas as pd
import lzma
import io
import xarray as xr

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
        log("ISSUE: Error loading hpp id config file.")
        return

    # try to open communication line
    port = serial_port.open_port(portname)
    if port == None:
        print('ISSUE: Cannot open specified port: %s' % str(portname))
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
            log('ISSUE: Cannot open specified port: %s' % str(portname))
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
                # print("Exception: %s" % e)
                # print("Error occurred parsing: %s" % str(parameters[p]))
                
                log("ISSUE: Exception occured while parsing parameter %s, %s from HPP board: %s" % (str(p), str(parameters[p]), e))
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
        
    log("Loaded parameter names.")
        
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

def poll_gps(gps_session, timeout, location_config):
    
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
            log("GPS signal acquisition timed out (%d s)." % timeout)
            gps_session.close()
            gps_session = gps.gps(mode=gps.WATCH_ENABLE)
            log("GPS session was restarted.")
            break
        
        try:
            ## check the read is OK. The read returns negative if not OK.
            if gps_session.read() != 0:
                gps_read_try_n += 1
                if gps_read_try_n >= 2:
                    log("GPS session read failed multiple times.")
                    gps_session.close()
                    gps_session = gps.gps(mode=gps.WATCH_ENABLE)
                    log("GPS session was restarted.")
                    gps_read_try_n = 0
                sleep(1)
                continue
            
            ## check that the packet contains time, position, velocity (TPV) data
            if not (gps.MODE_SET & gps_session.valid):
                gps_read_try_n += 1
                if gps_read_try_n >= 2:
                    log("GPS session read packet was invalid multiple times.")
                    gps_session.close()
                    gps_session = gps.gps(mode=gps.WATCH_ENABLE)
                    log("GPS session was restarted.")
                    gps_read_try_n = 0
                sleep(1)
                continue
                
            ## check the GPS fix. 2 is 2D fix and 3 is 3D fix
            if gps_session.fix.mode >= 2 and gps.isfinite(gps_session.fix.latitude):
                
                gps_dict["fix"] = ("Invalid", "NO_FIX", "2D", "3D")[gps_session.fix.mode]
                gps_dict["lat"] = gps_session.fix.latitude
                gps_dict["lon"] = gps_session.fix.longitude
                gps_dict["alt"] = gps_session.fix.altitude
                
                log("GPS data acquired: %s" % gps_dict)
                
                ## set the location config so the gps coordinates will be in the file header
                if gps_dict["fix"] == '2D' or gps_dict["fix"] == '3D':
                    location_config[5] = gps_dict["lat"]
                    location_config[7] = gps_dict["lon"]
                    location_config[9] = gps_dict["alt"]
                
                break
            
        except ConnectionResetError as e:
            log("ISSUE: GPS ConnectionResetError: %s" % str(e))
            gps_session.close()
            gps_session = gps.gps(mode=gps.WATCH_ENABLE)
            log("GPS session was restarted.")
            break
        
    return gps_dict, gps_session, location_config
    
def check_time_sync(printing=False, logging=True):
    
    #TODO add grep *, call grep nmea only once, logging, test this feature
    ## check if system time is sync'd with chrony high quality (*) data
    chrony_star_output = subprocess.run(["chronyc sources | grep \*"], 
                                shell=True, capture_output=True, 
                                text=True).stdout.split()
    
    ## check if system time is sync'd with GPS NMEA data
    chrony_nmea_output = subprocess.run(["chronyc sources | grep NMEA"], 
                                shell=True, capture_output=True, 
                                text=True).stdout.split()
    
    # chrony_last_sync = subprocess.run(["chronyc sources | grep NMEA"], 
    #                             shell=True, capture_output=True, 
    #                             text=True).stdout.split()[5]
    
    # chrony_offset = subprocess.run(["chronyc sources | grep NMEA"], 
    #                             shell=True, capture_output=True, 
    #                             text=True).stdout.split()[6].split('[')[0]

    if printing:
        try:
            if int(chrony_star_output[5]) <= 70 or int(chrony_nmea_output[5]) <= 13:
                print("Time is synchronized")
                
            else:
                print("Time is not synchronized...")
                
        except:
            print("Time is not synchronized...")
            
    if logging:
        ## log the * source, if there is one. If not, log that it was missing
        try:
            log("Chrony time sync check, last * source sync: %s, offset: %s" % (str(chrony_star_output[5]), str(chrony_star_output[6].split('[')[0])))
        except IndexError:
            log("Chrony time sync check, no * source found.")
            
        log("Chrony time sync check, last NMEA sync: %s, offset: %s" % (str(chrony_nmea_output[5]), str(chrony_nmea_output[6].split('[')[0])))
    
    return chrony_nmea_output, chrony_star_output
            
def setup_paths_filenames(hpp_name):
    ## check whether the current date's directory and files exist
    ## if they don't, set them up
    
    daily_file_counter = 0
    
    time_file = dt.now()
    # TODO check if local or UTC on pi
    
    ## set up new year and month directories
    if os.path.exists(os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"))) == False:
        
        subprocess.run(["mkdir " + os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"))], shell=True)
        log("Created new year directory: %s" % os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y")))
        subprocess.run(["mkdir " + os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m"))], shell=True)
        log("Created new month directory: %s" % os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m")))

    ## set up new month directory
    elif os.path.exists(os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m"))) == False:
            
        subprocess.run(["mkdir " + os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m"))], shell=True)
        log("Created new month directory: %s" % os.path.join("/home/pi/Desktop/Logging_reports", time_file.strftime("%Y"), time_file.strftime("%m")))

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
        log("Happy Monday!")
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
        log("New file written. Path = %s\tFilename = %s" % (path, filename))

def load_location_config():
    
    location_config = [-9999, -9999]
    
    with open("/home/pi/Desktop/Python3Scripts/location.config", 'r') as f:
        location_config = f.read().split()
    
    log("Location loaded from config file was %s, which was last updated at %s" % (location_config[0], location_config[1]))
    
    location_config.insert(0, "Location_name")
    location_config.insert(2, "Date_location_updated")
    
    lookup = pd.read_csv("/home/pi/Desktop/Python3Scripts/location_lookup.csv", index_col="SITE")
    
    location_config.extend(["Latitude", lookup.loc[location_config[1],"LAT"]])
    location_config.extend(["Longitude", lookup.loc[location_config[1],"LON"]])
    location_config.extend(["Altitude", lookup.loc[location_config[1],"ALT"]])
    
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

def pack_lowres_data(data, location_config, method="netcdf"):
    
    
    #data["Time"] = (data["Time"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")
        
    data = data.astype({"unixtime":np.int32,
                        "CO2":np.int16, "P":np.int16,"T":np.int16,
                        "RH":np.int16,"IR":np.int32, "12V":np.int8})
    
    if method == "struct":
    
        n = data.shape[0]
        
        print("pack...")
        
        try:
            packet = struct.pack(f"BH5sff{n}i{n}h{n}h{n}h{n}h{n}i{n}b", int(n),
                        int(eval("0x"+hpp_name[-4:])), bytes(location_config[1].ljust(5), 'utf-8'),
                        location_config[5], location_config[7], *data["unixtime"].values,
                        *data["CO2"].values, *data["P"].values, *data["T"].values,
                        *data["RH"].values, *data["IR"].values, *data["12V"].values)
            
        except Exception as e:
            print(e)
            print(np.max(data,axis=0))
            print(np.min(data,axis=0))
            
            n = 0
            packet = struct.pack("BH5sff", int(n),
                        int(eval("0x"+hpp_name[-4:])), bytes(location_config[1].ljust(5), 'utf-8'),
                        location_config[5], location_config[7])
        
    elif method == "netcdf":
        
        data = data.set_index('unixtime')
        # data.index = data.index.astype(np.int32)
        data.index.name = "time"
        data = data.drop(["timebin", "Time"], axis=1)
        
        packet = xr.Dataset(data, attrs={"ID":hpp_name[-4:],"loc":location_config[1],"lat":location_config[5] ,"lon":location_config[7]})
        packet["time"] = packet['time'].astype(np.int32)
        
        print(packet)
        
        packet = packet.to_netcdf()
        
    else:
        packet = None
    
    return packet

def communicate(q_data_tuples, ssh, sftp, remote_path):
    
    daily_rsync_flag = False  ## Flag to check if once daily rsync has been done, True is rsync needed, False is rsync has already been done
    daily_remotedir_flag = True  ## Flag to check daily if remote directory exists and make dir if needed, True is check is needed, False is check has already been done
    boot_remotedir_flag = True  ## Flag to check on boot if remote directory exists and make dir if needed, True is check is needed, False is check has already been done

    time_down = 0

    ## infinite loop
    while True:
        
        #print("Communicate loop start...")
        
        ## check if there is anything in the queue
        if q_data_tuples.empty() == False:
            
            ## check internet connection status
            status_wifi, status_gsm, status_eth = check_networkmanager(logging=False)
            
            ## if you have wired internet but not wifi, check to make sure the
            ## wired connection isn't just a local connection
            if status_eth == True and status_wifi == False:
                status_localhost, status_internet, status_server = check_pings(ping_only=True)
                
                if status_internet == False:
                    status_eth = False
            
            ## take tuple from the queue and get it ready to send, if you have internet
            if status_wifi or status_gsm or status_eth:
                
                path, filename, time_file, data, location_config = q_data_tuples.get()
                
                ## set daily flags True if its not midnight so that flag tasks can be done next midnight
                if time_file.hour > 0:
                    daily_rsync_flag = True
                    daily_remotedir_flag = True
                
                # print(data.iloc[0])
                
                remote_lowres_dir = "low_res_raw/%s/%s/" % (hpp_name, time_file.strftime("%Y/%m/%d"))
                remote_lowres_filename = "%s-%s.nc.xz" % (hpp_name, data.loc[0,"Time"].strftime("%Y-%m-%dT%H%M"))
                
                remote_hires_dir = "high_res_raw/%s/%s/" % (hpp_name, time_file.strftime("%Y/%m"))


                if boot_remotedir_flag:
                    setup_remote_dirs(ssh, sftp, remote_lowres_dir, remote_hires_dir, remote_path)
                    boot_remotedir_flag = False

                elif time_file.hour == 0 and daily_remotedir_flag == True:
                    setup_remote_dirs(ssh, sftp, remote_lowres_dir, remote_hires_dir, remote_path)
                    daily_remotedir_flag = False
                
                packed_data = pack_lowres_data(data, location_config)
                packed_data_compressed = lzma.compress(packed_data)
                
            ## send data when wifi or ethernet internet are available
            if status_wifi or status_eth:
                
                time_down = 0
                
                try:
                    ## sftp low res data from queue
                    with io.BytesIO(packed_data_compressed) as file_object:
                        sftp.putfo(file_object, os.path.join(remote_path.split(":")[1], remote_lowres_dir, remote_lowres_filename))
                    
                    # rsync today's file
                    output = subprocess.run(["rsync --timeout=5 -e 'ssh -p2222' %s %s" 
                                              % (os.path.join(path, filename),
                                                os.path.join(remote_path, remote_hires_dir, filename))],
                                            shell=True, capture_output=True,
                                            text=True)
                    if output.returncode > 0:
                        raise TimeoutError
                    
                    if time_file.hour == 0 and daily_rsync_flag == True:
                        #rsync month directory, local path needs a trailing / so the dir contents are copied instead of the dir itself
                        output = subprocess.run(["rsync -r --timeout=5 -e 'ssh -p2222' %s %s" 
                                                 % (path+'/', 
                                                    os.path.join(remote_path, remote_hires_dir))],
                                                shell=True, capture_output=True,
                                                text=True)
                        if output.returncode > 0:
                            raise TimeoutError
                        
                        daily_rsync_flag = False
                    
                except TimeoutError:
                    log("ISSUE: remote copy (rsync or sftp.put) timed out")
                    
                    ## put data etc back into the queue since it may not have been sent correctly
                    q_data_tuples.put((path, filename, time_file, data, location_config))
                    
                    status_wifi, status_gsm, status_eth = check_networkmanager()
                    
                    if status_wifi == True or status_eth == True:
                        status_localhost, status_internet, status_server = check_pings()
                        cleanup_ssh(ssh, sftp)
                        ssh, sftp = initialize_ssh()
                        
                    else:
                        cleanup_ssh(ssh, sftp)
                        status_localhost, status_internet, status_server = check_pings()
                        ssh, sftp = initialize_ssh()
                        
                except OSError as e:
                    log("ISSUE: remote copy (rsync or sftp.put) raised OSError: %s" % str(e))
                    log("... low-res sftp tried to write to %s" % os.path.join(remote_path.split(":")[1], remote_lowres_dir, remote_lowres_filename))
                    
                    if 'No such file' in str(e):
                        setup_remote_dirs(ssh, sftp, remote_lowres_dir, remote_hires_dir, remote_path)
                    
                    ## put data etc back into the queue since it may not have been sent correctly
                    q_data_tuples.put((path, filename, time_file, data, location_config))
                    
                    cleanup_ssh(ssh, sftp)
                    status_localhost, status_internet, status_server = check_pings()
                    ssh, sftp = initialize_ssh()
                    print("OSError done...")
                    
                except Exception as e:
                    log("ISSUE: remote copy (rsync or sftp.put) raised exception: %s" % str(e))
                    
                    ## put data etc back into the queue since it may not have been sent correctly
                    q_data_tuples.put((path, filename, time_file, data, location_config))
                    
                    cleanup_ssh(ssh, sftp)
                    status_localhost, status_internet, status_server = check_pings()
                    ssh, sftp = initialize_ssh()
                    
                finally:
                    q_data_tuples.task_done()
                                    
            ## send low res data when only cellular is available
            elif status_gsm:
                
                time_down = 0
                
                try:
                    #sftp low res data from queue
                    with io.BytesIO(packed_data_compressed) as file_object:
                        sftp.putfo(file_object, os.path.join(remote_path.split(":")[1], remote_lowres_dir, remote_lowres_filename))
                    
                except TimeoutError:
                    log("ISSUE: remote copy over gsm (sftp.put) timed out")
                    
                    ## put data etc back into the queue since it may not have been sent correctly
                    q_data_tuples.put((path, filename, time_file, data, location_config))
                    
                    status_wifi, status_gsm, status_eth = check_networkmanager()
                    
                    if status_gsm == True:
                        status_localhost, status_internet, status_server = check_pings()
                        cleanup_ssh(ssh, sftp)
                        ssh, sftp = initialize_ssh()
                        
                    else:
                        cleanup_ssh(ssh, sftp)
                        networkmanager_restart()
                        ssh, sftp = initialize_ssh()
                        
                except OSError as e:
                    log("ISSUE: remote copy over gsm (sftp.put) raised OSError: %s" % str(e))
                    
                    if 'No such file' in str(e):
                        setup_remote_dirs(ssh, sftp, remote_lowres_dir, remote_hires_dir, remote_path)
                    
                    ## put data etc back into the queue since it may not have been sent correctly
                    q_data_tuples.put((path, filename, time_file, data, location_config))
                    
                    cleanup_ssh(ssh, sftp)
                    status_localhost, status_internet, status_server = check_pings()
                    ssh, sftp = initialize_ssh()
                    
                except Exception as e:
                    log("ISSUE: remote copy over gsm (sftp.put) raised exception: %s" % str(e))
                    
                    ## put data etc back into the queue since it may not have been sent correctly
                    q_data_tuples.put((path, filename, time_file, data, location_config))
                    
                    cleanup_ssh(ssh, sftp)
                    status_localhost, status_internet, status_server = check_pings()
                    ssh, sftp = initialize_ssh()
                    
                finally:
                    q_data_tuples.task_done()
            
            else:
                                
                if time_down == 0:
                    log("ISSUE: No internet connection (wifi/eth/gsm) was found when trying to send data.")
                    time_down = dt.utcnow()
                                
                cleanup_ssh(ssh, sftp)
                status_localhost, status_internet, status_server = check_pings()
                ssh, sftp = initialize_ssh()
                
                if (dt.utcnow() - time_down).days >= 3:
                    log("ISSUE: No internet connection for 3 days. Rebooting system. Bye!")
                    cleanup_ssh(ssh, sftp)
                    output = subprocess.run(["sudo reboot"],
                                            shell=True, capture_output=True,
                                            text=True)
                sleep(300)
                # sleep(30) # TODO remove this test line and uncomment above line
                    
        if q_data_tuples.empty() == True:
            sleep(30)


def check_pings(ping_only=False):
    ## ping localhost, internet site (google), and the server to help diagnose any network issues
    ## setting ping_only to True will simply do the pings and return the values without logging or restarts
    
    status_localhost = None
    status_internet = None
    status_server = None
    
    ## ping localhost
    output = subprocess.run(["ping -c 3 -q 127.0.0.1"],
                                shell=True, capture_output=True,
                                text=True)
    status_localhost = output.returncode == 0
    
    if status_localhost == False and ping_only == False:
        log("ISSUE: Troubleshoot connection: localhost was unpingable. Restarting networkmanager.")
        networkmanager_restart()
    
    ## ping google
    output = subprocess.run(["ping -c 3 -q google.ca"],
                                shell=True, capture_output=True,
                                text=True)
    status_internet = output.returncode == 0
    
    if status_internet == False and ping_only == False:
        status_wifi, status_gsm, status_eth = check_networkmanager()
        log("ISSUE: Troubleshoot connection: google.ca was unpingable. Restarting networkmanager.")
        networkmanager_restart()
    
    ## ping the server if there is internet connection
    else:
        output = subprocess.run(["ping -c 3 -q carbon.atmosp.physics.utoronto.ca"],
                                    shell=True, capture_output=True,
                                    text=True)
        status_server = output.returncode == 0
        
        if status_server == False and ping_only == False:
            log("ISSUE: Troubleshoot connection: Carbon/server was unpingable despite internet connection. Server-side problem?")

    if ping_only == False:
        log("Ping check results, localhost: %s, internet: %s, server: %s" % (status_localhost, status_internet, status_server))

    return status_localhost, status_internet, status_server

def networkmanager_restart():
    output = subprocess.run(["sudo systemctl restart NetworkManager"],
                                shell=True, capture_output=True,
                                text=True)
    
    ## TODO remove test lines below for disabling wifi
    subprocess.run("sudo nmcli connection down UofT",
    shell=True, text=True)
    
    if output.returncode == 0:
        log("NetworkManager was restarted.")
        sleep(10)
        
    else:
        log("NetworkManager was not successfully restarted. Return code: %s" % str(output.returncode))


def initialize_ssh(timeout=10):
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    tries = 0
    
    while True:
        
        try:
            ssh.connect(hostname="carbon.atmosp.physics.utoronto.ca", username="mpanas", port=2222, timeout=timeout)
        
            sftp = ssh.open_sftp()
            sftp.get_channel().settimeout(10)
            log("ssh/sftp session was initialized successfully.")
            
            break
            
        except TimeoutError as e:
            log("ISSUE: ssh session connect attempt timed out: %s" % str(e))
            tries += 1
            
        except OSError as e:
            log("ISSUE: ssh session connect attempt encountered error: %s" % str(e))
            tries += 1
            
        if tries >= 3:
            log("ISSUE: ssh session connect attempt failed thrice. Continuing without ssh session and will retry later.")
            ssh = None
            sftp = None
            break

    
    return ssh, sftp

def setup_remote_dirs(ssh, sftp, remote_lowres_dir, remote_hires_dir, remote_path):
    
    ## low res
    ## check if remote dir exists, will give error if not
    try:
        output = sftp.listdir(os.path.join(remote_path.split(":")[1], remote_lowres_dir))
        if output:
            log("Remote directory setup found existing low res directory %s, no new directory was made." % os.path.dirname(remote_lowres_dir))
        
    ## make the directory when it is not found
    except FileNotFoundError:
        ssh.exec_command("mkdir -p %s" % os.path.join(remote_path.split(":")[1], remote_lowres_dir))
        log("Remote directory was created: %s" % remote_lowres_dir)
    
    ## handle other exceptions
    except Exception as e:
        log("ISSUE: while setting up remote low res directory by sftp, exception was encountered: %s" % str(e))
    
    
    ## high res
    ## check if remote dir exists, will give error if not
    try:
        output = sftp.listdir(os.path.join(remote_path.split(":")[1], remote_hires_dir))
        if output:
            log("Remote directory setup found existing high res directory %s, no new directory was made." % os.path.dirname(remote_hires_dir))
        
    ## make the directory when it is not found
    except FileNotFoundError:
        ssh.exec_command("mkdir -p %s" % os.path.join(remote_path.split(":")[1], remote_hires_dir))
        log("Remote directory was created: %s" % remote_hires_dir)
    
    ## handle other exceptions
    except Exception as e:
        log("ISSUE: while setting up remote high res directory by sftp, exception was encountered: %s" % str(e))
    

def cleanup_ssh(ssh, sftp):
    try:
        sftp.close()
        ssh.close()
    except Exception as e:
        log("ISSUE: an exception occured while cleaning up ssh session: %s" % str(e))


def log(message):
    ## write messages to a monthly log file
    global lock
    
    date = dt.utcnow()
    status = lock.acquire(timeout=10)
    
    if status == True:
        try:
            
            filename = "/home/pi/logs/"+str(hpp_name)+"_log_"+date.strftime("%Y_%m")+".txt"
            
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'a') as f:
                f.write(date.isoformat()+"\t\t"+str(message)+"\n")
                
        except:
            print("Problem logging a message.")
            print(message)

        lock.release()
        
    else:
        print("Log function unable to acquire lock while logging:\n%s" % str(message))

def check_networkmanager(logging=True):

    output = subprocess.run("nmcli connection show --active | grep wifi",
    shell=True, capture_output=True, text=True)
    status_wifi = bool(output.stdout)
    
    output = subprocess.run("nmcli connection show --active | grep gsm",
    shell=True, capture_output=True, text=True)
    status_gsm = bool(output.stdout)
    
    output = subprocess.run("nmcli connection show --active | grep ethernet",
    shell=True, capture_output=True, text=True)
    status_eth = bool(output.stdout)
    
    if logging == True:
        log("NetworkManager check, (wifi, gsm, ethernet): "+str((status_wifi, status_gsm, status_eth)))
    
    return status_wifi, status_gsm, status_eth


def start_logging(interval):
    ## This is the main sensor function
    ## Initializes the sensor and then takes measurements every interval seconds
    
    ## set paths for important script parts
    port = subprocess.run(["ls /dev/serial/by-id/usb-FTDI*"],
                   shell=True, capture_output=True, text=True
                   ).stdout.strip("\n")
        
    # port = '/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_A600PJOV-if00-port0' # Port for Raspberry Pi 3
    config = '/home/pi/Desktop/Python3Scripts/my_parameters.config' # Path where to read which parameters to log

    ## set up lock for allowing the threads to write to log file safely
    global lock
    lock = Lock()

    hpp_serial = get_init_sensor_data(port).strip(' ')
    global hpp_name
    hpp_name = "HPP"+hpp_serial[-4:] # Last 4 digits of meter ID of HPP

    log("Initializating Carbonode %s..." % hpp_name)

    ## set up header and read information from location config file
    csv_header = ["#","HPP_serial", hpp_serial]
    location_config = load_location_config()

    ## set up GPS and record location
    gps_session = gps.gps(mode=gps.WATCH_ENABLE)
    gps_dict, gps_session, location_config = poll_gps(gps_session, 10, location_config)   # 10 s time out
    
    csv_header.extend(location_config)

    ## get parameter names and set up csv column names header
    csv_col_names = ['Timestamp']
    parameters = get_parameter_names(config)
    csv_col_names.extend(parameters)
    csv_col_names.extend(["DHT_Temperature", "DHT_Relative_Humidity", "CPU_Temperature", "12V_status"])
    parameter_formats = make_parameter_formats(csv_col_names)
    
    ## run important initialization steps
    ## see individual functions for more info
    chrony_nmea_output, chrony_star_output = check_time_sync()
    path, filename, time_file = setup_paths_filenames(hpp_name)
    setup_csv_headers(path, filename, csv_header, csv_col_names)
    
    ## initialize GPIO pin for reading 12V sensor status
    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(11, GPIO.IN, pull_up_down=GPIO.PUD_UP)   # set board pin number 11 to input with pull-up resistor
    
    ## start the ssh session and sftp instance for sending data
    ssh, sftp = initialize_ssh()
    
    ## set up thread for sending data/files and queue for low res data packets
    q_data_tuples = Queue()
    Thread(target=communicate, daemon=True, args=[q_data_tuples, ssh, sftp, "mpanas@carbon.atmosp.physics.utoronto.ca:/export/data2/tame-insitu/carbonodes"]).start()
    # Thread(target=communicate, daemon=True, args=[q_data_tuples, ssh, sftp, "mgp@100.1.1.1:/home/mgp/Documents/test/"]).start()

    
    ## initialize variables for the measurement loop
    DHT_works = True #Boolean to control if DHT works or not
    time_prev = False
    counter = 0
    lowres_data = pd.DataFrame(columns=["Time","CO2", "P", "T", "RH", "IR", "12V"])
    time_last_packet = dt.now()
    
    print("Beginning measurement loop...")
    log("Beginning measurement loop...")
    
    while True: # Never ending loop for measurement
        try:
        
            row = []
            time0 = dt.now()
            
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
            row.append(round(int(subprocess.run(["cat /sys/class/thermal/thermal_zone0/temp"],
                           shell=True, capture_output=True, text=True
                           ).stdout.strip("\n")),-2) // 100)
                
            ## read in the 12V sensor status
            try:
                row.append(GPIO.input(11))
            except:
                row.append(-9999)
            
            time1 = dt.now()
            
            time_row = time0 + (time1 - time0)/2
            
            row.insert(0, time_row.isoformat(timespec='milliseconds'))
            
            ## adjust formats for certain parameters in row
            for k,v in parameter_formats.items():
                
                if v[0] == "hex" and row[k] != -9999:
                    try:
                        row[k] = f"{int(row[k],0):{v[1]}}"
                    except Exception as e:
                        log("Parameter exception while parsing hex key,value %s,%s: %s" % (str(row[k]), str(v[1]), e))
                        # print(row[k])
                        row[k]=-9999
                else:
                    row[k] = f"{row[k]:{v[1]}}"
            
            ## check if the day changed during the measurement
            if time_row.day != time_file.day:
                
                chrony_nmea_output, chrony_star_output = check_time_sync()
                
                gps_dict, gps_session, location_config = poll_gps(gps_session, 10, location_config)   # 10 s time out
                    
                csv_header[3:] = location_config
    
                path, filename, time_file = setup_paths_filenames(hpp_name)
            
            ## check for an existing file, and write header to file if not
            setup_csv_headers(path, filename, csv_header, csv_col_names)
            
            ## append measured values to csv
            with open(os.path.join(path, filename),'a') as f:
                wr = csv.writer(f, dialect='excel')
                wr.writerow(row)
    
            ## send data section
            ## add current row to low resolution packet accumulator
            lowres_data = pd.concat((lowres_data, 
                                        pd.DataFrame([[time_row,
                                                       row[4], row[23], row[24],
                                                       row[25], row[20], row[27]]],
                                        columns=lowres_data.columns)))
            
            
            ## check time since last queued data packet
            if (dt.now() - time_last_packet).total_seconds() >= 600:
            # if (dt.now() - time_last_packet).total_seconds() >= 60:
    
                t_mean = 10
                lowres_data = lowres_data.astype({"Time":np.datetime64,
                                                  "CO2":np.int16, "P":np.int16,"T":np.int16,
                                                  "RH":np.int16,"IR":np.int32, "12V":int})
                
                lowres_data["unixtime"] = (lowres_data["Time"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")
                
                ## set up bins that are 10 s wide in the range of times present in the data
                bins = pd.date_range(lowres_data["Time"].iloc[0] - pd.Timedelta('1s'), lowres_data["Time"].iloc[-1] + pd.Timedelta(str(t_mean)+'s'), freq=str(t_mean)+'s')
            
                ## set up labels to use to identify each bin
                labels = np.arange(0, bins.size - 1)
                
                ## assign a new column "timebin" based on the bins and labels
                lowres_data["timebin"] = pd.cut(lowres_data["Time"],bins=bins,labels=labels)
                
                ## use the timebin to aggregate the data and take the mean
                lowres_data = lowres_data.replace(-9999,np.NaN).groupby("timebin", as_index=False, observed=False).mean().replace(np.nan, -9999).round().astype(int)
                
                lowres_data["Time"] = pd.to_datetime(lowres_data["unixtime"], unit='s', origin='unix')
                
                # print(lowres_data.columns)
                # print(lowres_data.head)
                
                ## add packet to queue
                q_data_tuples.put((path, filename, time_file, lowres_data, location_config))
                
                ## clear the low resolution data accumulator
                lowres_data = pd.DataFrame(columns=["Time","CO2", "P", "T", "RH", "IR", "12V"])
                
                time_last_packet = time_row
    
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
        
        except KeyboardInterrupt:
            print("\n###################\nKeyboard interrupt\n###################")
            log("Keyboard interrupt from user. Stopping sensor program...")
            
            print("Waiting for communication thread to empty its queue...")
            q_data_tuples.join()
            
            cleanup_ssh(ssh, sftp)
            GPIO.cleanup()
            
            print("\nSensor program shut down successfully.")
            log("Sensor program shut down successfully.")
            print("\nRun shutdown.py script to finish sensor shutdown procedure.")
            print("$ python3 ~/Desktop/Python3Scripts/shutdown.py")
            
            break
        
if __name__ == "__main__":
    
    try:
        initial_sleep = int(sys.argv[1])
    except IndexError:
        initial_sleep = 60
        
    sleep(initial_sleep)   # initial sleep to allow for user interrupt and gps cold start
    
    start_logging(2.5)