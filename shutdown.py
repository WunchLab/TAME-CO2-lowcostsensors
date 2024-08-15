#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  1 12:11:37 2024

@author: mgp
"""

import pandas as pd
import datetime as dt
from time import sleep
import subprocess


print("""
   ______           __                          __        _____ __          __      __                  
  / ____/___ ______/ /_  ____  ____  ____  ____/ /__     / ___// /_  __  __/ /_____/ /___ _      ______ 
 / /   / __ `/ ___/ __ \/ __ \/ __ \/ __ \/ __  / _ \    \__ \/ __ \/ / / / __/ __  / __ \ | /| / / __ \ 
/ /___/ /_/ / /  / /_/ / /_/ / / / / /_/ / /_/ /  __/   ___/ / / / / /_/ / /_/ /_/ / /_/ / |/ |/ / / / / 
\____/\__,_/_/  /_.___/\____/_/ /_/\____/\__,_/\___/   /____/_/ /_/\__,_/\__/\__,_/\____/|__/|__/_/ /_/ 
                                                                                                        
                                       _____           _       __
                                      / ___/__________(_)___  / /_
                                      \__ \/ ___/ ___/ / __ \/ __/
                                     ___/ / /__/ /  / / /_/ / /_
                                    /____/\___/_/  /_/ .___/\__/
                                                    /_/
      """)


lookup = pd.read_csv("/home/pi/Desktop/Python3Scripts/location_lookup.csv", index_col="SITE")
# lookup = pd.read_csv("/home/mgp/OneDrive/TAME/github/TAME-CO2-lowcostsensors/location_lookup.csv", index_col="SITE")


while True:
    
    answer = input("Do you need to change the location config file? (y/n/cancel): ")
    
    if answer == 'y':
        while True:
            
            print("\nPlease select from the following locations:")
            
            for i in range(lookup.index.shape[0]):
                if i != lookup.index.shape[0] -1:
                    print(str(lookup.index.values[i]), end=', ')
                else:
                    print(str(lookup.index.values[i]))
                    
            answer = input("\nEnter location code as shown above, or enter cancel: ")
            
            if answer in lookup.index.values:
                location_config = [-9999, -9999]
                
                location_config[0] = answer
                location_config[1] = dt.datetime.now().isoformat()
                
                
                # with open("/home/mgp/Documents/test/location.config", 'w') as f:
                #     f.write(location_config[0] + " " + location_config[1])
                
                with open("/home/pi/Desktop/Python3Scripts/location.config", 'w') as f:
                    f.write(location_config[0], location_config[1])
            
                break
            
            elif answer == "cancel":
                break
            
            else:
                print("\nInvalid input. Please type location code when prompted.")
        
        if answer == 'y':
            print("\nLocation config file was successfully updated.")
        
    
    if answer == 'n':
        print("\nLocation file was not updated. Bye!")
        break
    
    if answer == "cancel":
        print("\nExiting shutdown script. Bye!")
        break
    
    else:
        print("\nInvalid input. Please type y or n when prompted.")

if answer != "cancel":        
    print("\n\nShutting down in 5 s. Ctrl+c to interrupt.")
    sleep(5)
    subprocess.run(["sudo poweroff"], shell=True, text=True)