# This work is subject to Canadian Crown Copyright.
# © His Majesty the King in Right of Canada, as represented by the Minister of Environment and Climate Change, 2024.
# For more information, please consult the Canadian Intellectual Property Office.

# This work was authored by Environment and Climate Change Canada.
# Point of contact: Felix Vogel

##########

'''
This Python script provides the function that returns the temperature and humidity from the DHT
sensor
Modified by L. Gillespie 20220328
'''
# import adafruit_dht
# import board
# import time

# def DHT_sensor(pin):
#     dht = adafruit_dht.DHT22(pin) #initializes sensor with new library
#     time.sleep(0.1)
#     temp = dht.temperature #reads tempreautre from sensor
#     hum = dht.humidity #reads relative humidity from sensor
#     
#     #return same values as old code...
#     return [temp,hum]
#     


#################################################################
######################THE GRAVEYARD##############################
#################################################################
import Adafruit_DHT as DHT


def DHT_sensor(pin):
    humidity, temperature = DHT.read_retry(DHT.DHT22, pin)
    return [temperature, humidity]

