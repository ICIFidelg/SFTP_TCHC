#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 12:24:19 2020

@author: fidelg
"""

# This reads a float 32. 

import minimalmodbus
#import numpy as np
#import csv
import argparse
import struct
#import pandas as pd
#import time

parser = argparse.ArgumentParser()
parser.add_argument("port", type=str, help="the port to be used")
parser.add_argument("baudrate", type=str, help="Baurate to be used")
parser.add_argument("Id", type=str, help="Address to read in decimal")
parser.add_argument("Address", type=str, help="Address to read in decimal")
#parser.add_argument("-f", "--verbosity", action="count", default=0)
args = parser.parse_args()

usbPort="/dev/tty"+args.port
baudrate=args.baudrate
modbusAddress=int(args.Address)
modbusId=int(args.Id)
theArray=[]
newArray=[]
try:
	theMeter = minimalmodbus.Instrument(usbPort,modbusId,mode='rtu',debug=True)
	theMeter.serial.baudrate = baudrate
	theMeter.serial.timeout= 3
except Exception as e:
	print("No connection was possible due to:",e)
	
try:
	theArray=theMeter.read_registers(modbusAddress,2,3)
except Exception as e:
	read=None
	print("Could not connect becaus of:",e)

firstWord=theArray[0]<<16
secondWord=theArray[1] #Im reversing the 16 words here by putting the first word first
f=firstWord^secondWord
print (struct.unpack('f', struct.pack('I', f))[0])

#if(decodedUINT32 & 0x80000000): # This take care of the negative ones
#	decodedUINT32 = -0x100000000 + decodedUINT32
	