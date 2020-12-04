import ftplib
import time
import MySQLdb
import csv
import os
import logging
import socket
import subprocess

#This script uses file taken from the input to update the Meters table from ICILocal database
#you may want to use example.csv and edit it as per your needs
#this script will skip row number 1 as if there were a header
# Standard ICI Database definitions apply 

db = MySQLdb.connect('127.0.0.1','ICI','pickering','ICILocal')
cursor = db.cursor()

filename = input("Enter the csv file for Meters table:")

# First I should fill the blanks of all those SN fields blank
# with whatever information in on Tables meter
# That way Im going to start completely fresh

db.query("SELECT SN,MeterId FROM Meters")
r=db.store_result()
Meters=r.fetch_row(0,1)

for a in Meters:
	SN=a["SN"]
	MeterParam=a["MeterId"]+"-kwh"
	#print(SN)
	#print(MeterParam)
	sql1="UPDATE Reg5Min SET SN='%s' WHERE MeterParam='%s' AND SN IS NULL" %(SN,MeterParam)
	#print(sql1)
	cursor.execute(sql1)
	db.commit()
	sql2="UPDATE Reg5MinReadings SET SN='%s' WHERE MeterParam='%s' AND SN IS NULL" %(SN,MeterParam)
	#print(sql2)
	cursor.execute(sql2)
	db.commit()
	print("filling blanks in SN...")


# Now Im changing the Meters, Reg5Min and Reg5MinReadings table with the new Information 
#available in the new Meters.csv file

try:
	with open(filename) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
		next(csv_file) # This skipe the first row known as header
		for row in csv_reader:
			#Now I need to update the Meters table with the information I just gathered
			sql="UPDATE Meters SET MeterId='%s' WHERE SN='%s'" %(row[1],row[0])
			#print(sql)
			cursor.execute(sql)
			db.commit()
			NewMeterParam=row[1]+"-kwh"
			#Reg5Min Table needs to be updated too
			sql1="UPDATE Reg5Min SET MeterParam='%s' WHERE SN='%s'" %(NewMeterParam,row[0])
			#print(sql1)
			cursor.execute(sql1)
			db.commit()
			#As well as Reg5MinReadings
			sql2="UPDATE Reg5MinReadings SET MeterParam='%s' WHERE SN='%s'" %(NewMeterParam,row[0])
			#print(sql2)
			cursor.execute(sql2)
			db.commit()
			print("Changing values in data tables")
except Exception as e:
	print(e)



db.close()

