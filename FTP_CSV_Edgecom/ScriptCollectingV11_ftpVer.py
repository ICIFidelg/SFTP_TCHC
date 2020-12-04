#!/usr/bin/env python3 
#Version 11 Latest modify on April 6 12:38 pm
#In this version database changed to include type of utility
#the purpose of includying type of utility is because its needed
#to group by utility so repors with end user app makes sense
#Changes to mach new format of database that includes different rables
#for different tables (water gas or electricity)
# May 5 2020 sync drivers directory with ICISetup webpage
# June 1 2020 Get rid of the round(none) problem when the DCU does not pick up the read (meter off)

import minimalmodbus
minimalmodbus.CLOSE_PORT_AFTER_EACH_CALL=True
import time
import MySQLdb
import csv
import os
import logging
import ftplib
#import json
#import paho.mqtt.client as mqtt
import socket
import subprocess

### Main definitions over here, please!
databaseSetup="ICILocal"
#driverDirectory="/var/www/html/Setup/_lib/file/doc/"  This must be uncommented when running in an odroid
driverDirectory="/root/ICI/Drivers/"   #This must be un commented when running in an odroid daemon version

logging.basicConfig(filename='/root/ICI/logs/ICI-DB.log',format='%(name)s - %(asctime)s - %(levelname)s - %(message)s',level=logging.INFO) #un comment for daemon version
#logging.basicConfig(filename='./logs/ICI-DB.log',format='%(name)s - %(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)

db = MySQLdb.connect('127.0.0.1','ICI','pickering',databaseSetup)
cursor = db.cursor()


##End of main definitions

#db1 = MySQLdb.connect('127.0.0.1','ICI','pickering','ICILocal')
#cursor1 = db1.cursor()

#No remote connection on this version. I will connect from the server directly to the database
# an scheduled backup  at midnight every day in the case  cloud backup service is 
#purchased. The remote connection from the server will be SSL-rsync the dump will be unddumped just when asked to do so


#Prepare info from Meters tables and copying files from new drivers are
f=os.popen("rsync -r --include '*.ici' --exclude '*' /var/www/html/Setup/_lib/file/doc/ /root/ICI/Drivers/")
db.query("SELECT * FROM Meters")
r=db.store_result()
Meters=r.fetch_row(0,1)
NoOfMeters=len(Meters)
#print(Meters)

#Prepare the drivers tables
db.query("SELECT * FROM TypeOfMeter")
r=db.store_result()
TypeOfMeters=r.fetch_row(0,1)
NoOfTypeOfMeters=len(TypeOfMeters)
endString='      </td></tr><tr><td align="center">'
mappingData=[]


for x in range(0,NoOfMeters):
	NameOfFile=Meters[x]["ModbusMap"]
	MyFile = driverDirectory+NameOfFile
	with open(MyFile) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
		line_count = 0
		for row in csv_reader:
			if (line_count>=22):
				if row[0] == endString: break
				row.append(Meters[x]["SN"])
				row.append(Meters[x]["MeterId"])
				row.append(Meters[x]["ModbusId"])
				row.append(Meters[x]["Port"])
				row.append(Meters[x]["MeterType"])
				row.append(Meters[x]["ModbusMap"])
				row.append(Meters[x]["startAddress"])
				row.append(Meters[x]["multiplier"])
				row.append(Meters[x]["typeOfUtility"])
				row.append(Meters[x]["units"])
				mappingData.append(row)
				line_count += 1
			else:
				line_count += 1

noOfItems=len(mappingData)
#print(mappingData)
counter=0

			
def EM3Meter(Id,port,add,dataType,baudrate,timeout):
	usbPort="/dev/tty"+port
	try:
		theMeter = minimalmodbus.Instrument(usbPort,Id,mode='rtu')
		theMeter.serial.baudrate = baudrate
		theMeter.serial.timeout= timeout
	except:
		theMeter = minimalmodbus.Instrument(usbPort,Id,mode='rtu')
		theMeter.serial.baudrate = baudrate
		theMeter.serial.timeout= timeout
	register=theMeter.read_registers(add,2,3)
	string1=minimalmodbus._num_to_twobyte_string(register[0])
	string2=minimalmodbus._num_to_twobyte_string(register[1])
	string3=string2+string1
	floatValue=minimalmodbus._bytestring_to_float(string3)
	return floatValue

def I45Meter(Id,port,add,dataType,baudrate,timeout):
	usbPort="/dev/tty"+port
	try:
		theMeter = minimalmodbus.Instrument(usbPort,Id,mode='rtu')
		theMeter.serial.baudrate = baudrate
		theMeter.serial.timeout= timeout
	except Exception as e:
		logging.exception("I will wait for add %s on modbus ID %s",str(Id),str(add))
		time.sleep(2)
		try:
			theMeter = minimalmodbus.Instrument(usbPort,Id,mode='rtu')
			theMeter.serial.baudrate = baudrate
			theMeter.serial.timeout= timeout
		except Exception as e:
			logging.exception("I tried twice add %s on modbus ID %s and was unable to simply open the port",str(Id),str(add))
	if dataType=="UINT32":
		try:
			theValue=theMeter.read_long(add,3,False)
		except Exception as e:
			logging.exception("I will wait to read data from add %s on modbus ID %s",str(Id),str(add))
			time.sleep(2)
			try:
				theValue=theMeter.read_long(add,3,False)
				logging.info("I made it in the second attempt in add %s modbus ID %s", str(Id),str(add))
			except Exception as e:
				theValue=None
				logging.exception("I tried twice in add %s modbus ID %s and did not make it :(",str(Id),str(add))		
	if dataType=="INT32":
		try:
			theValue=theMeter.read_long(add,3,True)
		except:
			print("I will try again after sleeping")
			try:
				theValue=theMeter.read_long(add,3,True)
				print("I made in the second atempt")
			except:
				print("I tried twice and I did not make it")
			#pass
	theValue=theValue
	return theValue

def ISMAB8I(Id,port,add,dataType,baudrate,timeout):
	usbPort="/dev/tty"+port
	try:
		theMeter = minimalmodbus.Instrument(usbPort,Id,mode='rtu')
		theMeter.serial.baudrate = baudrate
		theMeter.serial.timeout= timeout
	except:
		os.system('/root/usbreset /dev/bus/usb/001/003')
		theMeter = minimalmodbus.Instrument(usbPort,Id,mode='rtu')
		theMeter.serial.baudrate = baudrate
		theMeter.serial.timeout= timeout
	register=theMeter.read_registers(add,2,3)
	string1=minimalmodbus._num_to_twobyte_string(register[0])
	string2=minimalmodbus._num_to_twobytes_tring(register[1])
	string3=string2+string1
	theValue=minimalmodbus._bytestring_to_long(string3)
	return theValue

def S5301(Id,port,add,dataType,baudrate,timeout):
	usbPort="/dev/tty"+port
	try:
		theMeter = minimalmodbus.Instrument(usbPort,Id,mode='rtu')
		theMeter.serial.baudrate = baudrate
		theMeter.serial.timeout= timeout
	except:
		theMeter = minimalmodbus.Instrument(usbPort,Id,mode='rtu')
		theMeter.serial.baudrate = baudrate
		theMeter.serial.timeout= timeout
	try:
		theValue=theMeter.read_long(add,3,True)
	except:
		time.sleep(60)
		theValue=theMeter.read_long(add,3,False)
	#print(theValue)
	return theValue


theMeters = {1: EM3Meter, 2: I45Meter, 3: ISMAB8I, 4:S5301}

class poollingStorage:
	def __init__(self,itemMap,timeStamping, baudrate, timeout): #I sent all the stuff i needed from the very begining
		self.itemMap = itemMap
		self.timeStamp = timeStamping
		self.baudrate = baudrate
		self.timeout = timeout
	def pooling(self):
		measurements=[]
		if self.itemMap[9]=="EM3":
			reading=theMeters[1](int(self.itemMap[7]),self.itemMap[8],int(self.itemMap[11])+int(self.itemMap[1]),self.itemMap[3],self.baudrate,self.timeout)
		if self.itemMap[9]=="i-45" or self.itemMap[9]=="i-636" or self.itemMap[9]=="MFX":
			reading=theMeters[2](int(self.itemMap[7]),self.itemMap[8],int(self.itemMap[11])+int(self.itemMap[1]),self.itemMap[3],self.baudrate,self.timeout)
		if	self.itemMap[9]=="ISMA-B-8I":
			reading=theMeters[3](int(self.itemMap[7]),self.itemMap[8],int(self.itemMap[11])+int(self.itemMap[1]),self.itemMap[3],self.baudrate,self.timeout)
		if	self.itemMap[9]=="S5301":
			reading=theMeters[4](int(self.itemMap[7]),self.itemMap[8],int(self.itemMap[11])+int(self.itemMap[1]),self.itemMap[3],self.baudrate,self.timeout)
		try:
			value=round(reading*float(self.itemMap[12]),2)
		except:
			value=None
		if self.itemMap[13]=="E":
			tableInterval="Reg5Min"
			tableReadings="Reg5MinReadings"
		elif self.itemMap[13]=="W":
			tableInterval="Reg5MinW"
			tableReadings="Reg5MinReadingsW"
		elif self.itemMap[13]=="G":
			tableInterval="Reg5MinG"
			tableReadings="Reg5MinReadingsG"
		if self.itemMap[4]=="yes":
			SN=self.itemMap[5]
			meterParam=self.itemMap[6]+"-"+self.itemMap[0]
			#print(meterParam)
			#print(tableReadings)
			db.query("SELECT value FROM %s WHERE SN='%s' AND TimeStamping=(SELECT MAX(TimeStamping) FROM %s WHERE SN='%s') "%(tableReadings,SN,tableReadings,SN) )
			r=db.store_result()
			queryResults=r.fetch_row(0,1)
			#print(len(queryResults))
			if (len(queryResults)==0):
				formerEnergy=0
			else:
				formerEnergy=queryResults[0]["value"]
				#print(formerEnergy)
			try:
				newEnergy=value-formerEnergy
			except:
				newEnergy=None
			newMeterParam=self.itemMap[6]+"-"+self.itemMap[0]
			try:
				sql= "INSERT INTO %s(TimeStamping,SN,MeterParam,value) VALUES('%s','%s','%s','%f')"%(tableInterval,self.timeStamp,self.itemMap[5],newMeterParam,newEnergy)
				cursor.execute(sql)
				db.commit()
			except:
				logging.exception("No data stored for ",newMeterParam,timeStamp)
				time.sleep(10)
			try:
				sql= "INSERT INTO %s(TimeStamping,SN,MeterParam,value) VALUES('%s','%s','%s','%f')"%(tableReadings,self.timeStamp,self.itemMap[5],newMeterParam,value)
				cursor.execute(sql)
				db.commit()
			except:
				logging.exception("No data stored for ",newMeterParam,timeStamp)
				time.sleep(10)
			measurements.append(self.timeStamp)
			measurements.append(self.itemMap[5])
			measurements.append(newMeterParam)
			measurements.append(value)
		else:
			meterParam=self.itemMap[6]+"-"+self.itemMap[0]
			sql= "INSERT INTO %s(TimeStamping,SN,MeterParam,value) VALUES('%s','%s','%s','%f')"%(tableReadings,self.timeStamp,self.itemMap[5],meterParam,value)
			cursor.execute(sql)
			db.commit()
			measurements.append(self.timeStamp)
			measurements.append(self.itemMap[5])
			measurements.append(newMeterParam)
			measurements.append(newEnergy)
		return (measurements)

#lets connect to the mqtt broker Not now

#broker_address = "localhost"
#broker_portno = 1883


#hostname = socket.gethostname()


#f= open("/var/www/html/Setup1/pidFile","r") #uncoment this when running an odroid

"""
status=[]
for val in f.read().split():
	status.append(str(val))
	f.close()
if status[0]=="ON":
	timeStamp = time.strftime("%Y-%m-%d %H:%M:00",time.localtime())
	for y in range(0,noOfItems):
		#print (mappingData[y])
		objetoConejo = poollingStorage(mappingData[y], timeStamp)
		data=objetoConejo.pooling()
		#dataFormatted=json.dumps(data)
		#print(dataFormatted)
		#print(mappingData[y][12])
		#client.subscribe(mappingData[y][12])
		#client.publish(topic = mappingData[y][12], payload = dataFormatted)
		#print(dataFormatted)
else: 
	print("Nothing to do")
"""	
#f= open("./pidFile","r") #Comment this when running in an odroid

def uploadTheFile(file2Upload,date2Confirm):
	db.query("SELECT * FROM ftpReports WHERE count=(SELECT MAX(count) FROM ftpReports)")
	r=db.store_result()
	serverArray=r.fetch_row(0,1)
	ftpServer=serverArray[0]["ftpServer"]
	userName=serverArray[0]["userName"]
	password=serverArray[0]["password"]
	ftpDirectory="./"+serverArray[0]["directory"]
	#print(ftpServer)
	#print(userName)
	#print(password)
	#print(ftpDirectory)
	try:
		session = ftplib.FTP(ftpServer, userName, password)	
	except ConnectionRefusedError:
		logging.fatal("Failed To Connect To The FTP Server {}@{}".format(userName, ftpServer))
		logging.fatal("Aborting Application...")
		pass
	
	logging.info("Uploading File {} To FTP Server".format(file2Upload))
	file = open(file2Upload, 'rb')  # file to send
	session.cwd(ftpDirectory)
	#print(os.path.basename(file2Upload))
	try:
		session.storbinary('STOR ' + os.path.basename(file2Upload), file)  # send the file
	except Exception as e:
		print(e)		
	file.close()  # close file and FTP
	session.quit()
	#print(date2Confirm)
	sql= "INSERT INTO date4FTP(previousDate,dataUpload) VALUES('%s','yes')"%(date2Confirm)
	cursor.execute(sql)
	db.commit()






	
while 1: #Lets start infinite loop
	f= open("/root/ICI/pidFile","r")  #Change this when running in an odroid
	MySavedSettings=[]
	with open('/root/ICI/ConfSettingsfile.csv', 'r') as file:
		reader = csv.reader(file)
		for row in reader:
			MySavedSettings.append(row)
	baudrate=int(MySavedSettings[1][0])
	timeOut=int(MySavedSettings[1][4])
	status=[]
	for val in f.read().split():
		status.append(str(val))
	f.close()
	seconds = time.time()
	#print(round(seconds,0))
	time.sleep(10)
	counter = counter+1
	#print(counter)
	if status[0]=="ON":
		myMinute=time.gmtime()[4]
		#print(myMinute)
		#if counter%90==0:
		if myMinute%5==0:
		#if myMinute==0 or myMinute==15 or myMinute==30 or myMinute==45:
			timeStamp = time.strftime("%Y-%m-%d %H:%M:00",time.localtime())
			#client = mqtt.Client(clean_session=True)
			#client.connect(broker_address, broker_portno)
			#for a in range(0,len(Meters)):
			#	topic=hostname+"/"+Meters[a]["MeterId"]
			#	client.subscribe(topic)
			for y in range(0,noOfItems):
				#print (mappingData[y])
				objetoConejo = poollingStorage(mappingData[y],timeStamp,baudrate,timeOut)
				data=objetoConejo.pooling()
				hostname = socket.gethostname()
				dateStamped=timeStamp.split(" ")[0]
				timeStamped=timeStamp.split(" ")[1]
				dateFormated=dateStamped+"_"+timeStamped
				file_name="/root/ICI/CSVs/"+hostname+"_"+dateFormated+".csv"
				with open(file_name, 'a+', newline='') as write_obj:
					csv_writer = csv.writer(write_obj)
					csv_writer.writerow(data)
				#print(data)
			#	dataFormatted=json.dumps(data)
				#print(dataFormatted)
			#	client.publish(topic = hostname+"/"+mappingData[y][12], payload = dataFormatted)
			try:
				uploadTheFile(file_name,timeStamp)
				os.popen("rm '%s'"%(file_name))
			except:
				sql= "INSERT INTO date4FTP(previousDate,dataUpload) VALUES('%s','no')"%(timeStamp)
				cursor.execute(sql)
				db.commit()			
			time.sleep(60)			
	else: 
		print("Nothing to do")
		break  
