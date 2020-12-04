import ftplib
import time
import MySQLdb
import csv
import os
import logging
import socket
import subprocess


db = MySQLdb.connect('127.0.0.1','ICI','pickering','ICILocal')
cursor = db.cursor()

file="./FileForMeters.csv"

try:
	#Im going to update the Meters database with the new Information
	sql= "INSERT IGNORE INTO ftpReports(ftpServer,userName) VALUES('%s','%s')"%(server,username)
	cursor.execute(sql)
	db.commit()
	db.close()
	print("Data was succesfully recorded")
except Exception as e:
	print(e)
	print("Unable to write into database, so no data was recorded")
	pass
