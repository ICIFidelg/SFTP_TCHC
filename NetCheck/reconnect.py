import subprocess
import os
import time

o=os.popen("ping -c4 -w10 8.8.8.8").read()
myArray=o.split("\n")
responses=len(myArray)
#print(myArray)
if responses<9:
	os.popen("ifdown eth0")
	os.popen("ifup eth0")
	#os.popen("systemctl restart openvpn")
	print("I did re-connect to the Network")
else:print("I did nothing, eth0 connection is ok")

