#! /usr/bin/python

import argparse
import csv
import time
import MySQLdb

parser = argparse.ArgumentParser()
parser.add_argument("myFile", type=str, help="the file to be imported")
#parser.add_argument("-f", type=str, help="the exponent")
#parser.add_argument("-f", "--verbosity", action="count", default=0)


def readFile(theFile):
	with open(theFile) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
	print("file succesfully imported")
args = parser.parse_args()
answer = readFile(args.myFile)

if args.verbosity >= 2:
	print("{} to the power {} equals {}".format(args.x, args.y, answer))
elif args.verbosity >= 1:
	#print("{}^{} == {}".format(args.x, args.y, answer))
	print("..reading file {}",format(args.myFile))
else:
	print(answer)


