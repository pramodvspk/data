from pyspark import SparkConf, SparkContext
from datetime import datetime
from collections import namedtuple

conf = SparkConf().setMaster("local").setAppName("Flights-Mining")
sc = SparkContext(conf = conf)

fields = ('date', 'airline', 'flightnum', 'origin', 'dest', 'dep', 'dep_delay', 'arv', 'arv_delay', 'airtime', 'distance')
Flight = namedtuple('Flight', fields, verbose= True)
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

def parseRow(row):
	row[0] = datetime.strptime(row[0], DATE_FMT).date()
	row[5] = datetime.strptime(row[5], TIME_FMT).time()
	row[6] = float(row[6])
	row[7] = datetime.strptime(row[7], TIME_FMT).time()
	row[8] = float(row[8])
	row[9] = float(row[9])
	row[10] = float(row[10])
	return Flight(*row[:11]) 

flights = sc.textFile("file:///SparkExamples/flights.csv")
flightsParsed = flights.map(lambda x : x.split(",")).map(parseRow)
print(flightsParsed.first())
# totalDistance = flightsParsed.map(lambda x: x.distance).reduce(lambda x,y: x+y)
# print(totalDistance)