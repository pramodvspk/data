from pyspark import SparkConf, SparkContext
from datetime import datetime
from collections import namedtuple
import csv
from StringIO import StringIO 

conf = SparkConf().setMaster("local[*]").setAppName("Flights-Mining")
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

def notHeader(row):
	return "Description" not in row

def split(line):
	reader = csv.reader(StringIO(line))
	return reader.next()

airports = sc.textFile("file:///SparkExamples/airports.csv").filter(notHeader).map(split)

airportLookUp = airports.collectAsMap()
airportBC = sc.broadcast(airportLookUp)

flights = sc.textFile("file:///SparkExamples/flights.csv")
#flights = sc.textFile("/user/pramodsripada7155/flightdata")
flightsParsed = flights.map(lambda x : x.split(",")).map(parseRow).persist()
airportDelays = flightsParsed.map(lambda x: (x.origin, x.dep_delay))
#(origin, delay)
airportTotalDelay = airportDelays.reduceByKey(lambda x,y: x+y)
airportCount = airportDelays.mapValues(lambda x: 1).reduceByKey(lambda x,y: x+y)
#Join 
airportSumCount = airportTotalDelay.join(airportCount)
airportSumCount2 = airportDelays.combineByKey((lambda value: (value, 1)),
											   (lambda acc, value: (acc[0]+ value, acc[1]+1)),
											   (lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1])))
airportAvgDelay = airportSumCount2.mapValues(lambda x: x[0]/float(x[1])).map(lambda x:(airportBC.value[x[0]], x[1]))
#(airportCode, avgDelay)
worstAirports = airportAvgDelay.sortBy(lambda x:-x[1]).take(10)