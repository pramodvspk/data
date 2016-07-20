from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("Playground")
sc = SparkContext(conf = conf)

def processLog(line):
	global errcount
	dateField = line[:24]
	logField = line[24:]

	if "ERROR" in line:
		errcount = errcount + 1

	return (dateField, logField)

errcount = sc.accumulator(0)

logs = sc.textFile("file:///SparkExamples/hbase.log")
logs.map(processLog).saveAsTextFile("file:///SparkExamples/output")






