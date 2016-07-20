from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("PageRank")
sc = SparkContext(conf = conf)

def computeContribs(urls, rank):
	contributors = []
	no_neighbors = len(urls)
	for url in urls:
		contributors.append((url, rank/no_neighbors))
	return contributors

#Filter out the headers, split the fromNode and toNode into a list []
googleRDD = sc.textFile("file:///SparkExamples/web-Google.txt").filter(lambda x: "#" not in x).map(lambda x: x.split("\t"))
#Create a links RDD, with a fromNode Id and a list of toNodes's and cache it
linksRDD = googleRDD.groupByKey().partitionBy(100).cache()
#Set the ranks of all nodes to 1
ranksRDD = linksRDD.map(lambda link: (link[0], 1))

for i in range(1):
	#Join the links and the ranks RDD, to get the information of a node, its neighbors and its current rank and transfer the ranks
	contribsRDD = linksRDD.join(ranksRDD).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
	#Calculate the ranks RDD again
	ranksRDD = contribsRDD.reduceByKey(lambda x,y: x+y).mapValues(lambda rank: rank*0.85 + 0.15)

for (link, rank) in ranksRDD.sortBy(lambda x:-x[1]).take(10):
	print "Link: ", link, " has a rank: ", rank
