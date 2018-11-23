from pyspark import SparkConf, SparkContext  #can not create sparkContext without SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
#collection pacakge from python and use OrderedDict to create ordered dictionary
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
