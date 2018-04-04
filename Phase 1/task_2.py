import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
geotweets = sc.textFile("data/geotweets.tsv")
records = geotweets.map(lambda x: x.split("\t"))

countryTweetCount = records.map(lambda rec: (rec[1], 1)).reduceByKey(lambda x,y: x+y)
sortedCount = countryTweetCount.sortByKey(True,1)
reallySortedCount = sortedCount.sortBy(lambda x: x[1], False)
reallySortedCount.coalesce(1,True).saveAsTextFile('data/result_5.tsv')
