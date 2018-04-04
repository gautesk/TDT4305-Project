import findspark
import datetime, time

findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
geotweets = sc.textFile("data/geotweets.tsv")
records = geotweets.map(lambda x: x.split("\t"))
sampleGeo = geotweets.sample(False, 0.0005, 5)
sampleRec = sampleGeo.map(lambda x: x.split("\t"))

#Filtering only relevant columns
countryCode= records.map(lambda x: (x[2],x[3],x[4]))
#sorting on US and city tags
justUS = countryCode.filter(lambda x: x[0]=='US' and x[1]=='city')
#just place needed for counting
place= justUS.map(lambda x: (x[2], 1))
#counting
counted= place.reduceByKey(lambda x,y: x+y).sortByKey(True,1).sortBy(lambda x: x[1], False)

counted.coalesce(1,True).saveAsTextFile('data/result_5.tsv')
