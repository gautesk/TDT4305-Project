import findspark
import datetime, time

findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
geotweets = sc.textFile("data/geotweets.tsv")
#records = geotweets.map(lambda x: x.split("\t"))
sampleGeo = geotweets.sample(False, 0.0005, 5)
sampleRec = sampleGeo.map(lambda x: x.split("\t"))

countryCode= sampleRec.map(lambda x: (x[2],x[3]))

justUS = countryCode.reduce(lambda x: )

justUS.coalesce(1,True).saveAsTextFile('data/result_5.tsv')
