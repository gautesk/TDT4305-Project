import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

def centroid_of_List(latitude, longitude):
    latitudeCentroid = latitude.sum() / len(latitude)
    longitudeCentroid = longitude.sum() / len(longitude)
    return latitudeCentroid, longitudeCentroid

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
geotweets = context.textFile("data/geotweets.tsv", use_unicode=True)
records = geotweets.map(lambda x: x.split("\t"))

tweetsPerCountry = records.map(lambda x: (x[1], 1)) \
    .reduceByKey(lambda x,y: x+y) \
    .filter(lambda x: int(x[1]) > 10) \
    .map(lambda x: x[0]) \
    .collect()

tweetsFilteredByCountries = records.filter(lambda x: x[1] in countriesWithMoreThan10) \
    .map(lambda x: (x[1], x[11]))


aTuple = (0,0)
avgLatitude = records.map(lambda x: (x[1], float(x[11]))) \
    .aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1),
                            lambda a,b: (a[0] + b[0], a[1] + b[1])) \
                            .mapValues(lambda v: v[0]/v[1])

avgLongitude = records.map(lambda x: (x[1], float(x[12]))) \
    .aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1),
                            lambda a,b: (a[0] + b[0], a[1] + b[1])) \
                            .mapValues(lambda v: v[0]/v[1])

latitudeAndLongitude = avgLatitude.join(avgLongitude)

latitudeAndLongitude.map(lambda x: str(x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][1])).coalesce(1,True).saveAsTextFile('data/result_3.tsv')
