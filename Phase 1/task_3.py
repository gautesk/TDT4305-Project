import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
geotweets = context.textFile("data/geotweets.tsv", use_unicode=True)
records = geotweets.map(lambda x: x.split("\t"))

# Collects a list of all countries with more than 10 tweets
countriesWithMoreThan10 = records.map(lambda x: (x[1], 1)) \
    .reduceByKey(lambda x,y: x+y) \
    .filter(lambda x: int(x[1]) > 10) \
    .map(lambda x: x[0]) \
    .collect()

# Filter RDD with all tweets to only contain countries with more than 10 tweets
filteredTweets = records.filter(lambda x: x[1] in countriesWithMoreThan10)

aTuple = (0,0)
# Calculate centroid latitude value of each country
avgLatitude = filteredTweets.map(lambda x: (x[1], float(x[11]))) \
    .aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1),
                            lambda a,b: (a[0] + b[0], a[1] + b[1])) \
                            .mapValues(lambda v: v[0]/v[1])

# Calculate centroid longitude value of each country
avgLongitude = filteredTweets.map(lambda x: (x[1], float(x[12]))) \
    .aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1),
                            lambda a,b: (a[0] + b[0], a[1] + b[1])) \
                            .mapValues(lambda v: v[0]/v[1])

# Join longitude and latitude values on key country
latitudeAndLongitude = avgLatitude.join(avgLongitude)

# Write out results to tsv file
latitudeAndLongitude.map(lambda x: str(x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][1])).coalesce(1,True).saveAsTextFile('data/result_3.tsv')
