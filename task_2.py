import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
geotweets = context.textFile("data/geotweets.tsv", use_unicode=True)
records = geotweets.map(lambda x: x.split("\t"))

# Map our daya to (key, value) pair with (country, 1)
# Reduce by key (country) to count tweets from each
# Sort by country first to get alphabetical sort, required for countries with equal tweets in next sort
# Finally sort by value
tweetsPerCountry = records.map(lambda x: (x[1], 1)) \
    .reduceByKey(lambda x,y: x+y) \
    .sortByKey(lambda x: x) \
    .sortBy(lambda x: x[1], False)

# Write out results
tweetsPerCountry.coalesce(1,True).saveAsTextFile('data/result_2')
