import findspark
findspark.init()

from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col, countDistinct, approxCountDistinct

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
rdd = context.textFile("data/geotweets.tsv", use_unicode=True)
records = rdd.map(lambda x: x.split("\t")) \
        .map(lambda x: (int(x[0]), x[1], x[2], x[3], x[4], x[5], x[6], x[7], int(x[8]), int(x[9]), x[10], float(x[11]), float(x[12])))
sqlContext = SQLContext(context)

schema = StructType([
    StructField("utc_time", LongType(), True),
    StructField("country_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("place_type", StringType(), True),
    StructField("place_name", StringType(), True),
    StructField("language", StringType(), True),
    StructField("username", StringType(), True),
    StructField("user_screen_name", StringType(), True),
    StructField("timezone_offset", IntegerType(), True),
    StructField("number_of_friends", IntegerType(), True),
    StructField("tweet_text", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)])

df = sqlContext.createDataFrame(records, schema)
df.printSchema()

# Number of tweets
numberOfTweets = df.count()
print("Number of tweets: " + str(numberOfTweets))

# Count distinct usernames, country names, place names and languages
distvals = df.agg(countDistinct("username"), countDistinct("country_name"), countDistinct("place_name"), countDistinct("language"))
# Shows the table with the distinct values
distvals.show()

# Min values for latitude and longitude
minValues = df.agg({"latitude": "min", "longitude": "min"})
minValues.show()

# Max values for latitude and longitude
maxValues = df.agg({"latitude": "max", "longitude": "max"})
maxValues.show()
