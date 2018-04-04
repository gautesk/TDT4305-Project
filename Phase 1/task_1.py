#import findspark
#findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
geotweets = context.textFile("data/geotweets.tsv")
records = geotweets.map(lambda x: x.split("\t"))

# 1a) Count the number of tweets in our dataset
numberOfTweets = records.count()
print("Number of teweets: " + str(numberOfTweets))

# 1b) Count the number of distinct usernames in dataset
countDistinctUsernames = records.map(lambda rec: rec[6]).distinct().count()
print("Number of distinct usernames: " + str(countDistinctUsernames))

# 1c) Count distinct countries
countDistinctCountries = records.map(lambda rec: rec[1]).distinct().count()
print("Number of distinct countries: " + str(countDistinctCountries))

# 1d) Count distinct places
countDistinctPlaces = records.map(lambda rec: rec[4]).distinct().count()
print("Number of distinct places: " + str(countDistinctPlaces))

# 1e) How many languages users post tweets
countDistinctLanguages = records.map(lambda rec: rec[5]).distinct().count()
print("Number of distinct languages: " + str(countDistinctLanguages))

# 1f) Find minimum latitude
findMinLatitude = records.map(lambda rec: float(rec[11])).min()
print("Minimum latitude: " + str(findMinLatitude))

# 1g) Find minimum longitude
findMinLongitude = records.map(lambda rec: float(rec[12])).min()
print("Minimum longitude: " + str(findMinLongitude))

# 1h) Find maximum latitude
findMaxLatitude = records.map(lambda rec: float(rec[11])).max()
print("Maximum latitude: " + str(findMaxLatitude))

# 1i) Find maximum longitude
findMaxLongitude = records.map(lambda rec: float(rec[12])).max()
print("Maximum longitude " + str(findMaxLongitude))

# 1j) Average length of tweet text (characters)
averageCharsTweet = records.map(lambda rec: len(rec[10])).sum()
averageLengthChars = averageCharsTweet / numberOfTweets
print("Average characters per tweet: " + str(averageLengthChars))

# 1k Average length of tweet text (words)
averageWordsTweet = records.map(lambda rec: len(rec[10].split())).sum()
averageLengthWords = averageWordsTweet / numberOfTweets
print("Average words per tweet: " + str(averageLengthWords))

# Parallelize all answers as a rdd to be written out
results = context.parallelize([
    numberOfTweets,
    countDistinctUsernames,
    countDistinctCountries,
    countDistinctPlaces,
    countDistinctLanguages,
    findMinLatitude,
    findMaxLatitude,
    findMinLongitude,
    findMaxLongitude,
    averageLengthChars,
    averageLengthWords
])

# Write rdd with answers to a tsv file
results.coalesce(1,True).saveAsTextFile('data/result_1.tsv')
