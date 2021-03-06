import findspark
import datetime, time

findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
geotweets = sc.textFile("data/geotweets.tsv")
context = SparkContext.getOrCreate(sc)
records = geotweets.map(lambda x: x.split("\t"))
sampleGeo = geotweets.sample(False, 0.005, 5)
sampleRec = sampleGeo.map(lambda x: x.split("\t"))

#Filtering only relevant columns
countryCode= records.map(lambda x: (x[2],x[3],x[4]))
#sorting on US and city tags
justUS = countryCode.filter(lambda x: x[0]=='US' and x[1]=='city')
#just place needed for counting
place= justUS.map(lambda x: (x[2], 1))
#counting
counted= place.reduceByKey(lambda x,y: x+y).sortByKey(True,1).sortBy(lambda x: x[1], False).take(5)

citiesRDD=context.parallelize(counted)

stopWords = []
with open('data/stop_words.txt', 'r') as stopwords:
    for word in stopwords:
        stopWords.append(word.strip())
stopwords.close()

# Stepwise we:
# Filter out tweets from US
# Flatmap to change tweet sentences to words, make them lowercase
# Filter out stopwords and words shorter than length 2
# Map and reduce to count words on the form (word, wordcount)
# Sort words by count value, take top 10 words

usTweets = records.map(lambda x: (x[4],x[10])) \
                .join(citiesRDD).sortBy(lambda x: x[1][1], False) \
                .map(lambda x: ((x[0],x[1][0]),x[1][1])) \
                .flatMap(lambda x: (((x[1],x[0]),y) for y in x[0][1].lower().split(" "))) \
                .filter(lambda x: x[1] not in stopWords and len(x[1]) > 1) \
                .map(lambda x: (((x[0][0],x[0][1][0],x[1]),1))) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: (((x[0][0],x[0][1])),(x[0][2],x[1]))).sortBy(lambda x: x[1][1], False) \
                .groupByKey().mapValues(list).sortBy(lambda x: x[0][0], False) \
                .map(lambda x: (x[0][1],x[1][:10]))

usTweets.coalesce(1,True).saveAsTextFile('data/result_8.tsv')
