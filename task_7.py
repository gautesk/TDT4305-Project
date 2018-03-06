import findspark
<<<<<<< HEAD
import datetime, time

=======
>>>>>>> 5cdf18a59febf58688e644045ec45e6dfd223309
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf
<<<<<<< HEAD

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
geotweets = sc.textFile("data/geotweets.tsv")
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

cities=[]
for line in counted:
    cities.append(line[0])
=======
from collections import Counter

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
geotweets = context.textFile("data/geotweets.tsv", use_unicode=True).map(lambda x: x.split("\t"))
records = geotweets #.sample(False, 0.001, 5)
>>>>>>> 5cdf18a59febf58688e644045ec45e6dfd223309

stopWords = []
with open('data/stop_words.txt', 'r') as stopwords:
    for word in stopwords:
        stopWords.append(word.strip())
stopwords.close()

<<<<<<< HEAD
# Stepwise we:
# Filter out tweets from US
# Flatmap to change tweet sentences to words, make them lowercase
# Filter out stopwords and words shorter than length 2
# Map and reduce to count words on the form (word, wordcount)
# Sort words by count value, take top 10 words

usTweets = records.filter(lambda x: x[4] in cities) \
                .flatMap(lambda x: ((x[4],y) for y in x[10].lower().split(" "))) \
                .filter(lambda x: x[1] not in stopWords and len(x[1]) > 2) \
                .map(lambda x: ((x[0], x[1]),1)) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: ((x[0][0]),(x[0][1],x[1]))).sortBy(lambda x: x[1][1], False) \
                .groupByKey().mapValues(list) \
                .map(lambda x: (x[0],x[1][:10]))




usTweets.coalesce(1,True).saveAsTextFile('data/result_7.tsv')
"""
with open('data/result_7.tsv', 'w') as outputfile:
    for line in cities:
        outputfile.write(line + "\n")
outputfile.close()
"""
=======
def countWords(words):
    result = {}
    words = words.lower().split(' ')
    for word in words:
        if len(word)>2 and word not in stopWords:
            if result.get(word):
                result[word] += 1
            else:
                result[word] = 1
    result = dict(Counter(result).most_common(10))
    return [(key, value) for key, value in result.items()]

def formatWords(words):
    resultString = ""
    for word in words:
        resultString += word[0] + "\t" + str(word[1]) + "\t"
    return resultString

# Finne de 5 byene med flest tweets
top5Cities = records.filter(lambda x: x[2] == 'US' and x[3] == 'city') \
                .map(lambda x: (x[4], 1, x[10])) \
                .keyBy(lambda x: x[0]) \
                .aggregateByKey((0, ''), \
                (lambda x, y: (x[0]+y[1], x[1]+' '+y[2])), \
                (lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1]))) \
                .sortBy(lambda x: x[1], False) \
                .map(lambda x: (x[0], x[1][0], x[1][1])) \
                .top(5, key=lambda x: x[1]) \

topWords = context.parallelize(top5Cities) \
            .map(lambda x: (x[0], countWords(x[2]))) \
            .map(lambda x: x[0] + "\t" + formatWords(x[1])) \
            .coalesce(1) \
            .saveAsTextFile('data/result_7.tsv')
>>>>>>> 5cdf18a59febf58688e644045ec45e6dfd223309
