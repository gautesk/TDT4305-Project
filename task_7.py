import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf
from collections import Counter

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
geotweets = context.textFile("data/geotweets.tsv", use_unicode=True).map(lambda x: x.split("\t"))
records = geotweets #.sample(False, 0.001, 5)

stopWords = []
with open('data/stop_words.txt', 'r') as stopwords:
    for word in stopwords:
        stopWords.append(word.strip())
stopwords.close()

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
