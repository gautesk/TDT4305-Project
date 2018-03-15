import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
geotweets = context.textFile("data/geotweets.tsv", use_unicode=True)
records = geotweets.map(lambda x: x.split("\t"))

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
usTweets = records.filter(lambda x: x[1] == 'United States') \
                .flatMap(lambda x: x[10].lower().split(" ")) \
                .filter(lambda x: x not in stopWords and len(x) > 1) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda x, y: x + y) \
                .sortBy(lambda x: x[1], False) \
                .take(10)

final=context.parallelize(usTweets)

final.coalesce(1,True).saveAsTextFile('data/result_6tsv')
