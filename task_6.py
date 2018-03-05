import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
geotweets = context.textFile("data/geotweets.tsv", use_unicode=True)
records = geotweets.map(lambda x: x.split("\t")).sample()

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
                .filter(lambda x: x not in stopWords and len(x) > 2) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda x, y: x + y) \
                .sortBy(lambda x: x[1], False) \
                .take(10)

# Write out top 10 words and their frequency to a file
with open('data/result_6.tsv', 'w') as outputfile:
    for line in usTweets:
        outputfile.write(line[0] + "\t" + str(line[1]) + "\n")
outputfile.close()
