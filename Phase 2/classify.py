import argparse as ap
from pyspark import SparkContext
from pyspark import SparkConf

#def classifyTweet(training_data, tweet):
    # Implementere Naive Bayes Classifier
    #city_count = training_data.map(lambda rec: (rec[0], 1)).reduceByKey(lambda x,y: x+y)
    #training_data = training_data.reduceByKey(lambda x,y: x+ "\t" + y)

    # Returnere rdd-objekt (Sted, Sannsynlighet) med raden med høyest sannsynlighet

# Initialisere med input-parametere
parser = ap.ArgumentParser(description='A program to classify tweet locations')
parser.add_argument('-t', '--training', dest='training_data')
parser.add_argument('-i', '--input', dest='input_tweet')
parser.add_argument('-o', '--output', dest='output_file', default="./data/output1.tsv")
args = parser.parse_args()

# Initiate Spark
conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)

# Initiate stopwords
stopWords = []
with open('../data/stop_words.txt', 'r') as stopwords:
    for word in stopwords:
        stopWords.append(word.strip())
stopwords.close()

# Initiate rdd-ene våre
training_data = context.textFile(args.training_data)
training_data = training_data.sample(False, 0.005, 5)
training_data = training_data.map(lambda x: x.split("\t")) \
                    .map(lambda x: (x[4], x[10])) \
                    .map(lambda x: (x[0], set(x[1].lower().split(" ")))) \
                    .flatMap(lambda x: ((x[0], y) for y in x[1])) \
                    .filter(lambda x: x[1] not in stopWords and len(x[1]) > 1) \
                    .map(lambda x: ((x[0], x[1]), 1)) \
                    .reduceByKey(lambda x,y: x+y) \
                    .sortBy(lambda x: x[1], False) \
                    .take(10)

print(training_data)

#tweet = context.textFile(args.input_tweet)
#tweet = open(args.input_tweet, 'r').readline()
tweet = "Hallo"

# Snurr film og skriv ut resultatet til fil
tweet_location = classifyTweet(training_data, tweet)
tweet_location.saveAsTextFile(args.output_file)



"""
Oppgave:

Input:
    Stor fil (geotweets.tsv), fil med tweet (én linje) og en spesifisert output-fil

Prosessering av data:
    Bruke Naive Bayes Classifier til å beregne hvor tweeten mest sannsynligvis kommer fra
    Sannsynligheten for at en tweet bestående av ord (w0, w1, ... , wn) er fra by c er gitt ved:
    p(c) = |Tc|/|T| * |Tc,w1|/|Tc| * |Tc,w2|/|Tc| * ... * |Tc,wn|/|Tc|
    Der
        |T| = Antall tweets i treningssett
        |Tc| = Antall tweets fra by c i treningssettet
        |Tc,wi| = Antall tweets fra by c med ord wi


Output:
    Skrive ut stedet med høyest sannsynlighet for at tweeten er fra til fil. F.eks. London<Tab>0.5
    Hvis flere med samme sannsynlighet, skrives London<Tab>New York<Tab>Texas<Tab>0.33
"""
