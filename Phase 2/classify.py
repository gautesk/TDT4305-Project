import argparse as ap
from pyspark import SparkContext
from pyspark import SparkConf


def initSpark(inputfile):
    conf = SparkConf().setMaster("local[*]")
    context = SparkContext.getOrCreate(conf)
    rdd = context.textFile(inputfile)
    return rdd

def classifyTweet(training_data, tweet):
    # Implementere Naive Bayes Classifier

    # Returnere rdd-objekt (Sted, Sannsynlighet) med raden med høyest sannsynlighet

# Initialisere med input-parametere
parser = ap.ArgumentParser(description='A program to classify tweet locations')
parser.add_argument('-t', '--training', dest='training_data')
parser.add_argument('-i', '--input', dest='input_tweet')
parser.add_argument('-o', '--output', dest='output_file', default="./data/output1.tsv")
args = parser.parse_args()

training_data = initSpark(args.training_data)
tweet = open(args.input_tweet, 'r').readline()
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
