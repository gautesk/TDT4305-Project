import argparse as ap
from pyspark import SparkContext, SparkConf
from functools import reduce

# Initialisere med input-parametere
parser = ap.ArgumentParser(description='A program to classify tweet locations')
parser.add_argument('--t', '-training', dest='training_data')
parser.add_argument('--i', '-input', dest='input_tweet')
parser.add_argument('--o', '-output', dest='output_file', default="../data/output1.tsv")
args = parser.parse_args()

# Create Spark config and context
conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)

# Read in stopwords.txt and create list of stopwords
stopWords = []
with open('../data/stop_words.txt', 'r') as stopwords:
    stopWords = [word.strip() for word in stopwords]

# Read input tweet file and make it into a list of words, excluding stopwords, 1-letter words and making everything lower case
with open(args.input_tweet, 'r') as inputfile:
    tweet = [x.lower().strip() for x in inputfile.readline().split() if x not in stopWords and len(x)>1]


# Checks if word with index i is in list of words y. If it is, increment integer at position i in x
# Returns updated list x containing count of how many times tweet words have occured
def count_words(x, y):
    for i in range(length_input_tweet):
        if tweet[i] in y:
            x[i] += 1
    return x

# Calculate probability by Naive Bayes classifier, using the data from one place
# Return probability value
def get_prob(data):
    no_tweets_place = data[1] 
    no_of_tweet_words = data[0]
    probability = (no_tweets_place / total_tweets)
    probability *= reduce(lambda x, y: x*y, no_of_tweet_words) / (no_tweets_place**length_input_tweet)
    return probability

# Load in training data file as RDD. Map to exclude every column but place_name and tweet_text.
training_data = context.textFile(args.training_data)
training_data = training_data.map(lambda x: x.split("\t")) \
                    .map(lambda x: (x[4], x[10].lower().split(" ")))

# Create variables with the number of tweets in the training data and length of input tweet (count of words)
total_tweets = training_data.count()
length_input_tweet = len(tweet)

# Aggregate data on the form (Place, ([List], count of words in city))
# List contains how many times a word in the tweet is mentioned in tweets from the place. Index in the list containing the words in the input tweet maps to the indices in the List.
# Filter with all() to remove all rows where the List contains a zero element (since that will make probability = 0)
# Map every row to contain (Place, Probability)
probability = training_data.aggregateByKey(
                    ([0]*length_input_tweet, 0),
                    lambda x, y: (count_words(x[0], y), x[1] +1),
                    lambda rdd1, rdd2: (
                        [rdd1[0][i] + rdd2[0][i] for i in range(len(rdd1[0]))],
                        rdd1[1] + rdd2[1]))\
                    .filter(lambda x: all(x[1][0])) \
                    .map(lambda x: (x[0], get_prob(x[1]))) \
                    .cache()

# Check if probability contains any element (if any place contains the words in input tweet).
# Find the max probability value and filter out any place without the max value
# Write out places with max value and the value to file
with open(args.output_file, 'w') as outputfile:
    if probability.count() > 0:
        max_probability = probability.map(lambda x: x[1]).max()
        highest_probability_location = probability.filter(lambda x: x[1] == max_probability).collect()
        for place in highest_probability_location:
            outputfile.write(place[0] + "\t")
        outputfile.write(str(place[1]))