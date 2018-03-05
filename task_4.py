import findspark
import datetime, time

findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

#print(int(time.time()%(3600*24)//3600))

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
geotweets = sc.textFile("data/geotweets.tsv")
records = geotweets.map(lambda x: x.split("\t"))
sampleGeo = geotweets.sample(False, 0.0005, 5)
sampleRec = sampleGeo.map(lambda x: x.split("\t"))

#timezones = sampleRec.map(lambda x: (x[1],x[8])).distinct().sortByKey(True,1)
#adjusting time to local
adjustedTweetTime = records.map(lambda rec: (rec[1], ((int(rec[0]))/1000)+int(rec[8]),))
#calculating hour of day
tweetHour = adjustedTweetTime.map(lambda x: (x[0],int((((x[1])%(3600*24))//3600))))
#counting tweets per hour
counted = tweetClock.map(lambda x: (((x[0]),int(x[1])),1)).reduceByKey(lambda x,y: x+y).sortByKey(True,1)
#adjusting structure for the join
reverseCounted = counted.map(lambda x: ((x[0][0],x[1]),x[0][1]))
#finding max count value for each
countryCount = counted.map(lambda x: (x[0][0],x[1])).reduceByKey(max)
#adjusting for join again
countryCountAdjusted= countryCount.map(lambda x: ((x[0],x[1]),0))
#joining to find mathcing count and hour
finalSolution = countryCountLul.join(reverseCounted)
#getting rid of junk "0" added for structure, sorting
unpacking = finalSolution.map(lambda x: (x[0][0],x[1][1],x[0][1])).sortByKey(True,1)
#getting rid of duplicates
test= unpacking.map(lambda x: (x[0],(x[1],x[2]))).reduceByKey(max)
#making output easier to read
final = test.map(lambda x: (x[0],x[1][0],x[1][1]))

final.coalesce(1,True).saveAsTextFile('data/result_4.tsv')
