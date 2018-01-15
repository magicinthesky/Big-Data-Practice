import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from textblob import TextBlob
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# initialize a SparkContext
sc = SparkContext(appName="SparkStreamingandKafka")
# initialize a StreamingContext and set one second for batch interval
strc = StreamingContext(sc, 60)
consumer = KafkaConsumer('twitteranalysis',bootstrap_servers=['localhost:9092'])
for content in consumer:
    tb_output = TextBlob(content.value.decode("utf-8"))
    for each in tb_output.sentences:
        print each
        if float(each.sentiment.polarity) == 0.0:
            print "- Neutral"
        elif float(each.sentiment.polarity) > 0.0:
            print "- Positive"
        else:
            print "- Negative"
        print "\n"
strc.start()
strc.awaitTermination()
