
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.clustering import KMeans, KMeansModel
from kafka import KafkaProducer

from numpy import array
from math import sqrt
from numpy import append
import numpy as np
import sys

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: direct_kafka_wordcount.py <model_path> <broker_list> <topic>", file=sys.stderr)
        exit(-1)
    model, brokers, topicIn, topicOut= sys.argv[1:]

    def predict(line):
        print(" *************************in predict")
        if len(line) == 0:
            print ("retrun none")
            return None
        print(line)
        X_Flow = line.split('|')
        X_predt=np.fromstring(X_Flow[1],sep=',')
        result = sameModel.predict(X_predt)
        #result = ([X_Flow[0]]] if result in clusters else None)
        result = ([X_Flow[0]] if result == 7 else None)
        print(str(result))
        return result

    def sendkafka(messages,topicOut):
        producer = KafkaProducer(bootstrap_servers=[brokers])
	messages = [m for m in messages if m is not None]
	for message in messages:
            print ("sending msg........")
	    print(message[0])
	    try:
	        producer.send(topicOut, ",".join(str(x) for x in message))
	    except:
		pass
	producer.flush()

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sameModel = KMeansModel.load(sc, model)
    print (str(brokers))
    print (str(topicIn))
    kvs = KafkaUtils.createDirectStream(ssc, [topicIn], {"metadata.broker.list": brokers})

    lines = kvs.map(lambda x: x[1])
    lines = lines.filter(lambda x: x != None)
    out=lines.map(predict)
    ddosOut=out.filter(lambda x: x != None)
    sentRDD = out.foreachRDD(lambda rdd: rdd.foreachPartition(lambda records: sendkafka(records,topicOut)))

    ssc.start()
    ssc.awaitTermination()
