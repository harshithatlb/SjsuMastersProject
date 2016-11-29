
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
        print(line)
        X_Flow = line.split('|')
        print(X_Flow[1])
        print("type of X_Flow[1]")
        print(type(X_Flow[1]))
        X_predt=np.fromstring(X_Flow[1],sep=',')
        #X_p = array([float(x) for x in X_Flow[1].split(',')])
        #print(type(X_p))
        result = sameModel.predict(X_predt)
        print("*********result is ")
        print(result)
        return [X_Flow[0], result]

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
    print (str(kvs))
    lines = kvs.map(lambda x: x[1])
    out=lines.map(predict)
    #print (str(lines))
    #print (out)
    #print ("printing lines")
    #print ("t0 Kafka")
    sentRDD = out.foreachRDD(lambda rdd: rdd.foreachPartition(lambda records: sendkafka(records,topicOut)))

    ssc.start()
    ssc.awaitTermination()
