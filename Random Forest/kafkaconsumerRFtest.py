from sklearn.externals import joblib
import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from numpy import array
from math import sqrt
from numpy import append
import sys


topicOut = ""
brokers = 'localhost:9092'

if __name__ == "__main__":

    modelPath, brokers, topicIn, topicOut = sys.argv[1:]

    def predict(line):
        print("in predict")
        print(line)
        X_Flow = line.split('|')
        print(type(X_Flow[1]))
        X_predict = np.fromstring(X_Flow[1], sep=',')
        # 'packets','bytes','duration','A','F','P','R','S'
        result = randomforest.predict(X_predict.reshape(1, -1))
        print(result)
        return [line, result]

    def sendkafka(messages, topicOut):

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

    randomforest = joblib.load(modelPath)

    sc = SparkContext(appName="Spark streaming ddos")
    ssc = StreamingContext(sc, 2)

    kvs = KafkaUtils.createDirectStream(
        ssc, [topicIn], {"metadata.broker.list": brokers})
    print (str(kvs))
    lines = kvs.map(lambda x: x[1])
    out = lines.map(predict)
    sentRDD = out.foreachRDD(lambda rdd: rdd.foreachPartition(lambda records: sendkafka(records, topicOut)))
    ssc.start()
    ssc.awaitTermination()
