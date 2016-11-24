from __future__ import print_function

# $example on$
from numpy import array
from math import sqrt
from numpy import append
import numpy as np
# $example off$
import string
from pyspark import SparkContext
# $example on$
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
# $example off$

def add_flags(arr, str):
    lflags = list('AFPRS')
    candidate = list(str)
    for e in lflags:
        arr = append(arr, (float(1) if e in candidate else float(0)))
    return arr

def parseLine(line):

    lflags = list('AFPRS')
    res = array([])
    y = line.split(',')
    z = np.array(y)
    li = list()
    # remove second element
    for i,x in enumerate(y):
        if i == 2:
            t = x
            y.remove(x)
            res= add_flags(np.array(y), x)
    res = res.astype (float)
    for i,x in enumerate(res):
        li.insert(i,x)
    print (li)

    return li

if __name__ == "__main__":
    sc = SparkContext(appName="KMeansExample")  # SparkContext

    brokers = "127.0.0.1:9200"
    topics = "kmeans"
    kvs = KafkaUtils.createDirectStream(ssc, ['ddosInput'], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    print (lines)
    # $example on$
    # Load and parse the data
    data = sc.textFile("/home/harshitha/sjsuproject/prediction/roc")
    #--parsedData =  data.map(parseLine)

    # Build the model (cluster the data)
    #--clusters = KMeans.train(parsedData, 15, maxIterations=10,
                            runs=10, initializationMode="random")

    #--clusters.save(sc, "/home/harshitha/sjsuproject/model/KMeansModelcombination")

    # $example off$
    sc.stop()
