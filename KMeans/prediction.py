from __future__ import print_function

# $example on$
from numpy import array
from math import sqrt
from numpy import append
import numpy as np
# $example off$
import string
from pyspark import SparkContext
import matplotlib.pyplot as plt

# $example on$
from pyspark.mllib.clustering import KMeans, KMeansModel
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

    # $example on$
    # Load and parse the data
    data = sc.textFile("/home/harshitha/sjsuproject/prediction/attack")
    parsedData =  data.map(parseLine)

    sameModel = KMeansModel.load(sc, "/home/harshitha/model/KMeansModel")
    row = sameModel.predict(parsedData).collect()
    uniqueRows = np.array(row)
    b = np.unique(uniqueRows)
    print (b)
    c = sc.parallelize(uniqueRows)
    result = c.countByValue()
    print (result)
    # $example off$
    plt.bar(range(len(result)), result.values(), align='center')
    plt.xticks(range(len(result)), result.keys())
    plt.show()
    # Build the model (cluster the data)
    # $example off$
    sc.stop()
