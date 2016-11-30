from __future__ import print_function

# $example on$
from numpy import array
from math import sqrt
from numpy import append
import numpy as np
# $example off$
import sys
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
    #print (line	)
    res = array([])
    # remove second element
    for i,x in enumerate(line.split(',')):
        if i == 0:
            res = add_flags(res, x)
	else:
            res = append(res,float(x))
    #print (res)
    return res

if __name__ == "__main__":
    if len(sys.argv) != 4:
        l = len(sys.argv)
        print (l)
        print("Check usage in README")
        sys.exit(1)

    app_name, data_path, save_path = sys.argv[1:]

    print (app_name)
    print (data_path)
    print (save_path)

    sc = SparkContext(appName=app_name)  # SparkContext
    data = sc.textFile(data_path)
    parsedData =  data.map(parseLine)

    sameModel = KMeansModel.load(sc,save_path)

    row = sameModel.predict(parsedData).collect()
    uniqueRows = np.array(row)
    b = np.unique(uniqueRows)
    c = sc.parallelize(uniqueRows)
    result = c.countByValue()
    print (result)
    # $example off$
    plt.bar(range(len(result)), result.values(), align='center')
    plt.xticks(range(len(result)), result.keys())
    plt.show()
    sc.stop()
