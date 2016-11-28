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


def parseLin(line):
    print (line)
    return line

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
    if len(sys.argv) != 8:
        l = len(sys.argv)
        print (l)
        print("Check usage in README")
        sys.exit(1)

    app_name, data_path, save_path, n_clusters, max_iter, runs, init_mode = sys.argv[1:]

    print (app_name)
    print (data_path)
    print (save_path)
    print (n_clusters)
    print (max_iter)
    print (runs)
    print (init_mode)

    sc = SparkContext(appName=app_name)  # SparkContext
    data = sc.textFile(data_path)
    print ("type of data")
    print (type(data))

    parsedData =  data.map(parseLine)
    print ("type parsedData")
    print(type(parsedData))
    # Build the model (cluster the data)
    clusters = KMeans.train(parsedData, int(n_clusters), int(max_iter),
                            int(runs), init_mode)
    clusters.save(sc, save_path )
    centers = clusters.clusterCenters
    print("Cluster Centers: ")
    print(type(centers))
    print (str(centers))
    def error(point):
    	center = clusters.centers[clusters.predict(point)]
    	return sqrt(sum([x**2 for x in (point - center)]))

    WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    print("Within Set Sum of Squared Error = " + str(WSSSE))
    sc.stop()
