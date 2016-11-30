from __future__ import print_function

import numpy as np
import matplotlib.pyplot as plt
# $example on$
from numpy import array
from numpy import unique
from math import sqrt
from collections import defaultdict
# $example off$

from pyspark import SparkContext
# $example on$
from pyspark.mllib.clustering import KMeans, KMeansModel
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="KMeansExample")  # SparkContext

    # $example on$
    # Load and parse the data
    data = sc.textFile("/home/harshitha/predict")
    parsedData = data.map(lambda line: array([float(x) for x in line.split(',')]))

    # Save and load mode
    sameModel = KMeansModel.load(sc, "/home/harshitha/model/KMeansModel")
    row = sameModel.predict(parsedData).collect()
    # print (row)
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
    sc.stop()
