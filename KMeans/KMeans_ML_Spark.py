from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *  
from pyspark.sql import Row

from pyspark.ml.clustering import KMeans
from pyspark.ml.pipeline import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler

conf = SparkConf().setMaster("local").setAppName("attackData")

spark = SparkSession.builder.master("local").appName("attaaak").getOrCreate()

sc = spark.sparkContext
# spark is an existing SparkSession.

# Load a text file and convert each line to a Row.
lines = sc.textFile("/home/harshitha/2.txt")
parts = lines.map(lambda l: l.split(","))
attack = parts.map(lambda p: Row(protocol=int(p[0]), bytes=int(p[1]), packets=int(p[2]), duration=float(p[3]) ))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(attack)
schemaPeople.createOrReplaceTempView("attack")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT bytes,packets,duration FROM attack")
teenagers.show();

# Pipeline
vecAssembler = VectorAssembler(inputCols=["packets", "bytes", "duration"], outputCol="features")
vecAssembler.transform(schemaPeople).head().features

kmeans = KMeans().setK(9).setSeed(1).setFeaturesCol("features").setPredictionCol("prediction")
listPipeline = [vecAssembler,kmeans]
pipeline = Pipeline().setStages(listPipeline)

model = pipeline.fit(schemaPeople)

#Transform
prediction = model.transform(schemaPeople)
selected = prediction.select("features", "prediction")

prediction.show();



wssse = model.computeCost(schemaPeople)
print("Within Set Sum of Squared Errors = " + str(wssse))

    # Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
	print(center)
