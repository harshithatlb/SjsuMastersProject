from sklearn.externals import joblib
import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
from kafka import KafkaProducer



topicOut=""
brokers='localhost:9092'

if __name__ == "__main__":

	def predict(line):
		print("in predict")
		print(line)
		#Default Non DDOS
		result=[0,]
		if line:
			try:
				X_Flow=line.split('|')
				X_predict=np.fromstring(X_Flow[1],sep=',')
				# 'packets','bytes','duration','A','F','P','R','S'
				result = svm_pipeline.predict(X_predict.reshape(1,-1))
				print(result)
			except:
				pass
			
		return [line,result[0]]

	def sendkafka(messages,topicOut):
		
		producer = KafkaProducer(bootstrap_servers=[brokers])
		print ("in sendKafka")
		
		for message in messages:
			print ("sending msg........")
			print(message[0])
			try:
				producer.send(topicOut, ",".join(str(x) for x in message))
			except:
				pass
		
		producer.flush()
	

	print ("started")
	
	

	modelPath, brokers, topicIn, topicOut = sys.argv[1:]

	svm_pipeline = joblib.load(modelPath) # '/home/komalydedhia/295/svm/svm_pipeline.pkl'

	sc = SparkContext(appName="Spark streaming ddos")
	ssc = StreamingContext(sc, 2)

	kvs = KafkaUtils.createDirectStream(ssc, [topicIn], {"metadata.broker.list": brokers})
	lines = kvs.map(lambda x: x[1])
	lines.pprint()
	out=lines.map(predict)
	# Filter data to get only DDOS prediction
	ddosOut=out.filter(lambda x: x[1]==1)
	
	print ("t0 Kafka")
	
	sentRDD = ddosOut.foreachRDD(lambda rdd: rdd.foreachPartition(lambda records: sendkafka(records,topicOut)))

	ssc.start()
	ssc.awaitTermination()