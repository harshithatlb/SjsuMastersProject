from sklearn.externals import joblib
import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":

	sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
	ssc = StreamingContext(sc, 2)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, ['ddosInput'], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    print (lines)

	# 'packets','bytes','duration','A','F','P','R','S'
	X_test = np.array([1.0, 48.0, 0.78, 0.0, 0.0, 0.0,0.0,1.0])

	svm_pipeline = joblib.load('svm_pipeline.pkl')

	result = svm_pipeline.predict(X_test)
	print(result)

	# producer = KafkaProducer(bootstrap_servers='localhost:2181')
	# producer.send('ddosInput', b'some_message_bytes')
	ssc.start()
    ssc.awaitTermination()