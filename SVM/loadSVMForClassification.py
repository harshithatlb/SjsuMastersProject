from sklearn.externals import joblib
import numpy as np
import time
import argparse

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Script to Test SVM Pipeline')
	parser.add_argument('-m', '--model',required=True,help='Model File Path',type=argparse.FileType('r')) #'svm_pipeline.pkl'
	parser.add_argument('-t', '--testData',required=True,help='Test Data')

	args=parser.parse_args()
	model=args.model #svm_pipeline.pkl
	testData=args.testData #"1.0,48.0,0.78,1.0,0.0,0.0,0.0,1.0"
	svm_pipeline = joblib.load(model)	
	# 'packets','bytes','duration','A','F','P','R','S'
	
	#X_test = np.array([1.0, 48.0, 0.78, 1.0, 0.0, 0.0,0.0,1.0])

	start_time = time.time()
	X_test=np.fromstring(testData,sep=',')
	
	result = svm_pipeline.predict(X_test.reshape(1,-1))
	print("--- %s seconds ---" % (time.time() - start_time))
	print(result)
	print(result[0])
