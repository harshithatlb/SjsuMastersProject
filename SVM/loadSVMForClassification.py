from sklearn.externals import joblib
import numpy as np
import time

if __name__ == "__main__":

	

	# 'packets','bytes','duration','A','F','P','R','S'
	X_test = np.array([1.0, 48.0, 0.78, 0.0, 0.0, 0.0,0.0,1.0])

	start_time = time.time()
	svm_pipeline = joblib.load('svm_pipeline.pkl')
	result = svm_pipeline.predict(X_test)
	print("--- %s seconds ---" % (time.time() - start_time))
	print(result)
