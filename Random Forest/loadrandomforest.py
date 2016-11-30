from sklearn.externals import joblib
import numpy as np
import time
import argparse

parser = argparse.ArgumentParser(description = 'RandomForest Script will be executed')
parser.add_argument('-s', '--storedmodel',required=True,help='PKL File',type=argparse.FileType('r')) 
parser.add_argument('-v', '--testvalue',required=True,help='Test Value')

args=parser.parse_args()
model=args.storedmodel
testData=args.testvalue 
randomforest = joblib.load(model)
X_test=np.fromstring(testData,sep=',')
output = randomforest.predict(X_test.reshape(1,-1))
print (output)
