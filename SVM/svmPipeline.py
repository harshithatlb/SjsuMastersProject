#!/usr/bin/env python

import numpy as np
import pandas as pd
from sklearn import svm
from sklearn import preprocessing
from sklearn.cross_validation import train_test_split
from sklearn import metrics
from sklearn.metrics import accuracy_score
import sklearn.pipeline
from sklearn.externals import joblib
import argparse

parser = argparse.ArgumentParser(description='Script to build SVM Pipeline')
parser.add_argument('-g', '--gamma',required=True,help='gamma parameter',type=float)
parser.add_argument('-c', '--penalty',required=True,help='penalty parameter',type=float)
parser.add_argument('-f', '--dataset',required=True,help='Input Dataset File',type=argparse.FileType('r'))
parser.add_argument('-t', '--testSize',required=True,help='Test Size',type=float)
parser.add_argument('-r', '--randomState',required=True,help='randomState',type=int)
parser.add_argument('-m', '--modelPath',required=True,help='modelPath',type=argparse.FileType('w'))

args=parser.parse_args()
pGamma=args.gamma #10
datasetFile=args.dataset
pPenalty=args.penalty #10000000.0
pTestSize=args.testSize #.33
pRandomState=args.randomState #42
modelPath=args.modelPath#svm_pipeline.pkl

np.set_printoptions(suppress=True)
df = pd.read_csv(datasetFile) #/home/komalydedhia/Spark/SParkPreProcessed/sampleDDOSSplitFlag/part-00000
df.dropna(inplace=True)
print (df.isnull().any())

just_dummies = df['flags'].str.get_dummies(sep='|')

dfMod = pd.concat([df, just_dummies], axis=1) 

dfMod['A'] = dfMod['A'].astype(float)
dfMod['F'] = dfMod['F'].astype(float)
dfMod['P'] = dfMod['P'].astype(float)
dfMod['R'] = dfMod['R'].astype(float)
dfMod['S'] = dfMod['S'].astype(float)

print dfMod.head()

X = dfMod.as_matrix(columns=['packets','bytes','duration','A','F','P','R','S'])
print "converted into matrix"
print X[0:5,:]
print (np.any(np.isnan(X)))

Y = df['type']

X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=pTestSize, random_state=pRandomState)

clf = svm.SVC(kernel='rbf', gamma=pGamma, C=pPenalty,)

standardized_X = preprocessing.StandardScaler()

steps = [('feature_scaling', standardized_X),('svm', clf)]

pipeline = sklearn.pipeline.Pipeline(steps)

pipeline.fit( X_train, y_train )

y_prediction = pipeline.predict( X_test )

report = metrics.classification_report( y_test, y_prediction )

print(report)

joblib.dump(pipeline, modelPath)

##############################################################################