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

np.set_printoptions(suppress=True)
df = pd.read_csv('/home/komalydedhia/Spark/SParkPreProcessed/sampleDDOSSplitFlag/part-00000')
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

X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.33, random_state=42)

clf = svm.SVC(kernel='rbf', gamma=10, C=10000000.0,)

standardized_X = preprocessing.StandardScaler()

steps = [('feature_scaling', standardized_X),('svm', clf)]

pipeline = sklearn.pipeline.Pipeline(steps)

pipeline.fit( X_train, y_train )

y_prediction = pipeline.predict( X_test )

report = metrics.classification_report( y_test, y_prediction )

print(report)

test=np.array([1., 48., 0., 0., 0.,0.,0.,1.])
# XY=standardized_X.transform(test)
# print (XY)
result = pipeline.predict(test)
print(result)

joblib.dump(pipeline, 'svm_pipeline.pkl')

##############################################################################

svm_pipeline = joblib.load('svm_pipeline.pkl')
X_test = np.array([1.0, 48.0, 0.000, 0.0, 0.0,0.0,0.0,1.0])
result = svm_pipeline.predict(X_test)
print(result)