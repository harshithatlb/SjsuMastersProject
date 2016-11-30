#!/usr/bin/env python

import numpy as np
import pandas as pd
from sklearn import svm
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn import preprocessing
from sklearn.cross_validation import train_test_split
from sklearn import metrics
from sklearn.metrics import accuracy_score


df = pd.read_csv('/home/komalydedhia/Spark/SParkPreProcessed/sampleDDOSSplitFlag/part-00000')
print df.head()
# df['S']=pd.DataFrame({'S':[0]})
# df['R']=pd.DataFrame({'R':[0]})
# for index, row in df.iterrows():
# 	#protocol=indx.protocol
# 	if 'S' in row['protocol']:
# 		df['S'] = 1
# 	else:
# 		df['S']=0

# print df.head()
X = df.as_matrix(columns=['packets','bytes','duration'])

np.set_printoptions(suppress=True)
df['packets'] = df['packets'].astype(float)
df['bytes'] = df['bytes'].astype(float)
#print df['packets']
#X = df.as_matrix(columns=df.columns[8:11])
#X = df.iloc[:,8:11].values
#Y = df.as_matrix(columns=['type'])
Y = df['type']
standardized_X = preprocessing.scale(X)
print standardized_X
# print(type(Y))
# X_train, X_test, y_train, y_test = train_test_split(standardized_X, Y, test_size=0.33, random_state=42)
# C = 1.0 
# rbf_svc = svm.SVC(kernel='rbf', gamma=0.7, C=C).fit(X_train, y_train)
# # make predictions
# expected = y_test
# predicted = rbf_svc.predict(X_test)
# # # summarize the fit of the model
# print(metrics.classification_report(expected, predicted))
# print (accuracy_score(expected, predicted))

# print(metrics.confusion_matrix(expected, predicted))
#fig = plt.figure()
#ax = fig.add_subplot(111, projection='3d')
# x=df.as_matrix(columns=['packets'])
# z=df.as_matrix(columns=['bytes'])
# y=df.as_matrix(columns=['duration'])
# x=standardized_X[:,0]
# y=standardized_X[:,1]
# z=standardized_X[:,2]
# ax.scatter(x, y, z, zdir='z', c= 'red')
# plt.savefig("demo.png")

