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

np.set_printoptions(suppress=True)
df = pd.read_csv('/home/komalydedhia/Spark/SParkPreProcessed/sampleDDOSSplitFlag/part-00000')
print (df.isnull().any())
df.dropna(inplace=True)
print (df.isnull().any())
#print df.head()
just_dummies = df['flags'].str.get_dummies(sep='|')
#print just_dummies.head()
dfMod = pd.concat([df, just_dummies], axis=1) 
print dfMod.head()
dfMod['A'] = dfMod['A'].astype(float)
dfMod['F'] = dfMod['F'].astype(float)
dfMod['P'] = dfMod['P'].astype(float)
dfMod['R'] = dfMod['R'].astype(float)
dfMod['S'] = dfMod['S'].astype(float)
# df['S']=pd.DataFrame({'S':[0]})
# df['R']=pd.DataFrame({'R':[0]})
# for index, row in df.iterrows():
# 	#protocol=indx.protocol
# 	if 'S' in row['protocol']:
# 		df['S'] = 1
# 	else:
# 		df['S']=0

# print df.head()
X = dfMod.as_matrix(columns=['packets','bytes','duration','A','F','P','R','S'])
print "converted into matrix"
#print X[:,1]
print (np.any(np.isnan(X)))

# #print df['packets']
# #X = df.as_matrix(columns=df.columns[8:11])
# #X = df.iloc[:,8:11].values
# #Y = df.as_matrix(columns=['type'])
Y = df['type']

#standardized_X = preprocessing.scale(X)
scaler = preprocessing.StandardScaler().fit(X)
standardized_X=scaler.transform(X)
print standardized_X
# print(type(Y))
X_train, X_test, y_train, y_test = train_test_split(standardized_X, Y, test_size=0.33, random_state=42)
C = 10000000.0
rbf_svc = svm.SVC(kernel='rbf', gamma=10, C=C).fit(X_train, y_train)
# # # make predictions
expected = y_test
predicted = rbf_svc.predict(X_test)
# # # # summarize the fit of the model
print(metrics.classification_report(expected, predicted))

X_tt = np.array([3.0, 792.0, 1.614, 1.0, 1.0,0.0,0.0,1.0])
XY=scaler.transform(X_tt)
result = rbf_svc.predict(XY)
print(result)

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

