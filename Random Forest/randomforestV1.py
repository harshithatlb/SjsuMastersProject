import csv
import json
import numpy as np
import pandas as pd
import timeit
from   sklearn import preprocessing
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report,roc_auc_score
from sklearn.externals import joblib
from sklearn.metrics import (precision_score, recall_score,f1_score)
from sklearn.metrics import confusion_matrix
import os
import psutil
from sklearn.preprocessing import Imputer
import argparse

parser = argparse.ArgumentParser(description = 'RandomForest Script will be executed')
parser.add_argument('-d', '--dataset',required=True,help='Dataset File',type=argparse.FileType('r'))
parser.add_argument('-m', '--storedmodel',required=True,help='Input Dataset File',type=argparse.FileType('w'))
args=parser.parse_args()
inputfile = args.dataset
savedmodel = args.storedmodel
start_time = timeit.default_timer()
np.set_printoptions(suppress=True)

df=pd.read_csv(inputfile)
print (df.isnull().any())
df.dropna(inplace=True)
print (df.isnull().any())



just_dummies = pd.Series(df['flags'])
values = just_dummies.str.get_dummies(sep='|')




dfMod = pd.concat([df, values], axis=1) 

print dfMod.head()


dfMod['A'] = dfMod['A'].astype(float)
dfMod['F'] = dfMod['F'].astype(float)
dfMod['P'] = dfMod['P'].astype(float)
dfMod['R'] = dfMod['R'].astype(float)
dfMod['S'] = dfMod['S'].astype(float)

print(dfMod.columns.tolist())
X = df.as_matrix(columns=['packets','bytes','duration','A','F','P','R','S'])
X[ ~np.isfinite(X) ] = 0
print "converted into matrix"
print X[0:5,:]
print (np.any(np.isnan(X)))

y = df['type']

scaler = preprocessing.StandardScaler().fit(X)
standardized_X= Imputer().fit_transform(X)
print standardized_X


with open('input.json') as data_file:
	input_dict = json.load(data_file)
print input_dict


X_train, X_test, y_train, y_test = train_test_split(standardized_X, y, test_size=input_dict['test_size'],random_state=input_dict['random_state'])
clf = RandomForestClassifier(n_estimators=input_dict['n_estimators'], criterion=input_dict['criterion'], min_samples_split=input_dict['min_samples_split'], min_samples_leaf=input_dict['min_samples_leaf'], min_impurity_split=input_dict['min_impurity_split'], 
	bootstrap=True, oob_score=True, n_jobs=input_dict['n_jobs'],verbose=input_dict['verbose'])
value= clf.fit(X_train, y_train)
joblib.dump(value, savedmodel)
y_prediction = value.predict(X_test)
print (accuracy_score(y_test, y_prediction))
report = classification_report(y_test, y_prediction)
print (report)
print(confusion_matrix(y_test, y_prediction))

runningtime = timeit.default_timer() - start_time

print(runningtime)


















	
	













