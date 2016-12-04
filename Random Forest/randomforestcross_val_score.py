import csv
import numpy as np
import pandas as pd
import timeit
from   sklearn import preprocessing
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report,roc_auc_score
from sklearn.externals import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (precision_score, recall_score,f1_score)
from sklearn.metrics import confusion_matrix
from sklearn.cross_validation import cross_val_score
from sklearn.preprocessing import Imputer
import os
import psutil
import argparse

 
start_time = timeit.default_timer()
np.set_printoptions(suppress=True)

parser = argparse.ArgumentParser(description = 'RandomForest Script will be executed')
parser.add_argument('-d', '--dataset',required=True,help='Dataset File',type=argparse.FileType('r'))
args=parser.parse_args()
inputfile = args.dataset


df=pd.read_csv(inputfile)

df.dropna(inplace=True)
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


clf = RandomForestClassifier(n_estimators=50)
value = cross_val_score(clf,standardized_X, y,cv=10,scoring='accuracy')
print value
print value.mean()


runningtime = timeit.default_timer() - start_time

print(runningtime)


















	
	













