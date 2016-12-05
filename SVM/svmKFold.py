import numpy as np
import pandas as pd
from sklearn.svm import SVC
from sklearn import preprocessing
from sklearn.cross_validation import train_test_split
from sklearn import metrics
from sklearn.metrics import accuracy_score
import sklearn.pipeline
from sklearn.externals import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.model_selection import GridSearchCV
import argparse

parser = argparse.ArgumentParser(description='Script to build SVM Pipeline')
parser.add_argument('-g1', '--gamma1',required=True,help='gamma parameter',type=float)
parser.add_argument('-g2', '--gamma2',required=True,help='gamma parameter',type=float)
parser.add_argument('-g3', '--gamma3',required=True,help='gamma parameter',type=float)
parser.add_argument('-c1', '--penalty1',required=True,help='penalty parameter',type=float)
parser.add_argument('-c2', '--penalty2',required=True,help='penalty parameter',type=float)
parser.add_argument('-c3', '--penalty3',required=True,help='penalty parameter',type=float)
parser.add_argument('-f', '--dataset',required=True,help='Input Dataset File',type=argparse.FileType('r'))
parser.add_argument('-t', '--testSize',required=True,help='Test Size',type=float)
parser.add_argument('-r', '--randomState',required=True,help='randomState',type=int)
parser.add_argument('-s', '--splits',required=True,help='splits',type=int)

args=parser.parse_args()
pGamma1=args.gamma1 #9
pGamma2=args.gamma2 #3
pGamma3=args.gamma3 #13
pPenalty1=args.penalty1 #-2
pPenalty2=args.penalty2 #10
pPenalty3=args.penalty3 #13
datasetFile=args.dataset #'/home/komalydedhia/Spark/SParkPreProcessed/sampleDDOSSplitFlag/part-00000'
pTestSize=args.testSize #.2
pRandomState=args.randomState #42
pSplits=args.splits #3

np.set_printoptions(suppress=True)
df = pd.read_csv(datasetFile)
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
print X[0:2,:]
print (np.any(np.isnan(X)))

y = df['type']

scaler = StandardScaler()
X_Standard = scaler.fit_transform(X)

################## K-Fold ###########################

C_range = np.logspace(pPenalty1, pPenalty2, pPenalty3)
gamma_range = np.logspace(pGamma1, pGamma2, pGamma3)
param_grid = dict(gamma=gamma_range, C=C_range)
cv = StratifiedShuffleSplit(n_splits=pSplits, test_size=pTestSize, random_state=pRandomState)
grid = GridSearchCV(SVC(), param_grid=param_grid, cv=cv)
grid.fit(X_Standard, y)

print("The best parameters are %s with a score of %0.2f" % (grid.best_params_, grid.best_score_))

