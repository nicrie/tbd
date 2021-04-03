#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# Imports #%%
# =============================================================================
#part| #%%
from datetime import datetime

import numpy as np
import pandas as pd
from datetime import timedelta
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import VotingClassifier
from sklearn.ensemble import VotingRegressor
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.ensemble import StackingRegressor
import xgboost as xgb
from tpot import TPOTRegressor
from tensorflow.keras.models import Sequential
from tensorflow.keras import layers
import tensorflow as tf 

def mse(ground_truth, predictions):
    diff = (ground_truth - predictions)**2
    return diff.mean()

df = pd.read_csv("Enriched_Covid_history_data.csv")
df = df.dropna()
print(df)

X1=df[['idx', 'pm25', 'no2']]
X2=df[['idx', 'pm25', 'no2','o3','pm10','co','pm257davg','no27davg','o37davg','co7davg','pm107davg']]

y= df['newhospi']

# Hold-out
X_train, X_test, y_train, y_test = train_test_split(X1, y, test_size=0.33,random_state = 84)
X_train2, X_test2, y_train2, y_test2 = train_test_split(X2, y, test_size=0.33,random_state = 84)
print("Neural Network")
X_trainNN = X_train2.values.reshape(22168, 11, 1)
y_trainNN = y_train2.values
print(X_trainNN.shape)
print(X_trainNN)
print(y_trainNN)
NNmodel = Sequential()
#NNmodel.add(layers.Dense(215, input_shape=(X_trainNN.shape[0], X_trainNN.shape[1])))
NNmodel.add(layers.LSTM(units=22, activation='relu',return_sequences=True, input_shape=X_trainNN.shape[1:]))
NNmodel.add(layers.Dense(11, activation="relu"))
NNmodel.add(layers.Dense(22, activation="relu"))
NNmodel.add(layers.Dense(11, activation="relu"))
NNmodel.add(layers.Dense(1, activation="linear"))

# The compilation
NNmodel.compile(loss='mse', 
              optimizer='adam')

# The fit
NNmodel.fit(X_trainNN, y_trainNN,
         batch_size=16,
         epochs=50, verbose=1)

# The prediction
NNpred = NNmodel.predict(X_test2.values.reshape(X_test2.shape[0],X_test2.shape[1],1)) 
MSE7 = mse(y_test2,NNpred)
print("Average error on new number of hospitalizations per day:", round(MSE7 ** 0.5,0))
print(MSE7)
print('OK')
print(" Scikit Learn RandomForestRegressor without feature engineering")
regr = RandomForestRegressor()
regr.fit(X_train, y_train)
pred = regr.predict(X_test).round(0)
RFRMSE = mse(y_test, pred)
print(RFRMSE)
print("Average error on new number of hospitalizations per day:", round(RFRMSE ** 0.5,0))



print(" Scikit Learn RandomForestRegressor with feature engineering")
regr2 = RandomForestRegressor()
regr2.fit(X_train2, y_train2)
pred2 = regr2.predict(X_test2).round(0)
RFRMSE2 = mse(y_test2, pred2)
print("Average error on new number of hospitalizations per day:", round(RFRMSE2 ** 0.5,0))
print(RFRMSE2)

print(" Scikit Learn ExtratreesRegressor with feature engineering")
ETregr = ExtraTreesRegressor()
ETregr.fit(X_train2, y_train2)
predET = ETregr.predict(X_test2).round(0)
ETMSE = mse(y_test2, pred2)
print("Average error on new number of hospitalizations per day:", round(ETMSE** 0.5,0))
print(ETMSE)

print("GradientBoostingRegressor Model")
model = GradientBoostingRegressor(
    n_estimators=100, 
    learning_rate=0.1
)
model.fit(X_train2,y_train2)
pred4 = model.predict(X_test2).round(0)
MSE4 = mse(y_test2, pred4)
print(MSE4)
print("Average error on new number of hospitalizations per day:", round(MSE4 ** 0.5,0))



print("DecisionTreeRegressor Model")
regr2 = DecisionTreeRegressor()
regr2.fit(X_train2, y_train2)
pred2 = regr2.predict(X_test2).round(0)
RFRMSE2 = mse(y_test2, pred2)
print(RFRMSE2)
print("Average error on new number of hospitalizations per day:", round(RFRMSE2 ** 0.5,0))


print("XGBoost Regressor Model")
xgb_model = xgb.XGBRegressor(n_jobs=1).fit(X_train2, y_train2)
pred3 = xgb_model.predict(X_test2).round(0)
RFRMSE3 = mse(y_test2, pred3)
print("Average error on new number of hospitalizations per day:", round(RFRMSE3 ** 0.5,0))
print(RFRMSE3)

print("VotingRegressor")
ensemble = VotingRegressor(
    estimators = [("rf", regr),("gbr", model),("dtr",regr2),("xgbr",xgb_model)],
   )

ensemble.fit(X_train2, y_train2)
predvot = ensemble.predict(X_test2).round(0)
MSE5 = mse(y_test2,predvot)
print("Average error on new number of hospitalizations per day:", round(MSE5 ** 0.5,0))
print(MSE5)

print("VotingRegressor2")
ensemble2 = VotingRegressor(
    estimators = [("rf", regr),("gbr", model)],
   )

ensemble2.fit(X_train2, y_train2)
predvot2 = ensemble2.predict(X_test2).round(0)
MSE6 = mse(y_test2,predvot2)
print("Average error on new number of hospitalizations per day:", round(MSE6 ** 0.5,0))
print(MSE6)
print('OK')




print("TPOTRegressor")
tpot = TPOTRegressor(generations=20, population_size=50, verbosity=2, random_state=42)
tpot.fit(X_train2, y_train2)
print(tpot.score(X_test2, y_test2))
tpot.export('tpot_covid_pipeline.py')