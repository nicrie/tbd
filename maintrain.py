#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# Imports #%%
# =============================================================================
#part| #%%
import datetime
import numpy as np
import pandas as pd
from datetime import timedelta
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import VotingClassifier
from sklearn.ensemble import VotingRegressor
import xgboost as xgb
from tpot import TPOTRegressor

def mse(ground_truth, predictions):
    diff = (ground_truth - predictions)**2
    return diff.mean()

df = pd.read_csv("Enriched_Covid_history_data.csv")
df = df.drop(columns = ['Unnamed: 0'])
print (df)
print (df.columns)

X=df[['idx', 'pm25', 'no2']]
y= df['newhospi']

print("RandomForestRegressor Model")
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33)
regr = RandomForestRegressor(max_depth=20)
regr.fit(X_train, y_train)
pred = regr.predict(X_test).round(0)
RFRMSE = mse(y_test, pred)
print("Average error on new number of hospitalizations per day:", round(RFRMSE ** 0.5,0))
print(RFRMSE)

print("GradientBoostingRegressor Model")
model = GradientBoostingRegressor(
    n_estimators=100, 
    learning_rate=0.1
)
model.fit(X_train,y_train)
pred4 = model.predict(X_test).round(0)
MSE4 = mse(y_test, pred4)
print("Average error on new number of hospitalizations per day:", round(MSE4 ** 0.5,0))
print(MSE4)


print("DecisionTreeRegressor Model")
regr2 = DecisionTreeRegressor()
regr2.fit(X_train, y_train)
pred2 = regr2.predict(X_test).round(0)
RFRMSE2 = mse(y_test, pred2)
print("Average error on new number of hospitalizations per day:", round(RFRMSE2 ** 0.5,0))
print(RFRMSE2)

print("XGBoost Regressor Model")
xgb_model = xgb.XGBRegressor(n_jobs=1).fit(X_train, y_train)
pred3 = xgb_model.predict(X_test).round(0)
RFRMSE3 = mse(y_test, pred3)
print("Average error on new number of hospitalizations per day:", round(RFRMSE3 ** 0.5,0))
print(RFRMSE3)

print("VotingRegressor")
ensemble = VotingRegressor(
    estimators = [("rf", regr),("gbr", model),("dtr",regr2),("xgbr",xgb_model)],
   )

ensemble.fit(X_train, y_train)
predvot = ensemble.predict(X_test).round(0)
MSE5 = mse(y_test,predvot)
print("Average error on new number of hospitalizations per day:", round(MSE5 ** 0.5,0))
print(MSE5)

print("VotingRegressor2")
ensemble2 = VotingRegressor(
    estimators = [("rf", regr),("gbr", model)],
   )

ensemble2.fit(X_train, y_train)
predvot2 = ensemble2.predict(X_test).round(0)
MSE6 = mse(y_test,predvot2)
print("Average error on new number of hospitalizations per day:", round(MSE6 ** 0.5,0))
print(MSE6)
print('OK')

print("TPOTRegressor")
tpot = TPOTRegressor(generations=20, population_size=50, verbosity=2, random_state=42)
tpot.fit(X_train, y_train)
print(tpot.score(X_test, y_test))
tpot.export('tpot_covid_pipeline.py')