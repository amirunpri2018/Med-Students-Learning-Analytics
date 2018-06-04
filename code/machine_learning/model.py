######################################################################
# Author : Zach Fernandes                                            #
# Date   : 4/29/2018                                                 #
# Desc   : Transforms the data and tests various machine learning	   #
# 				 models																										 #
######################################################################

#import packages
import numpy    as np
import sqlite3  as sql
import pandas as pd
import re
import os
import sys
import seaborn as sns 
import yellowbrick as yb
import matplotlib.pyplot as plt 

#import machine learning packages
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.linear_model import RidgeCV, LassoCV, ElasticNetCV 
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.model_selection import cross_val_score

# change the defaults
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

## Folder within the github repository where my helper functions are stored
wd = os.getcwd()
mod_dir     = wd + '/../custom_modules/'

# Change directories to where my custom mod is stored
sys.path.insert(0, mod_dir)

# Import Custom Module
import cleaning_helpers as helpers


#################
# Load the data #
#################

path = '/Users/Zach/data_science/med_school_data/output/'
file_name = 'master.csv'

def load_data(csv, path):
	df = pd.read_csv(path + csv)
	return df

train = load_data(file_name, path)


#########################
# Make the data usable  #
#########################

## Convert gem_indicator to int #
train.gem_indicator = train.gem_indicator.apply(int)

## Convert race and gender to int ##
def race_dummies(df):
	cols = df.race.unique().tolist()
	for c in cols:
		df['race_' + c] = np.where(df['race'] == c, 1, 0)
	return(helpers.rename_columns(df))

train = race_dummies(train)
train['gender_indic'] = np.where(train.gender == 'M', 1, 0)

## Create a list of courses that we will use in the model ##
## right now only keeping m1 courses
courses = ['m1f_limbs', 'm1f_metabolism_nutrition_and_endo', 'm1f_molecular_and_cell_physiology',
		'm1f_molecular_and_human_genetics','m1s_cardio_pulmonary', 'm1s_ebm_and_population_health',
		'm1s_gastrointestinal', 'm1s_head_neck_and_special_senses', 'm1s_medical_neuroscience',
		'm1s_patients_populations_and_policy', 'm1s_physical_diagnosis_i',
		'm1s_renal_and_electrolytes', 'm1s_sexual_dev__and_reproduction']

## Fill course NA's with mode ##
## I think the mode will work best since these are almost categorical ##
## In the sense that there are no half grades ##
for c in courses:
	train[c] = train[c].fillna(train[c].mode()[0])
	train[c] = train[c].apply(int)

train['mcat_zscore'] 				 = train.mcat_zscore.fillna(train.mcat_zscore.mean())
train['mcat_total_attempts'] = train.mcat_total_attempts.fillna(0).apply(int)

## Filter to less than or equal to 2015 ##
train = train[train.m1_fall <= 201530]


#######################################
# Select the Features we want to use  #
#######################################

# Features to keep / target declaration
initial_features = ['mcat_zscore', 'mcat_total_attempts', 'double_bachelor',
		'master_degree', 'double_master', 'associate_degree', 'science_undergrad',
		'science_master', 'race_white', 'race_black_or_african_american',
		'race_asian', 'race_unknown', 'race_hispanic', 'race_two_or_more_races',
		'race_amer__indian_or_alaska_nat_',
		'gem_indicator', 'gender_indic'] + courses

initial_target = 'target_indicator'


#####################################################
# Do some Machine Learning With Cross Validation		#
#####################################################

##---------------------------------##
### Part One: Logistic Regression ###
##---------------------------------##

from sklearn.linear_model 		import LogisticRegression
from sklearn								  import cross_validation
from sklearn.cross_validation import train_test_split 
from sklearn.metrics import classification_report

X = train[initial_features]
y = train[initial_target]

lr = LogisticRegression(random_state=1)

X_train, X_test, y_train, y_test = cross_validation.train_test_split(X,y,test_size = 0.2)
log_reg = lr.fit(X_train, y_train)

log_reg.score(X_test, y_test)

expected   = y_test
predicted  = log_reg.predict(X_test)

classificationReport = classification_report(expected, predicted, target_names=["Not At Risk","At Risk"])
print(classificationReport)


##---------------------------------##
### Part Two: Random Forest			  ###
##---------------------------------##

from sklearn.ensemble import RandomForestClassifier

X = train[initial_features]
y = train[initial_target]

rf = RandomForestClassifier(n_estimators=50, oob_score=True)

X_train, X_test, y_train, y_test = cross_validation.train_test_split(X,y,test_size = 0.2)
ran_for = rf.fit(X_train, y_train)

ran_for.score(X_test, y_test)

expected   = y_test
predicted  = log_reg.predict(X_test)

classificationReport = classification_report(expected, predicted, target_names=["Not At Risk","At Risk"])
print(classificationReport)


##--------------------------------------##
### Part Three: Support Vector Machine ###
##--------------------------------------##

from sklearn.svm import SVC 

X = train[initial_features]
y = train[initial_target]

kernels  =  ['linear', 'poly', 'rbf']

splits     = cross_validation.train_test_split(X,y, test_size=0.2)
X_train, X_test, y_train, y_test = splits

for kernel in kernels:
	if kernel != 'poly':
		model      = SVC(kernel=kernel)
	else:
		model      = SVC(kernel=kernel, degree=3)
	model.fit(X_train, y_train)
	expected   = y_test
	predicted  = model.predict(X_test)
	print('\n')
	print(classification_report(expected, predicted))
