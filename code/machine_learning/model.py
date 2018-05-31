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


################
# Load the data#
################
path = '/Users/Zach/data_science/med_school_data/output/'
file_name = 'master.csv'

def load_data(csv, path):
	df = pd.read_csv(path + csv)
	return df

train = load_data(file_name, path)


#####################################
# Make some initial transformations #
#####################################

## Encode degrees ##
train['double_bachelor']		= np.where(pd.notnull(train['bachelor_2']), 1, 0)
train['master_degree'] 			= np.where(pd.notnull(train['master_1']), 1, 0)
train['double_master'] 			= np.where(pd.notnull(train['master_2']), 1, 0)
train['associate_degree'] 	= np.where(pd.notnull(train['associate_1']), 1, 0)

## Flag Science degrees ##
train['science_undergrad'] = np.where((train['bachelor_1'] == 'Science') | 
		(train['bachelor_2'] == 'Science') |
		(train['bachelor_3'] == 'Science'), 1, 0)

train['science_master'] = np.where((train['master_1'] == 'Science') | 
		(train['master_2'] == 'Science') |
		(train['master_3'] == 'Science'), 1, 0)

## Convert gem_indicator to bool #
train.gem_indicator = train.gem_indicator.apply(int)

## Convert race and gender to int ##
def race_dummies(df):
	cols = df.race.unique().tolist()
	for c in cols:
		df['race_' + c] = np.where(df['race'] == c, 1, 0)
	return(helpers.rename_columns(df))

train = race_dummies(train)
train['gender_indic'] = np.where(train.gender == 'M', 1, 0)


train = train.fillna(df.median())



# Features to keep / target declaration
initial_features = ['mcat_zscore', 'mcat_total_attempts', 'double_bachelor',
		'master_degree', 'double_master', 'associate_degree', 'science_undergrad',
		'science_master', 'race_white', 'race_black_or_african_american',
		'race_asian', 'race_unknown', 'race_hispanic', 'race_two_or_more_races',
		'race_amer__indian_or_alaska_nat_',
		'gem_indicator', 'gender_indic', 'm1f_limbs',
		'm1f_metabolism_nutrition_and_endo',
		'm1f_molecular_and_cell_physiology','m1f_molecular_and_human_genetics',
		'm1s_cardio_pulmonary', 'm1s_ebm_and_population_health',
		'm1s_gastrointestinal','m1s_head_neck_and_special_senses',
		'm1s_medical_neuroscience', 'm1s_patients_populations_and_policy',
		'm1s_physical_diagnosis_i', 'm1s_renal_and_electrolytes',
    'm1s_sexual_dev__and_reproduction']

initial_target = 'target_indicator'

courses = ['m1f_limbs', 'm1f_metabolism_nutrition_and_endo', 'm1f_molecular_and_cell_physiology',
		'm1f_molecular_and_human_genetics','m1s_cardio_pulmonary', 'm1s_ebm_and_population_health',
		'm1s_gastrointestinal', 'm1s_head_neck_and_special_senses', 'm1s_medical_neuroscience',
		'm1s_patients_populations_and_policy', 'm1s_physical_diagnosis_i',
		'm1s_renal_and_electrolytes', 'm1s_sexual_dev__and_reproduction']

for c in courses:
	train[c] = train[c].fillna(train[c].median())
	train[c] = train[c].apply(int)

train['mcat_zscore'] 				 = train.mcat_zscore.fillna(train.mcat_zscore.mean())
train['mcat_total_attempts'] = train.mcat_total_attempts.fillna(0).apply(int)



X = train[initial_features]
Y = train[initial_target]


##############################
# Do some Machine Learning #
##############################
from sklearn.linear_model import LogisticRegression

lr = LogisticRegression(random_state=1)
lr.fit(X, Y)














