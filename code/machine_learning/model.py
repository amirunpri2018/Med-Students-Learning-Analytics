######################################################################
# Author : Zach Fernandes                                            #
# Date   : 4/29/2018                                                 #
# Desc   : Transforms the data and tests various machine learning	   #
# 				 models																										 #
######################################################################

#import packages
import numpy    as np
import sqlite3  as sql
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











