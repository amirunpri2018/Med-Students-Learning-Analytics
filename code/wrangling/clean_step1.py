##############################################################
# Author : Zach Fernandes                                    #
# Date   : 4/26/2018                                         #
# Desc   : Pulls raw USMLE data from SQL, cleans it, pushes  #
#           step one scores back to excel                    #
##############################################################

# Import Packagaes #
import pandas   as pd
import numpy    as np
import sqlite3  as sql
import re
import os
import sys

# get current directory #
wd = os.getcwd()

## Folder within the github repository where my helper functions are stored
mod_dir     = wd + '/../custom_modules/'

# Change directories to where my custom mod is stored
sys.path.insert(0, mod_dir)

# Import Custom Module
import cleaning_helpers as helpers

## Directories where CSVs and SQL files are stored ###
data_dir     = "/Users/Zach/data_science/med_school_data/data/"

# Create sql file path
db_raw     = data_dir + "medschool_raw.sqlite"
db_cleaned = data_dir + "medschool_cleaned.sqlite"

# change the stupid defaults
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)


########################
# Import MCAT datasets #
########################

step1 = helpers.pull_full_datasets('USMLE_scores' , db_raw)  

# Format Dates
step1 = helpers.find_format_dates(step1)
step1['year'] = step1['date'].map(lambda x: x.year)

# Drop useless_vars
step1 = step1[step1.test_1 == 'USMLE Step 1'].drop(['test_1', 'index', 'test'], 1) 

# Parse the score 
step1['score_num'] = pd.to_numeric(step1.score.str.slice(0, 3))
step1['pass_fail'] = step1.score.str.slice(4,5)
step1.drop(['score'],1, inplace = True)

# Get a zscore
step1 = helpers.yearly_zscore(step1, ['score_num'], keep = False)

# Keep FIRST test score #
step1['pass_indicator'] = np.where(step1['pass_fail'] == 'P', 1, 0)
step1.sort_values(['person_uid_anon', 'date'], inplace = True)
step1['try_number']      = step1.groupby('person_uid_anon').cumcount() + 1
step1['total_attempts']  = step1.groupby(['person_uid_anon'])['try_number'].transform(max)
step1 = step1[step1.try_number == 1]
step1.drop(['try_number', 'pass_fail', 'date', 'year'], 1, inplace = True)
step1.reset_index(inplace = True, drop = True)


## Push the data back to the cleaned SQL #
helpers.push_to_sql(step1, 'step1', db_cleaned)

