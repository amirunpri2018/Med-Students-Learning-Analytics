#############################################################
# Author : Zach Fernandes                                   #
# Date   : 4/17/2018                                        #
# Desc   : Pulls standardarized test scores from the raw    #
#          SQL database, parses them into smaller tables an #
#          pushes them to a "cleaned" SQL database          #
#############################################################

# Import Packagaes #
import pandas   as pd
import numpy    as np
import sqlite3  as sql
import re
import sys
import os

## Directories SQL files are stored ###
sql_dir     = "/Users/Zach/data_science/med_school_data/sql/"

# get current directory #
wd = os.getcwd()

## Folder within the github repository where my helper functions are stored
mod_dir     = wd + '/../custom_modules/'

# Create file path
db_raw     = sql_dir + "medschool_raw.sqlite"
db_cleaned = sql_dir + "medschool_cleaned.sqlite"

# Change directories to where my custom mod is stored
sys.path.insert(0, mod_dir)

# Import Custom Module
import cleaning_helpers as helpers



#-----------------------------------------------#
# Pull in all of the CSVs from the SQL databse  #
#-----------------------------------------------#

academic_history   = helpers.pull_full_datasets('academic_history', db_raw)                      
grades             = helpers.pull_full_datasets('grades', db_raw)  
honors_highpass    = helpers.pull_full_datasets('honors_highpass', db_raw)  
leave_of_abs       = helpers.pull_full_datasets('leave_of_abs', db_raw)  
shelf_exams        = helpers.pull_full_datasets('shelf_exams', db_raw)   
USMLE_scores       = helpers.pull_full_datasets('USMLE_scores', db_raw)  


# Format Dates
df_list = ['academic_history', 'grades','honors_highpass','leave_of_abs', 'shelf_exams', 'USMLE_scores']
for name in df_list:
  df = globals()[name]
  globals()[name] = helpers.find_format_dates(df)


#--------------------------------------------#
# Create an MCAT Testing Guide
#--------------------------------------------#

# Drop all of the confidence band scores #
MCAT_scores = helpers.drop_if_contain(USMLE_scores, 'test_1', ['CB', 'Conf Band', 'Shelf', 'Step'])

## Find Start dates, end dates, and test names
test_start        = pd.DataFrame(MCAT_scores.groupby('test').min()['date']).reset_index()
test_end          = pd.DataFrame(MCAT_scores.groupby('test').max()['date']).reset_index()
tests             = MCAT_scores.drop_duplicates(subset='test', keep="last")[['test', 'test_1']]
nobs              = MCAT_scores.groupby(['test']).size().reset_index(name='counts')

# Get most frequen test score per by group
mode       = lambda x: x.mode()
score_mode = MCAT_scores.groupby('test')['score'].agg(mode).reset_index()


## Rename the date columns
test_start = test_start.rename(index=str, columns={"date": "first_test"})
test_end   = test_end.rename(index=str, columns={"date": "last_test"})
score_mode   = score_mode.rename(index=str, columns={"score": "most_freq_score"})

## Merge to have one test guide ##
mcat_guide = pd.merge(tests, test_start,        left_on = 'test', right_on = 'test', how = 'inner')
mcat_guide = pd.merge(mcat_guide, test_end,     left_on = 'test', right_on = 'test', how = 'inner')
mcat_guide = pd.merge(mcat_guide, score_mode,   left_on = 'test', right_on = 'test', how = 'inner')
mcat_guide = pd.merge(mcat_guide, nobs,         left_on = 'test', right_on = 'test', how = 'inner')


#--------------------------------------------------------#
# Reshape the MCAT Scores so that each test is a column  #
#--------------------------------------------------------#

# The guide shows that most reliable data comes from the breakdown of the MCAT #
# 1993 - 2015 doesn't seem to have a composite score, but we can just add these up #
# For now I'll reshape the data so that all of the columns correspond to relevant tests #
# Then we can worry about creating crosswalk to put the tests on the same scale

# Dictionary of all the scores we want to keep #
mcat_dict = {
  'MBS'     :  "MCAT_Bio_Sci_old",   
  'MPS'     :  "MCAT_Phy_Sci_old",  
  'MVR'     :  "MCAT_Verbal_old",  
  'MBBF'    :  "MCAT_Biol_Biochem_new",
  'MCAR'    :  "MCAT_Crit_Anal_new",   
  'MCPB'    :  "MCAT_Chem_Phys_Biol_new",
  'MPSB'    :  "MCAT_Psych_Soc_Bio_new",   
  'MTOT'    :  "MCAT_Total_new",
}

# Loop through the dictionary and create dfs #
for key in mcat_dict.keys():
  df = MCAT_scores[MCAT_scores['test'] == key]
  df = df.rename(index=str, columns={"score": mcat_dict[key]})
  df = df[['person_uid_anon', 'date', mcat_dict[key]]]
  globals()[key] = df

# Initialize the reshaped df #
merge_vars = ['person_uid_anon', 'date']
MCAT_reshaped = pd.DataFrame(columns = ['person_uid_anon', 'date'])

# Loop through the datasets and merge them into one #
for key in mcat_dict.keys():
  MCAT_reshaped = pd.merge(MCAT_reshaped, globals()[key], on = merge_vars, how = 'outer')
  MCAT_reshaped[mcat_dict[key]] = pd.to_numeric(MCAT_reshaped[mcat_dict[key]])
  del globals()[key]


#--------------------------------------------------------#
# Do some additional cleaning to the MCAT dataset        #
#--------------------------------------------------------#

# Drop what appears to be garbage data
# student 64587155 has 230 rows
# student 38650964 has 125 rows
MCAT_reshaped = MCAT_reshaped[MCAT_reshaped.person_uid_anon != 64587115]
MCAT_reshaped = MCAT_reshaped[MCAT_reshaped.person_uid_anon != 38650964]


# Make the student be the index
MCAT_reshaped = MCAT_reshaped.set_index('person_uid_anon')

# Create a column with the old score total #
MCAT_reshaped['MCAT_Total_old'] = MCAT_reshaped['MCAT_Bio_Sci_old'] + \
                                  MCAT_reshaped['MCAT_Phy_Sci_old'] + \
                                  MCAT_reshaped['MCAT_Verbal_old']

# Create a column for year #
MCAT_reshaped['year'] = MCAT_reshaped['date'].map(lambda x: x.year)

# It appears that we only have good data from 2005 for MCAT #
# I'm dropping everything pre-2005 #
MCAT_reshaped = MCAT_reshaped[MCAT_reshaped.year >= 2005]


#--------------------------------------------------------#
# Normalize the dataset (So we can compare accros years  #
#--------------------------------------------------------#


# MEAN STATS BY YEAR # 
MCAT_mean = helpers.add_suffix(MCAT_reshaped.groupby('year').mean().reset_index(), "_mean", 'MCAT')
MCAT_min  = helpers.add_suffix(MCAT_reshaped.groupby('year').min().reset_index(), "_min", 'MCAT')
MCAT_max  = helpers.add_suffix(MCAT_reshaped.groupby('year').max().reset_index(), "_max", 'MCAT')

## Merge the data back in ##
MCAT_reshaped = MCAT_reshaped.reset_index()
MCAT_reshaped = pd.merge(MCAT_reshaped, MCAT_mean, on = 'year', how = 'left')
MCAT_reshaped = pd.merge(MCAT_reshaped, MCAT_min, on = 'year', how = 'left')
MCAT_reshaped = pd.merge(MCAT_reshaped, MCAT_max, on = 'year', how = 'left')
MCAT_reshaped = MCAT_reshaped.drop(['date_x', 'date_y'], 1)

# Loop through each variable and create a normalized column #
base_list = list(mcat_dict.values()) + ["MCAT_Total_old"]

for var in base_list:
  # Define the min, max, and mean #
  x    = MCAT_reshaped[str(var)]
  min  = MCAT_reshaped[str(var) + '_min']
  max  = MCAT_reshaped[str(var) + '_max']
  mean = MCAT_reshaped[str(var) + '_mean']
  # Make the calculation
  MCAT_reshaped[str(var) + '_norm'] = (x - min)/(max - min)
  MCAT_normalized = MCAT_reshaped.drop([str(var) + '_min', str(var) + '_max', str(var) + '_mean'], 1)

# Push to cleaned
helpers.push_to_sql(df = MCAT_normalized, sql_name = 'MCAT_normalized', dbname = db_cleaned)



#######################################################
# Specialized operations to clean STEP 1 Data        #
#######################################################

# Filter only step 1 scores
step1 = USMLE_scores[USMLE_scores.test_1 == 'USMLE Step 1']

# break the score into two columns
step1['score_num'] = pd.to_numeric(step1.score.str.slice(0, 3))
step1['pass_fail'] = step1.score.str.slice(4,5)

# Find min, max etc by year
step1['year'] = step1['date'].map(lambda x: x.year)
step1_mean = step1.groupby('year').mean().reset_index()[['year', 'score_num']]
step1_min  = step1.groupby('year').min().reset_index()[['year', 'score_num']]
step1_max  = step1.groupby('year').max().reset_index()[['year', 'score_num']]

#rename columns
step1_mean = step1_mean.rename(index = str, columns = {'score_num' : 'score_num_mean'})
step1_min  = step1_min.rename(index = str, columns = {'score_num' : 'score_num_min'})
step1_max  = step1_max.rename(index = str, columns = {'score_num' : 'score_num_max'})

#Merge them back into the dataset
step1 = pd.merge(step1, step1_mean, on = 'year', how = 'left')
step1 = pd.merge(step1, step1_min, on = 'year', how = 'left')
step1 = pd.merge(step1, step1_max, on = 'year', how = 'left')

# Normalize the data 
x    = step1['score_num']
min  = step1['score_num_min']
max  = step1['score_num_max']
mean = step1['score_num_mean']
step1['normalized_score'] = (x - min) / (max - min)

# Drop repetitive variables
step1_normalized = step1.drop(['score_num_max', 'score_num_min', 'score_num_mean', 'test', 'score'], 1)

# Push to cleaned
helpers.push_to_sql(df = step1_normalized, sql_name = 'step1_normalized', dbname = db_cleaned)





