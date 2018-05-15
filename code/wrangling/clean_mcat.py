##############################################################
# Author : Zach Fernandes                                    #
# Date   : 4/26/2018                                         #
# Desc   : Pulls raw MCAT data from SQL, cleans it, and puts #
#          it into the cleaned sql file                      #
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
MCAT_pre_2015  = helpers.pull_full_datasets('MCAT_pre_2015' , db_raw) 
MCAT_post_2015 = helpers.pull_full_datasets('MCAT_post_2015', db_raw)  

#############################################
# Apply Operations needed by both datasets  #
#############################################

# Format dates
MCAT_pre_2015['date']  = pd.to_datetime(MCAT_pre_2015['date'])
MCAT_post_2015['date'] = pd.to_datetime(MCAT_post_2015['date'])

# Rename field3
MCAT_pre_2015 = MCAT_pre_2015.rename(index = str,   columns ={'field3': 'test_desc'})
MCAT_post_2015 = MCAT_post_2015.rename(index = str, columns ={'field3': 'test_desc'})


###############################
# Clean the Pre-2015 Dataset  #
###############################

#Pivot the table ##
MCAT_pre_2015 = MCAT_pre_2015.pivot_table(index = ['person_uid_anon', 'date'] , columns = 'test', values = 'score')

# MCT appears to be consistent with the sum of the other three columns #
# However, it is so rarely populated, and even when it is, we have the other 3 scores #
# This column will be dropped and we will calculate our own new score #
MCAT_pre_2015 = MCAT_pre_2015.reset_index()
MCAT_pre_2015['MCTOT'] = MCAT_pre_2015['MBS'] + MCAT_pre_2015['MPS'] + MCAT_pre_2015['MVR']
MCAT_pre_2015 = MCAT_pre_2015.drop(['MCT'], axis = 1)


###############################
# Clean the Post-2015 Dataset #
###############################

# Even though this is the post 2015 dataset, it has a bunch of scores from the 2015 test
# I'm going to find those old scores, filter them out, and then append them to the 
# pre-2015 dataset

#Pivot the table ##
MCAT_post_2015 = MCAT_post_2015.pivot_table(index = ['person_uid_anon', 'date'] , columns = 'test', values = 'score')

# Keep only old observations where all 3 subjects are populated
# Drop the confidence band stuff. This is for the new test. 
MCAT_post_old = MCAT_post_2015[(MCAT_post_2015['MBS'] > 0) & (MCAT_post_2015['MPS'] > 0) & MCAT_post_2015['MVR'] > 0]
MCAT_post_old = MCAT_post_old.drop(['MTOL','MTOH'], 1)
MCAT_post_old['MCTOT'] = MCAT_post_old.MBS + MCAT_post_old.MPS + MCAT_post_old.MVR
MCAT_post_old = MCAT_post_old.reset_index()

#######################################################################
# Append the old test data from the new dataset to the pre_2015 table #
#######################################################################
MCAT_pre_2015 = MCAT_pre_2015.append(MCAT_post_old).reset_index(drop = True)


##########################################################
# Clean and normalize this final 'pre2015' set of scores #
##########################################################

# Drop duplicates in case some of the observations we appended were duplicates #
MCAT_pre_2015.drop_duplicates(inplace = True)

# Mark which number test this is for the student. Ungroup and then regroup #
MCAT_pre_2015.sort_values(['person_uid_anon', 'date'], ascending = [True, True], inplace = True)
MCAT_pre_2015['test_num'] = MCAT_pre_2015.groupby(['person_uid_anon']).cumcount()+1

# Create a year column #
MCAT_pre_2015['year'] = MCAT_pre_2015['date'].map(lambda x: x.year)

# Find the Z-Score by year #
MCAT_pre_2015 = helpers.yearly_zscore(MCAT_pre_2015, ['MBS','MPS','MVR','MCTOT'], keep = False)


###########################################################
# Clean and normalize this final 'post2015' set of scores #
###########################################################

# Find the total where we have components
MCAT_post_2015.reset_index(inplace = True)
MCAT_post_2015 = MCAT_post_2015[(MCAT_post_2015['MTOH'] > 0) & (MCAT_post_2015['MTOL'] > 0)]
MCAT_post_2015['MCAVG'] = ((MCAT_post_2015.MTOH + MCAT_post_2015.MTOL)/2)

# Do a little extra cleaning
MCAT_post_2015.drop(['MBS', 'MPS', 'MTOH', 'MTOL', 'MVR'], axis =1, inplace = True)
MCAT_post_2015.drop_duplicates(inplace = True)

# Mark which number test this is for the student. Ungroup and then regroup #
MCAT_post_2015.sort_values(['person_uid_anon', 'date'], ascending = [True, True], inplace = True)
MCAT_post_2015['test_num'] = MCAT_post_2015.groupby(['person_uid_anon']).cumcount()+1

# Create a year column #
MCAT_post_2015['year'] = MCAT_post_2015['date'].map(lambda x: x.year)

# Create a Z-Score #
MCAT_post_2015 = helpers.yearly_zscore(MCAT_post_2015, ['MCAVG'], keep = False)



###############################################
# Stack the datasets and keep the bast scores #
###############################################]

# rename and only keep total score #
old = MCAT_pre_2015[['person_uid_anon', 'MCTOT_z']]
new = MCAT_post_2015[['person_uid_anon', 'MCAVG_z']].rename(columns = {'MCAVG_z': 'MCTOT_z'})

# append #
mcat = old.append(new)
mcat['max_score']     = mcat.groupby(['person_uid_anon'])['MCTOT_z'].transform(max)
mcat['try_number']     = mcat.groupby('person_uid_anon').cumcount() + 1
mcat['total_attempts']  = mcat.groupby(['person_uid_anon'])['try_number'].transform(max)

# keep collapse and keep the max score #
mcat = mcat[['person_uid_anon', 'max_score', 'total_attempts']].drop_duplicates()


###############################
# Push the data to sql        #
###############################


helpers.push_to_sql(mcat, 'mcat', db_cleaned)




