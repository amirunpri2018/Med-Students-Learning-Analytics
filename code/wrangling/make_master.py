#########################################################################
# Author : Zach Fernandes                                               #
# Date   : 4/29/2018                                                    #
# Desc   : Pulls concatenates all of the cleaned SQL data in to one     #
#          spreadsheet where each student is an instance and all of the #
#          features are columns                                         #
#########################################################################
 
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


## Test ## 
grades        = helpers.pull_full_datasets('grades_wide', db_cleaned)
mcat          = helpers.pull_full_datasets('mcat', db_cleaned)
application   = helpers.pull_full_datasets('application', db_cleaned)
step1         = helpers.pull_full_datasets('step1', db_cleaned)


query = '''
                SELECT 
									a.person_uid_anon as student_id,
									a.fall_m1         as m1_fall,
                  b.max_score      as mcat_zscore,
                  b.total_attempts as mcat_total_attempts,
                  c.bachelor_1,
                  c.bachelor_2,
                  c.bachelor_3,
                  c.master_1,
                  c.master_2,
                  c.master_3,
                  c.associate_1,
                  c.associate_2,
                  c.gem_indicator,
                  c.race,
                  c.race_indic,
                  c.gender,
								  a.*,
                  d.score_num as step1_raw_score,
                  d.score_num as step1_z_score,
                  d.pass_indicator as step1_pass_indicator,
                  d.total_attempts as step1_total_attempts
               FROM grades_wide as a
               LEFT JOIN mcat as b
                  ON a.person_uid_anon = b.person_uid_anon
               LEFT JOIN application as c
                  ON a.person_uid_anon = c.person_uid_anon
               LEFT JOIN step1 as d
                  ON a.person_uid_anon = d.person_uid_anon
                ;'''


master = helpers.query_dataset(db_cleaned,query).drop(['index', 'person_uid_anon', 'fall_m1'], 1)

master.to_csv(data_dir + '/../output/master.csv', index = False)


