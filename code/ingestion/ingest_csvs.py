
#############################################################################
# Author : Zach Fernandes                                                   #
# Date   : 4/17/2018                                                        #
# Desc   : Ingests and 'cleans' all of the med student csvs                 #
#          Then pushed them to the 'raw' sqlite database.                   #
#          This file is just for an initial import and runs a               # 
#          bunch of iterative functions that all of the tables need         #
#          individual tables will be normalized and put into a              # 
#          'cleaned' SQL database in separate files                         #
#############################################################################

# Import Packagaes #
import pandas   as pd
import numpy    as np
import sqlite3  as sql
import re
import sys
import os

## Directories where CSVs and SQL files are stored ###
data_dir     = "/Users/Zach/data_science/med_school_data/data/"

 
# get current directory #
wd = os.getcwd()

## Folder within the github repository where my helper functions are stored
mod_dir     = wd + '/../custom_modules/'

# Create file path
db_raw     = data_dir + "medschool_raw.sqlite"
db_cleaned = data_dir + "medschool_cleaned.sqlite"

# Change directories to where my custom mod is stored
sys.path.insert(0, mod_dir)

import cleaning_helpers as helpers


# Create a Dictionary of file path and name strings #
table_dict = {
   "academic_history"      : data_dir + "Academic_History_Anonymized.csv",
   "academic_progress"     : data_dir + "Academic_Progress_Anonymized.csv",
   "grades"                : data_dir + "Grades_Anonymized.csv",
   "honors_highpass"       : data_dir + "Honors_HighPass_Anonymized.csv",
   "leave_of_abs"          : data_dir + "Leave_of_Absence_Anonymized.csv",
   "shelf_exams"           : data_dir + "Shelf_exams_Anonymized.csv",
   "USMLE_scores"          : data_dir + "USMLE_Scores_Anonymized.csv",
   'MCAT_post_2015'        : data_dir + 'M1_Roster_MCAT_After2015.csv',
   'MCAT_pre_2015'         : data_dir + 'M1_Roster_MCAT_Before2015.csv',
   'rosters'               : data_dir + 'Rosters_Anonymized.csv',
   'scale'                 : data_dir + 'Grades_Scale.csv',
   'degrees'               : data_dir + 'Degrees_Anonymized.csv',
   'gems'                  : data_dir + 'GEMS_Anonymized.csv',
   'gender'                : data_dir + 'Gender_Anonymized.csv',
   'race_ethnicity'        : data_dir + 'Race_Ethnicity_Anonymized.csv',
   'missing_students'      : data_dir + 'Missing_step1_Anonymized.csv',
   'repeat' 					     : data_dir + 'repeat_anon.csv'
}


###########################################
#  Execute the functions for each csv     #
# These functions were written by me and  #
# Are stored in "cleaning_helpers.py"     #
###########################################


#---------------------------------------#
# Step 1: Read in all the CSVs at once
#----------------------------------------#

# Read in CSVs #
for name in table_dict.keys():
  globals()[name] = helpers.read_csv(table_dict[name], str)
  print("Created a DF called : [" + name + "]\n")
  
#---------------------------------------#
# Step 2: Format all of column names 
#----------------------------------------#

for name in table_dict.keys():
  df = globals()[name]
  globals()[name] = helpers.rename_columns(df)
  print("Formatted all column names in [" + name + "]")


#-------------------------------------------------#
# Step 3: Convert column to numeric if appropriate
#--------------------------------------------------#
for name in table_dict.keys():
  df = globals()[name]
  globals()[name] = helpers.format_numbers(df)


#-------------------------------------------------#
# Step 4: Drop columns with no unique data
#--------------------------------------------------#
for name in table_dict.keys():
  df = globals()[name]
  print("\n-----------------------------------------")
  print("Looking for usesless vars in [" + name + "]")
  df = globals()[name]
  globals()[name] = helpers.drop_useless_vars(df)


#--------------------------------------------#
# Step 5: Push all of the tables to SQLite
#--------------------------------------------#

## Push all of the dfs to sqlite ##
for file in table_dict.keys():
  helpers.push_to_sql(df = globals()[file], sql_name = file, dbname = db_raw)

