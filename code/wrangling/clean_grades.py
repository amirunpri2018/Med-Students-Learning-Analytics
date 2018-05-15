
######################################################################
# Author : Zach Fernandes                                            #
# Date   : 4/29/2018                                                 #
# Desc   : Pulls in the student roster and reshapes it the way       #
#           we want it. The roster is then merged with the grades    #
#           data and reshaped such that every student is a row and   #
#           every course is a column                                 #
######################################################################

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

######################################################################
# Pull in M1 students from academic history and create a column for  #
# their spring and fall semesters                                    #
######################################################################

roster_query = '''
              SELECT
                  a.person_uid_anonymized as person_uid_anon,
                  a.academic_period as fall_m1,
                  b.academic_period as spring_m1,
                  c.academic_period as fall_m2,
                  d.academic_period as spring_m2
              
              FROM (
                  SELECT * from rosters 
                    WHERE academic_period % 100 == 30
                    AND   academic_period between 200900 and 201829
                    AND   student_classification == 'M1'
                  ) as a
              
              LEFT JOIN (
                  SELECT * from rosters
                    WHERE academic_period % 100 == 10
                    AND   academic_period between 200900 and 201829
                    AND   student_classification == 'M1'
                  ) as b
              ON  a.person_uid_anonymized = b.person_uid_anonymized
              AND b.academic_period = a.academic_period + 80

              LEFT JOIN (
                  SELECT * from rosters
                    WHERE academic_period % 100 == 30
                    AND   academic_period between 200900 and 201829
                    AND   student_classification == 'M2'
                  ) as c
              ON  a.person_uid_anonymized = c.person_uid_anonymized


             LEFT JOIN (
                  SELECT * from rosters
                    WHERE academic_period % 100 == 10
                    AND   academic_period between 200900 and 201829
                    AND   student_classification == 'M2'
                  ) as d
              ON  a.person_uid_anonymized = d.person_uid_anonymized

              ;'''


## All unique student IDs w/ data after 2009 ##
roster = helpers.query_dataset(db_raw,roster_query)

### If a student retook a class during m1, keep the first observation (marcio's instructions)
sort_list = ['fall_m1', 'spring_m1', 'fall_m2', 'spring_m2'] 
roster = roster.sort_values(sort_list , ascending=True).groupby(['person_uid_anon'], as_index=False).first()

# If the values are missing, force them to be what we think they should be #
# based on the previous semester #
roster.loc[roster['spring_m1'].isnull(),'spring_m1'] = roster.fall_m1 + 80
roster.loc[roster['fall_m2'].isnull(),'fall_m2'] = roster.spring_m1 + 20
roster.loc[roster['spring_m2'].isnull(),'spring_m2'] = roster.fall_m2 + 80

## reshape for merge ##
roster_long= pd.melt(roster, id_vars=['person_uid_anon'], value_vars=['fall_m1', 'spring_m1', 
                  'fall_m2', 'spring_m2'], value_name = 'academic_period' ,var_name = 'season')


#####################################################
# pull in all of the necessary grades data          #
#####################################################

grades_query = '''
                SELECT *
                FROM grades
                WHERE academic_period > 200900
                AND academic_period < 201900
                AND academic_period % 100 != 20
                ;'''

## All unique student IDs w/ data after 2009 ##
grades_raw = helpers.query_dataset(db_raw, grades_query).reset_index(drop = True)



######################################
# pull in the grade conversion data  #
#######################################

scale_query = '''
                SELECT *
                FROM scale 
                ;'''

scale = helpers.query_dataset(db_raw, scale_query).reset_index(drop = True)
scale.drop(['explanation','years','index'], 1, inplace = True)



###############################################
# Merge to get Fall Grades for M1 students    #
###############################################

# Merge the grades and scale data #
grade_vars = ['person_uid_anon', 'academic_period']
grades  = pd.merge(roster_long, grades_raw, on = grade_vars, how = 'left')
grades  = pd.merge(grades, scale, on = 'grade', how = 'left')


# Add prefixes to all the course names #
grades.loc[grades['season'] == 'fall_m1','course']   = 'M1F_' + grades.course
grades.loc[grades['season'] == 'fall_m2','course']   = 'M2F_' + grades.course
grades.loc[grades['season'] == 'spring_m1','course'] = 'M1S_' + grades.course
grades.loc[grades['season'] == 'spring_m2','course'] = 'M2S_' + grades.course


# Reshape the data from long to wide #
grades_wide = grades.pivot_table(index=['person_uid_anon'],
                                     columns='course', 
                                     values='num__scale',
                                     aggfunc='first')

grades_wide.reset_index(inplace = True)


# Merge the fall m1 semester back in so that we know when they started
m1_fall     = roster[['person_uid_anon', 'fall_m1']]
grades_wide = pd.merge(grades_wide, m1_fall, on = 'person_uid_anon', how = 'left')

#rearrange the columns 3
cols = grades_wide.columns.tolist()
cols = cols[-1:] + cols[:-1]
grades_wide = grades_wide[cols]
grades_wide.sort_values(by = 'fall_m1', ascending = True, inplace = True)
grades_wide = helpers.rename_columns(grades_wide, strings = [' ', '.', '/', '&', ',', '-'],
                                                  rep = ['_', '_', '_', 'and', '', '_'])


# Push the data to SQL #
helpers.push_to_sql(grades_wide, 'grades_wide', db_cleaned)

















