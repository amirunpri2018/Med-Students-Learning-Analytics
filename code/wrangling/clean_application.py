#######################################################################
# Author : Zach Fernandes                                             #
# Date   : 5/9/2018                                                   #
# Desc   : Pulls in the application data, formats it, and pushes it   #
#            to SQL                                                   #  
#######################################################################

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

############################
# Pull in the degree data  #
############################

## Pull everything from SQL ##
degree_query = '''
              SELECT * FROM degrees
              ;
              '''

## Create degree dictionary for bachelors and masters ##
bach_list = {
    "Bachelor of Science"           :   "Science"   ,
    "Bachelor of Arts"              :   "Arts"      ,
    "Other Bachelors"               :   "Other"     ,
    "Bachelor of Science (Bsc)"     :   "Science"   , 
    "BS Engineering"                :   "Science"   , 
    "B.A. in Liberal Studies"       :   "Other"     , 
    "Bachelor of Engineering"       :   "Science"   ,
    "Bachelor of Medicine"          :   "Medicine"  , 
    "B.S. in Business Admin."       :   "Business"  , 
    "Bachelor of Fine Arts"         :   "Arts"      , 
    "Bachelor of Business Admin"    :   "Business"  , 
    "Bachelor of Economics"         :   "Other"      
}

mast_list = {
    "Master of Science"           :   "Science"    ,      
    "Other Masters"               :   "Other"      , 
    "Master of Arts"              :   "Arts"       ,      
    "Master of Education"         :   "Education"  ,     
    "Master of Public Health"     :   "Other"      ,      
    "Master of Management"        :   "Business"   ,      
    "Master of Health Sciences"   :   "Science"    ,      
    "Master of Arts in Teaching"  :   "Education"  ,      
    "Master of Science (Msc)"     :   "Science"    ,      
    "Master of Business Admin."   :   "Business"         
}

assoc_list = {
    "Associate of Science"           : "Science"  , 
    "Associate of Arts"              : "Arts"     ,  
    "Assoc. of Science in Nursing"   : "Nursing"  ,
    "Other Associate"                : "Other"    
}



## Pull in all of the data ##
degrees = helpers.query_dataset(db_raw,degree_query).drop(['index'], 1,)

## Create subsets #
bachelors  = degrees[degrees['degree_desc'].isin(bach_list.keys())].sort_values('person_uid_anon')
masters    = degrees[degrees['degree_desc'].isin(mast_list.keys())].sort_values('person_uid_anon')
associates = degrees[degrees['degree_desc'].isin(assoc_list.keys())].sort_values('person_uid_anon')

## Assign Degree types #
## We may want to get more specific later #
for degree in bach_list.keys():
  bachelors.loc[bachelors['degree_desc'] == degree, 'type'] = bach_list[degree]

for degree in mast_list.keys():
  masters.loc[masters['degree_desc'] == degree, 'type'] = mast_list[degree]

for degree in assoc_list.keys():
  associates.loc[associates['degree_desc'] == degree, 'type'] = assoc_list[degree]


## pivot so each degree is a column ##

# Bachelors #
bachelors['degree_num'] = bachelors.groupby('person_uid_anon').cumcount() + 1
bachelors_wide = bachelors.pivot_table(index=['person_uid_anon'],
                                     columns='degree_num', 
                                     values='type',
                                     aggfunc='first')

bachelors_wide.reset_index(inplace = True)
bachelors_wide.rename(columns = {1 : 'bachelor_1', 2 : 'bachelor_2', 3 : 'bachelor_3'}, inplace = True)


# Masters #
masters['degree_num'] = masters.groupby('person_uid_anon').cumcount() + 1
masters_wide = masters.pivot_table(index=['person_uid_anon'],
                                     columns='degree_num', 
                                     values='type',
                                     aggfunc='first')

masters_wide.reset_index(inplace = True)
masters_wide.rename(columns = {1 : 'master_1', 2 : 'master_2', 3 : 'master_3'}, inplace = True)


# Associates #
associates['degree_num'] = associates.groupby('person_uid_anon').cumcount() + 1
associates_wide = associates.pivot_table(index=['person_uid_anon'],
                                     columns='degree_num', 
                                     values='type',
                                     aggfunc='first')

associates_wide.reset_index(inplace = True)
associates_wide.rename(columns = {1 : 'associate_1', 2 : 'associate_2'}, inplace = True)


############################
# Pull in the gems data    #
############################

## Pull everything from SQL ##
gem_query = ''' SELECT * from gems 
              ;
              '''

## Pull in all of the data ##
gems = helpers.query_dataset(db_raw,gem_query).drop(['index'], 1,)

## drop duplicates and create and indicator for merging ##
## We don't really care when they were a gem, just that they were one ##
gems['gem_indicator'] = 1
gems = gems[['person_uid_anon', 'gem_indicator']].drop_duplicates().reset_index(drop = True)



############################
# Pull in the degree data  #
############################


## Pull everything from SQL ##
race_query = ''' SELECT * from race_ethnicity
              ;
              '''

## Pull in all of the data ##
race = helpers.query_dataset(db_raw,race_query).drop(['index'], 1,)
race = race[['person_uid_anon', 'primary_ethnicity_desc', 'prim_ethnicity_category']]
race.rename(columns = {'primary_ethnicity_desc' : 'race', 
                       'prim_ethnicity_category': 'race_indic'}, inplace = True)


#########################
# pull in Gender data   #
#########################

gender = helpers.pull_full_datasets('academic_history', db_raw)
gender = gender[['person_uid_anon', 'gender']].drop_duplicates()



###########################################################
# Combine the tables together into one application table  # 
###########################################################

application = pd.merge(bachelors_wide, masters_wide, on = 'person_uid_anon', how = 'outer')
application = pd.merge(application, associates_wide, on = 'person_uid_anon', how = 'outer')
application = pd.merge(application, gems,            on = 'person_uid_anon', how = 'outer')
application = pd.merge(application, race,            on = 'person_uid_anon', how = 'outer')
application = pd.merge(application, gender,          on = 'person_uid_anon', how = 'outer')
application.gem_indicator.fillna(0, inplace = True)






#####################################
# Push this cleaned dataset to SQL  # 
#####################################

helpers.push_to_sql(application, 'application', db_cleaned)

