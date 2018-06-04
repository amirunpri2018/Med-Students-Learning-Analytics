###############################################################################
# Author : Zach Fernandes                                                    	#
# Updated: 6/3/18																											      	#
#																																			   			#
# Desc	 : This program is a combination of the previous wrangling						#
# 				 progrms (clean_step1, clean_mcat, clean_grades, 										#
#					 clean_application data. I combined everything so that we could			#
#					 just run one program to wrangle everything at once 								#
#																																							#
# Sections:  Part 1 - Clean Application Data 																	#
#						 Part 2 - Clean MCAT data																					#
#						 Part 3 - Clean Step 1 data																				#
#					   Part 4 - Clean Grade data																				#
#						 Part 5 - Combine all of the new datasets into one 								#
#						 Part 6 - Make final adjustments and export to a CSV that can			#
#											be used for machine learning.														# 
###############################################################################

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


#============================================================#
#------------------------------------------------------------#
#----------- Part One - Wrangle Application Data ------------#
#------------------------------------------------------------#
#============================================================#

print('\n\n--------------------------------')
print('PART ONE: Cleaning Application Data')
print('--------------------------------')

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

## Create a list of students and all of their bachelor descriptions ##
major_list = bachelors[['person_uid_anon', 'first_major_desc']]
major_list = major_list[major_list.first_major_desc.notnull()]
major_list.drop_duplicates(inplace = True)


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
# Pull in the race data	   #
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


###############################
### Pull in dropout indicator #
###############################
drop_query = '''select person_uid_anony as person_uid_anon from missing_students'''
dropout = helpers.query_dataset(db_raw, drop_query)
dropout.drop_duplicates(inplace = True)
dropout['dropout_indic'] = 1

#############################
### Add repeat indicator	 ##
#############################
repeat = helpers.query_dataset(db_raw, 'select person_uid as person_uid_anon from repeat')
repeat.drop_duplicates(inplace = True)
repeat['repeat_indic'] = 1


#########################
# pull in Gender data   #
#########################

gender_query = 'select distinct gender, person_uid_anonym as person_uid_anon from gender';
gender = helpers.query_dataset(db_raw, gender_query)

###################################
# Pull in biochem likeliness data #
###################################

biochem = helpers.pull_full_datasets('biochem', db_raw).drop('index', 1)
biochem.rename(columns = {'unnamed:_1' : 'biochem_likelyhood',}, index = str, inplace = True)
tmp = pd.merge(major_list, biochem, on = 'first_major_desc', how = 'left')
tmp = tmp.groupby(['person_uid_anon'], sort = False)['biochem_likelyhood'].min()
major_biochem = tmp.reset_index()

###########################################################
# Combine the tables together into one application table  # 
###########################################################

def merge_loop(base, df_list, onvar, jointype):
	output = base
	for df in df_list:
		output = pd.merge(output, df, on = onvar, how = jointype)
	return output	

app_dfs = [masters_wide, associates_wide, gems, race, gender,
	major_biochem, repeat, dropout]
	
application = merge_loop(bachelors_wide, app_dfs, 'person_uid_anon', 'outer')

def fill_nas(df, cols, fill):
	for c in cols:
		df[c].fillna(fill, inplace = True)
	return df

application = fill_nas(application, ['gem_indicator', 'repeat_indic', 'dropout_indic'], 0)


#####################################
# Push this cleaned dataset to SQL  # 
#####################################

helpers.push_to_sql(application, 'application', db_cleaned)


#============================================================#
#------------------------------------------------------------#
#----------- Part Two - Wrangle MCAT Data -------------------#
#------------------------------------------------------------#
#============================================================#

print('\n\n--------------------------------')
print('PART TWO: Cleaning MCAT Data')
print('--------------------------------')

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


#============================================================#
#------------------------------------------------------------#
#----------- Part Three - Wrangle Step 1 Data ---------------#
#------------------------------------------------------------#
#============================================================#


print('\n\n --------------------------------')
print('PART THREE: CLeaning Step 1 Data')
print('--------------------------------')

#########################
# Import USMLE datasets #
#########################
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


#============================================================#
#------------------------------------------------------------#
#----------- Part Four - Wrangle Grade Data -----------------#
#------------------------------------------------------------#
#============================================================#

print('\n\n--------------------------------')
print('PART FOUR: CLeaning Grade Data')
print('--------------------------------')


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


# Keep First entry per student-course #
# The reshape command was barfing because sometimes a student 
# Was said to take the same course in the same season
sort_list = ['person_uid_anon', 'course', 'academic_period']
group_list= ['person_uid_anon', 'course']
grades = grades.sort_values(sort_list, ascending=True).groupby(group_list, as_index=False).first()
grades_wide = grades.pivot(index = 'person_uid_anon', columns = 'course')['num__scale']
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


# Create a list of courses to keep (provided by Marcio) #
courses = [
  "limbs",
  "metabolism_nutrition_and_endo",
  "molecular_and_cell_physiology",
  "molecular_and_human_genetics",
  "cardio_pulmonary",
  "ebm_and_population_health",
  "gastrointestinal",
  "head_neck_and_special_senses",
  "medical_neuroscience",
  "patients_populations_and_policy",
  "physical_diagnosis_i",
  "renal_and_electrolytes",
  "sexual_dev__and_reproduction",
  "clinical_skills_primer",
  "evidence_based_medicine_ii",
  "health_care_ethics",
  "human_sexuality",
  "lab_medicine_pblm_solving_case",
  "microbiology_and_immunology",
  "pathology",
  "pharmacology",
  "physical_diagnosis_ii",
  "psychiatry"
  ]

# Write a function to extend the prefix to each course #
def add_prefixes(master_list, prefix_list):
  new_list = []
  for m in master_list:
    for p in prefix_list:
      new = p + m
      new_list.append(new)
  return(new_list)

# Extend the list to include all prefixes #
courses = add_prefixes(courses, ['m1f_', 'm1s_', 'm2f_', 'm2s_'])


# Drop column if it's not in the course list.
courses.append('fall_m1')
courses.append('person_uid_anon')
for c in grades_wide.columns:
  if (c not in courses):
    grades_wide.drop([c],1,inplace =True)


# Drop column if it's not heavily populated ##
def drop_sparse(df):
	for c in df.columns:
		if df[c].count() < 100 :
			df.drop(c, 1, inplace = True)
	return df

grades_wide = drop_sparse(grades_wide)


# Push the data to SQL #
helpers.push_to_sql(grades_wide, 'grades_wide', db_cleaned)


#============================================================#
#------------------------------------------------------------#
#----------- Part Five - Compile Master Dataset--------------#
#------------------------------------------------------------#
#============================================================#

print('\n\n--------------------------------')
print('PART FIVE Compiling Master Dataset')
print('--------------------------------')

### Write a master query ####
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
									c.biochem_likelyhood,
                  c.gem_indicator,
                  c.race,
                  c.race_indic,
                  c.gender,
								  a.*,
                  d.score_num as step1_raw_score,
                  d.score_num_z as step1_z_score,
                  d.pass_indicator as step1_pass_indicator,
									c.repeat_indic,
									c.dropout_indic,
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

#============================================================#
#------------------------------------------------------------#
#----------- Part Six Final adjustments / output ------------#
#------------------------------------------------------------#
#============================================================#

print('\n\n--------------------------------')
print('PART SIX: Final Adjustments / Export')
print('--------------------------------')

## Create a target indicator ##
master['target_indicator'] = np.where((master.step1_pass_indicator == 0.0) | 
					(master.dropout_indic == 1.0) | 
					(master.repeat_indic == 1.0) , 1, 0)

## Encode degrees ##
master['double_bachelor']			= np.where(pd.notnull(master['bachelor_2']), 1, 0)
master['master_degree'] 			= np.where(pd.notnull(master['master_1']), 1, 0)
master['double_master'] 			= np.where(pd.notnull(master['master_2']), 1, 0)
master['associate_degree'] 		= np.where(pd.notnull(master['associate_1']), 1, 0)

## Flag Science degrees ##
master['science_undergrad'] = np.where((master['bachelor_1'] == 'Science') | 
		(master['bachelor_2'] == 'Science') |
		(master['bachelor_3'] == 'Science'), 1, 0)

master['science_master'] = np.where((master['master_1'] == 'Science') | 
		(master['master_2'] == 'Science') |
		(master['master_3'] == 'Science'), 1, 0)


##################################
### Change target based on this ##
##################################

master.to_csv(data_dir + '/../output/master.csv', index = False)
print('\n\nWrangling Complete!')





