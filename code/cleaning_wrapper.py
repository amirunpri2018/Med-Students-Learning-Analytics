##############################################################
# Author : Zach Fernandes                                    #
# Date   : 5/14/2018                                         #
# Desc   : This is a wrapper script that runs all of the		 #
#				 the files that ingest and pull the data  					 #
##############################################################

# Import Packagaes #
import re
import os
import sys

# get current directory #
wd = os.getcwd()

## Folder within the github repository where my helper functions are stored
mod_dir     = wd + '/custom_modules/'

# Change directories to where my custom mod is stored
sys.path.insert(0, mod_dir)

# Import Custom Module
import cleaning_helpers as helper

# ingestion
os.system('python ingestion/ingest_csvs.py')


### cleaning
#os.system('python wrangling/clean_applications.py')
#os.system('python wrangling/clean_grades.py')
#os.system('python wrangling/clean_mcat.py')
#os.system('python wrangling/clean_step1.py')
#os.system('python wrangling/make_master.py')
