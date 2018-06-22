#############################################################
# Author : Zach Fernandes                                   #
# Date   : 4/20/2018                                        #
# Desc   : Write a class of functions to clean data         #
#############################################################

import pandas   as pd
import sqlite3  as sql
import numpy    as np
import re

#-------------------------------------#
# Function 1 : function to input csv  #
#-------------------------------------#

def read_csv(filepath, data_types):
  print("Reading in Specified File")
  df = pd.read_csv(filepath, dtype = data_types)
  return df

#-----------------------------------------------#
# Function 2 : function to clean a column name  #
#-----------------------------------------------#

def rename_columns(df, strings = [' ', '.', ','], rep = [], low = True):
	if len(rep) == 0:
		rep = ['_', '_', '_']
	if low == True:
		df.columns = map(str.lower, df.columns)
	for idx, string in enumerate(strings):
		df.columns = df.columns.str.replace(string, rep[idx])
	return df


#-----------------------------------------------#
# Function 3 : function to format dates         #
#-----------------------------------------------#

# Source: https://github.com/sanand0/benchmarks/tree/master/date-parse
# Helper function I found to speed up the date parsing

def format_dates(s):
    dates = {date:pd.to_datetime(date) for date in s.unique()}
    return s.map(dates)


#----------------------------------------------------------------#
# Function 4 : funtion to find potential dates to format         #
#----------------------------------------------------------------#

def find_format_dates(df):
  for col in df.columns:
    for entry in df[col]:
      if type(entry) == str:
        if(re.match('[a-zA-Z]', entry)):
          break
        elif(("/" in entry and ":" in entry) or ("-" in entry and ":" in entry)):
          print("\ncolumn [" + col + "] is a date")
          df[col] = format_dates(df[col])
          print("converted it to a python date")
          print("The new date looks like : ")
          print(df[col].head(1))
          break
  return df


#-----------------------------------------------#
# Function 5 : function to format dates numbers #
#-----------------------------------------------#

# Look through every entry of every column of a df #
# Decide if the column contains only numeric entries #

def format_numbers(df):
  for col in df.columns:
    flag = False
    for entry in df[col]:
      if type(entry) == str:
        if entry.isdigit() == False:
          flag = True
          break
    if flag == False and df[col].dtypes == object:
      df[col] = pd.to_numeric(df[col])
      print("\nConverted to numeric: [" + col + "]")
      print("The numeric looks like :")
      print(df[col].head(1))
  return df



#-------------------------------------------------------------#
# Function 6 : Push the newly cleaned data to an SQLite file  #
#-------------------------------------------------------------#

def push_to_sql(df, sql_name, dbname):
  con = sql.connect(dbname, detect_types=sql.PARSE_DECLTYPES|sql.PARSE_COLNAMES)
  cur = con.cursor()
  df.to_sql(sql_name, con, if_exists = "replace")
  cur.close()
  print("\nPushed [" + sql_name + "] to \n" + dbname)



#---------------------------------------------------#
# Function 7 : Find which dataset a column belongs  #
#---------------------------------------------------#

# I use this behind the scenes to just see where 
# certain variables are coming from. It's not used
# in the official execution 

def which_dataset(column, file_dict):
	for idx, key in enumerate(file_dict.keys()):
		if column in globals()[key]:
			print("column found in " + key)
			print(globals()[key][column].head())


#--------------------------------------------------------#
# Function 8 : Drop variables with only one unique obs   #
#--------------------------------------------------------#

def drop_useless_vars(df):
  for col in df.columns:
    if len(df[col].unique()) <= 1:
      print("\nColumn [" + col + "] has only one unique value: " + str(df[col].unique()))
      print("Dropping this variable from the dataset")
      df = df.drop(col, 1)
  return df
  


#--------------------------------------------------------#
# Function 9 : Drop Observartions with string in list    #
#--------------------------------------------------------#

def drop_if_contain(df,column, string_list):
  for string in string_list:
    df = df[df[column].str.contains(string) == False]
  return df


#--------------------------------------------------------#
# Function 10 : Add a suffix to column names              #
#--------------------------------------------------------#

def add_suffix(df, suffix, condition):
  for c in df.columns:
    if condition in c:
      df = df.rename(index = str, columns = {str(c): str(c) + suffix})
  return df


#--------------------------------------------------------#
# Function 11 : Pull a full dataset                      #
#--------------------------------------------------------#

def pull_full_datasets(table, dbname):
  con = sql.connect(dbname)
  cur = con.cursor()
  df = pd.read_sql("select * from " + table + ';', con)
  cur.close()
  print("Sucessfully pulled: [" + table + "]")
  return df


#--------------------------------------------------------#
# Function 12 : Pull a full dataset                      #
#--------------------------------------------------------#
def query_dataset(dbname, query):
  con = sql.connect(dbname)
  cur = con.cursor()
  df = pd.read_sql(query, con)
  cur.close()
  print("\nQuery Successful")
  return df



#--------------------------------------------------------#
# Function 13 : Create a Zscore                          #
#--------------------------------------------------------#

def yearly_zscore(df, columns, keep, year_var = 'year'):
  for c in columns:
    df[c + '_mean'] = df.groupby(year_var)[c].transform('mean')
    df[c + '_std']  = df.groupby(year_var)[c].transform('std')
    df[c + '_z']    = (df[c] - df[c + '_mean'])/(df[c + '_std'])
    if keep == False:
      df = df.drop([c + '_mean', c + '_std'], 1)
  return df


#--------------------------------------------------------#
# Function 14 : drop strings from column                 #
#--------------------------------------------------------#

def replace_strings(df, columns, string, replacement):
  for c in columns:
    df[c].replace(string, replacement, regex = True, inplace = True)
  return df













