# Import Packagaes #
import pandas   as pd
import numpy    as np
import sqlite3  as sql
import matplotlib.pyplot as plt
import os
import sys

# Set a path allowing python to pull from mod_dir
wd = os.getcwd()
mod_dir     = wd + '/../custom_modules/'
sys.path.insert(0, mod_dir)

## Directories where SQL files are stored ###
data_dir     = "/Users/Zach/data_science/med_school_data/data/"
res_dir			= wd + '/../../results/'

# Import Custom Module
import cleaning_helpers as helpers

# Create file path
db_raw     = data_dir + "medschool_raw.sqlite"
db_cleaned = data_dir + "medschool_cleaned.sqlite"

# Write the Query #
mcat_dict = {
            'MCAT_Phy_Sci_old_norm'  : 'MCAT Physical Sciences',
            'MCAT_Verbal_old_norm'   : 'MCAT Verbal',
            'MCAT_Bio_Sci_old_norm'  : 'MCAT Biological',
            'MCAT_Total_old_norm'    : 'MCAT Total'
            }


# Loop Through Queries
for test in mcat_dict.keys():
  con = sql.connect(db_cleaned)
  cur = con.cursor()
  query = '''
      Select a.''' + test + ''',
          a.person_uid_anon,
          a.year,
          b.score_num, 
          b.normalized_score
      from MCAT_normalized as a
      inner join step1_normalized as b
      on a.person_uid_anon = b.person_uid_anon
      WHERE a.''' + test + ''' is not NULL
    ;
    '''                                             
  df = pd.read_sql(query, con)
  cur.close()
    
    # Make the plot #
  p = plt.figure()
  plt.scatter(df[test], df['normalized_score'])
  plt.title(str(mcat_dict[test]) + " vs. Step 1 Exam (2005 - 2015)")
  plt.ylabel("Normalized Step 1 Score")
  plt.xlabel("Normalized " + str(mcat_dict[test]) + " Score")
  plt.savefig(res_dir + str(test) + '.pdf')


command = ('pdfunite ' + 
res_dir +  'MCAT_Total_old_norm.pdf ' + 
res_dir +  'MCAT_Verbal_old_norm.pdf ' + 
res_dir +  'MCAT_Bio_Sci_old_norm.pdf ' + 
res_dir +  'MCAT_Phy_Sci_old_norm.pdf ' + 
res_dir +  'MCAT_vs_Step1.pdf')

os.system(command)
