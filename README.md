# Med-Students-Learning-Analytics
Cohort 11 Capstone Project for the Certificate of Data Science at Georgetown University School of Continuing Studies.


## README

This is a project started by the Georgetown Medical School with the goal of identifying students who may be at high risk for failing/dropping out of medical school. The project was done as a capstone assignment for the Data Science Certificate Program in the School of Continuing Studies. 


## CONTACT

This project has recieved contributions from 5 team members. 

1. Marcio Rosas (Employee of Georgetown Medical School)
2. Zach Fernandes
3. Kathryn Linehan
4. Shaina Francis
5. Andrew Pennington


## FILES BY DIRECTORY

1. /code/initial_formatting

- format.py - Reads in all of the student data CSVs, performs some initial transformations, pushes the data to SQLite

2. /code/wrangling

- wrangle.py - Performs all major wrangling / merging, pushes the data to one master table in SQLite
prepare_final.ipynb - Makes some final transformations based on visual analysis, outputs data for Machine Learning

3. /code/exploratory_data_analysis

- Exploratory_Data_Analysis.ipynb - Creates some visuals / takes a first look at the data


4. /code/machine_learning

- Feature_Selection.ipynb - Does some feature analysis in order to find the best features for machine learning
- Fail_Sample_ML.ipynb - Takes a looks at a small subset of the data (i.e. only students who have failed a class)
- FinalML.ipynb - The most relevant machine learning file (Our final product). Trains on certain years of data and predicts other years. 
- YellowBrick_ML.ipynb - Tries different iterations of different models. Uses a looped version of train_test_split

5. /code/custom/modules

- cleaning_helpers.py - A file of helper functions used throughout the data cleaning process

6. /code/statistical_analysis

- Statistical_Analysis.ipynb - Does some statistical analysis / bootstrapping


7. /visuals

- Contains some of the pictures that were added to the PowerPoint Presentation on 6/30/18, as well as the presentation itself. 




## DATA

While the data is anonymized, we are operating under an NDA which prohibits us from sharing the full dataset with anyone outside of our team. 
