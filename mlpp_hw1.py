# MLPP Assignment 1 : Data Collection and ETL

# Objective: Get data from the web (US Census ACS) and load it into a database

## Step 1: Get ACS Data

"""
Chosen state: Pennsylvania

Chosen Attributes: 
    (1) Percent unemployed (justification: unemployment rates can be used for labor market particiation comparison between blocks)
    (2) Percent without high school degree (justificaiton: high school education completion rates are of interest to labor market analyses)
    (3) Percent with 4-year college degree (justification: college education completion rates are of interest to labor market analyses)
    (4) Housing rent prices (justification: rent prices are estimates of portion of income deducted to cover basic living cost)
    (5) Median household income (justification: household incomes are estimates of labor productivity)

Downloaded a great pacakage here and used this example: https://github.com/jtleider/censusdata/blob/master/docs/notebooks/example1.ipynb

# Request an API key at https://api.census.gov/data/key_signup.html
# API key acquired - 66a4f48009858f566fb90eef618aa95722ac5bab

Example API links
https://api.census.gov/data/2019/acs/acs1?get=NAME,B01001_001E,C25121_001E&for=state:42&key=66a4f48009858f566fb90eef618aa95722ac5bab
https://api.census.gov/data/2019/acs/acs1?get=NAME,B01001_001E&for=county%20subdivision:*&in=state:36&in=county:*
"""

# Import packages
import requests
import censusdata
import pandas as pd


# Example of a raw API call getting acs data into json
acs_url = "https://api.census.gov/data/2019/pdb/blockgroup?get=State_name,Tot_Population_ACS_13_17,County_name&for=block%20group:*&in=state:42%20county:001&"
key = "66a4f48009858f566fb90eef618aa95722ac5bab"
response = requests.get("&".join([acs_url,"key="+key])
)

api_data = response.json()

# Figure out how to loop over all counties in Pennsylvania
# Now I will use the awesome package

# Use censusdata.search() to find variables relevant to unemployment
censusdata.search('acs5', 2019, 'label', 'unemploy')
# Found and print relevant table of interest for unemployment = B23025
censusdata.printtable(censusdata.censustable('acs5', 2019, 'B23025'))

# Use censusdata.search() to find variables relevant to education
censusdata.search('acs5', 2019, 'concept', 'education')
# Print table of relevant interest for education = B15003
censusdata.printtable(censusdata.censustable('acs5', 2019, 'B15003'))

# Use censusdata.search() to find variables relevant to housing rental prices
censusdata.search('acs5', 2019, 'label', 'median gross rent')
# Print table of relevant interest for housing rent prices
censusdata.printtable(censusdata.censustable('acs5', 2019, 'B25064'))

# Use censusdata.search() to find variables relevant to median household income
censusdata.search('acs5', 2019, 'label', 'median household income')
# Print table of relevant interest for household income
censusdata.printtable(censusdata.censustable('acs5', 2019, 'B19013'))

# Use censusdata.download() to get our variables

data_variables = censusdata.download('acs5', 2019,
                             censusdata.censusgeo([('state', '42'), ('county', '*'), ('block group', '*')]),
                             [
                             'B23025_003E', # number of people in labor force
                             'B23025_005E', # number of people unemployed
                             'B15003_001E', # number of people who responded to education attainment question
                             'B15003_002E', # number of people who indicated no schooling completed 
                             'B15003_003E', # number of people who indicated nursery school completion
                              'B15003_004E', # number of people who indicated kindergarten completion
                              'B15003_005E', # number of people who indicated 1st grade completion
                              'B15003_006E', # number of people who indicated 2nd grade completion
                              'B15003_007E', # number of people who indicated 3rd grade completion
                              'B15003_008E', # number of people who indicated 4th grade completion
                              'B15003_009E', # number of people who indicated 5th grade completion
                              'B15003_010E', # number of people who indicated 6th grade completion
                              'B15003_011E', # number of people who indicated 7th grade completion
                              'B15003_012E', # number of people who indicated 8th grade completion
                              'B15003_013E', # number of people who indicated 9th grade completion
                              'B15003_014E', # number of people who indicated 10th grade completion
                              'B15003_015E', # number of people who indicated 11th grade completion
                              'B15003_016E', # number of people who indicated 12th grade but no diploma
                              'B15003_017E', # number of people who indicated regular high school diploma
                              'B15003_018E', # number of people who indicated GED completion
                              'B15003_019E', # number of people who indicated less than one year of college
                              'B15003_020E', # number of people who indicated one or more years of college
                              'B15003_021E', # number of people who indicated completing an Associate's degree
                              'B15003_022E', # number of people who indicated completing a Bachelor's degree
                              'B25064_001E', # estimate median gross rent
                              'B19013_001E', # estimate median household income in the last 12 months                   
                              ]
)

# Calculate data and put them in a pandas dataframe
data_variables['percent_unemployed'] = data_variables.B23025_005E / data_variables.B23025_003E * 100
data_variables['percent_without_high_school_degree'] = (data_variables.B15003_002E + data_variables.B15003_003E + data_variables.B15003_004E
                          + data_variables.B15003_005E + data_variables.B15003_006E + data_variables.B15003_007E + data_variables.B15003_008E
                          + data_variables.B15003_009E + data_variables.B15003_010E + data_variables.B15003_011E + data_variables.B15003_012E
                          + data_variables.B15003_013E + data_variables.B15003_014E +
                          data_variables.B15003_015E + data_variables.B15003_016E) / data_variables.B15003_001E * 100
data_variables['percent_with_college_degree'] = data_variables.B15003_022E / data_variables.B15003_001E * 100
data_variables['median_rent'] = data_variables.B25064_001E
data_variables['median_household_income'] = data_variables.B19013_001E

## Step 2: Transform data to load into Postgres database

# Put the data in a pandas dataframe
data_variables = data_variables[['percent_unemployed', 'percent_without_high_school_degree', 'percent_with_college_degree','median_rent', 'median_household_income']]
pd_df = pd.DataFrame(data_variables)

# Download the pandas dataframe into a csv
pd_df.to_csv('census2019_data.csv') # csv is for sanity checking if the data looks okay

# Note (of admission) : the median rent income data looks funky. In a real project I'd investigate why some reported figures are -6666666... 


## Step 3: Load it into Postgres database

"""

Use this information to access the database:

host: acs-db.mlpolicylab.dssg.io
port: 5432
user: mlpp_student
password:
database: acs_data_loading


Create the table in the ACS schema
Name the table {andrew_id}_acs_data

"""

# import packages
import ohio
import ohio.ext.pandas
from sqlalchemy import create_engine

# comment out the line below to replace "pw" with the password, iykyk
with open("mlpp_student_temp_pw.txt","r") as f:
    pw=f.read().strip()

engine = create_engine('postgresql://mlpp_student:'+ pw +'@acs-db.mlpolicylab.dssg.io/acs_data_loading')

pd_df.pg_copy_to(
    name='huichenl_acs_data',
    con=engine,
    schema='acs',
)





