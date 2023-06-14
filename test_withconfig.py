import mysql.connector
import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
import configparser
import pandas as pd
import numpy as np
import requests
import csv
import sqlalchemy
import pymysql


parser = configparser.ConfigParser()
parser.read('D:/jugaad/FileConfig_jugaad.cfg')
directory_Path = parser.get('file_config','directory_Path')
scope = parser.get('file_config','scope')
api_Creds = parser.get('file_config','api_Creds')
dlyfb_key = parser.get('file_config','dlyfb_key')
dlyhooq_adnet_sheetname_start=parser.get('file_config','dlyhooq_adnet_sheetname_start')
ImportFilePath = parser.get('file_config','ImportFilePath')
ExportFilePath = parser.get('file_config','ExportFilePath')
AdWordImportFilePath = parser.get('file_config','AdWordImportFilePath')
AdWordExportFilePath = parser.get('file_config','AdWordExportFilePath')


CampaignExportFilePath = parser.get('file_config','CampaignExportFilePath')
CountryExportFilePath = parser.get('file_config','CountryExportFilePath')
MonthSumExportFilePath = parser.get('file_config','MonthSumExportFilePath')

ColumnName = parser.get('file_config','ColumnName')


credentials = ServiceAccountCredentials.from_json_keyfile_name(directory_Path + '\\' + api_Creds, scope.split(','))
gc = gspread.authorize(credentials)

# Find a workbook by name and open the first sheet
workbook_adnet = gc.open_by_key(dlyfb_key)
sheet = workbook_adnet.worksheet(dlyhooq_adnet_sheetname_start)

# Extract and print all of the values
list_of_hashes = sheet.get_all_records()
print(list_of_hashes)

#Writing gspread data into csv
with open(ImportFilePath,'w') as f:
    writer=csv.writer(f)
    writer.writerows(sheet.get_all_values())

#Forming a dataframe using pandas
df = pd.read_csv(ImportFilePath,error_bad_lines=False)

#replacing Blanks with '0'
df.clicks.fillna(value='0',inplace=True)

print(df)
#SQL query : Select country ,sum(cost),sum(imp),sum(clicks)
             #from df groupby 1(country)
df.clicks = df.clicks.astype(int)
df_CountryGroup=df.groupby(['country'])["cost", "impressions","clicks"].sum()
print(df_CountryGroup)
df_CountryGroup.to_csv(CountryExportFilePath)


#SQL query : Select Campaign ,sum(cost),sum(imp),sum(clicks)
             #from df groupby 1(Campaign)

df_CampaignGroup=df.groupby(['campaign_name'])["cost", "impressions","clicks"].sum()
print(df_CampaignGroup)
df_CampaignGroup.to_csv(CampaignExportFilePath)

#Sql query : select month, sum(cost)
df.date = pd.to_datetime(df.date)
dg_month = df.groupby(pd.Grouper(key='date', freq='1M'))['cost'].sum() # groupby each 1 month
dg_month.index = dg_month.index.strftime('%B')
print(dg_month)
dg_month.to_csv(MonthSumExportFilePath)

#Replacing country 1 with country 2
df['country']=df['country'].replace('COUNTRY 1','COUNTRY 2')


#exporting dataframe to export.csv
df.to_csv(ExportFilePath)

#Database connection
mydb =  mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="amma123",
    database="testdb"
)

mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS testdb")

#Creating engine to connect to database
engine = sqlalchemy.create_engine('mysql+pymysql://root:amma123@localhost:3306/testdb')

#writing dataframe(facebook) into database->table
df.to_sql(
    name='facebook',
    con=engine,
    index=False,
    if_exists='replace'
)

"""----------------------------1b: Pushing google adword dataframe to testDB-----------------"""
#Writing gspread data into csv
with open(AdWordImportFilePath,'w') as f:
    writer=csv.writer(f)
    writer.writerows(sheet.get_all_values())
    print(f)

#Forming a dataframe using pandas
AdWord_df = pd.read_csv(AdWordImportFilePath,error_bad_lines=False)

#print(df)

#Replacing country 1 with country 4
AdWord_df['country']=AdWord_df['country'].replace('COUNTRY 1','COUNTRY 4')

#Replacing country 2 with country 3
AdWord_df['country']=AdWord_df['country'].replace('COUNTRY 2','COUNTRY 3')

#Replacing campaign 1 with campaign 2
AdWord_df['campaign_name']=AdWord_df['campaign_name'].replace('CAMPAIGN 1','CAMPAIGN 2')

#replacing Blanks with '0'
AdWord_df.clicks.fillna(value='0',inplace=True)

print(AdWord_df)



#Creating engine to connect to database
engine = sqlalchemy.create_engine('mysql+pymysql://root:amma123@localhost:3306/testdb')

#Writing dataframe to adwords table in the database
AdWord_df.to_sql(
    name='adwords',
    con=engine,
    index=False,
    if_exists='replace'
)

AdWord_df.to_csv(AdWordExportFilePath)