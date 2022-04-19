import requests
import json
import pandas as pd

import os
import sys
import pyspark
import findspark
findspark.init()

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import numpy as np
import http.client
import mimetypes
import twarc
import tweepy
from random import sample
import pyarrow.parquet as pq
import pyarrow as pa
from os.path import exists
from datetime import date

from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("PySpark App") \
    .getOrCreate()


url = "https://footballapi.pulselive.com/football/teams/12/compseasons/418/staff?pageSize=30&compSeasons=418&altIds=true&page=0&type=player"

payload={}
headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0',
  'Accept': '*/*',
  'Accept-Language': 'en-GB,en;q=0.5',
  'Accept-Encoding': 'gzip, deflate, br',
  'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
  'Origin': 'https://www.premierleague.com',
  'Connection': 'keep-alive',
  'Referer': 'https://www.premierleague.com/',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'cross-site',
  'If-None-Match': 'W/0f9d899ca712646d64021eaf4b3f08ae3',
  'TE': 'trailers'
}

r = requests.get(url, headers=headers)

playerdata = r.json()

df_manu = pd.json_normalize(playerdata['players'])

df_manu.to_csv('manunited.csv', index=False)

def export_parquet_table(df,file_name):
    df.write.mode('overwrite').parquet(str(file_name)+"_files") #Overwrite the new dataframe.

schema = StructType([
    StructField('playerId', StringType(), True),
    StructField('name', StringType(), True)
])
df_players = df_manu[['playerId','name.display']]
df_players = spark.createDataFrame(df_players,schema)
df_players.show()

export_parquet_table(df_players,'players')

schema = StructType([
    StructField('playerId', StringType(), True),
    StructField('goals', StringType(), True),
    StructField('assists', StringType(), True),
    StructField('tackles', StringType(), True),
    StructField('shots', StringType(), True),
    StructField('keyPasses', StringType(), True)
])
df_stats = df_manu[['playerId','goals', 'assists', 'tackles', 'shots',
       'keyPasses']]
df_stats = spark.createDataFrame(df_stats,schema)
df_stats.show()

export_parquet_table(df_stats,'stats')

schema = StructType([
    StructField('playerId', StringType(), True),
    StructField('height', StringType(), True),
    StructField('weight', StringType(), True),
    StructField('age', StringType(), True),
    StructField('position', StringType(), True),
    StructField('shirt_no', StringType(), True),
    StructField('apps', StringType(), True),
    StructField('date_joined', StringType(), True)
])
df_player_info = df_manu[['playerId','height', 'weight','age', 'info.positionInfo',
                          'info.shirtNum','appearances', 'joinDate.label']]
df_player_info = spark.createDataFrame(df_player_info,schema)
df_player_info.show()

export_parquet_table(df_player_info,'player_info')

schema = StructType([
    StructField('playerId', StringType(), True),
    StructField('birthdate', StringType(), True),
    StructField('birth_country', StringType(), True),
    StructField('birth_place', StringType(), True),
    StructField('national_team', StringType(), True)
])

df_player_personal = df_manu[['playerId','birth.date.label','birth.country.country',
                             'birth.place','nationalTeam.country']]
df_player_personal = spark.createDataFrame(df_player_personal,schema)
df_player_personal.show()

export_parquet_table(df_player_personal,'player_personal')
