import http.client
import tweepy
import json
import pandas as pd

import os
import sys
import pyspark
import findspark
findspark.init()
import re

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import http.client
import mimetypes
import twarc
from random import sample
import pyarrow.parquet as pq
import pyarrow as pa

from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("PySpark App") \
    .getOrCreate()

client = tweepy.Client(bearer_token=config.Bearer_Token)

df_players=spark.read.parquet("players_files")

players = df_players.select("name").rdd.flatMap(lambda x: x).collect()
player_id = df_players.select("playerId").rdd.flatMap(lambda x: x).collect()

player_dict = dict(zip(players,player_id))

def export_parquet_table(df,file_name):
    df.write.mode('overwrite').parquet(str(file_name)+"_files") #Overwrite the new dataframe.

tweets_dict = {}

for player in players[:2]:
    query = str(player) + " -is:retweet"
    tweets = client.search_recent_tweets(query=query)
    
    tweets_id = []
    tweets_text = []
    
    for tweet in tweets.data:
        tweets_id.append(tweet.id)
        tweets_text.append(tweet.text)
    
    tweets_dict[player] = list(zip(tweets_id, tweets_text))

schema = StructType([
    StructField('playerId', StringType(), True),
    StructField('Tweet', StringType(), True),
    StructField('Tweet_id', StringType(), True)
])

x = tweets_dict.items()
df_tweets = pd.DataFrame(x,columns=['Player','Tweet'])
df_tweets = df_tweets.explode('Tweet')
df_tweets[['Tweet_id','Tweet']] = pd.DataFrame(df_tweets["Tweet"].tolist(), index= df_tweets.index)
df_tweets['playerId'] = df_tweets['Player'].map(player_dict)
df_tweets.drop(columns=['Player'],inplace=True)
first_column = df_tweets.pop('playerId')
df_tweets.insert(0, 'playerId', first_column)
df_tweets = spark.createDataFrame(df_tweets,schema)
df_tweets.show()

export_parquet_table(df_tweets,'tweets')