import os
import sys
import pyspark
import findspark
findspark.init()

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import http.client
import mimetypes
import json
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
    .config("spark.jars", "/project/postgresql-42.3.2.jar") \
    .getOrCreate()
    

players=spark.read.parquet("players_files")
player_info=spark.read.parquet("player_info_files")
stats=spark.read.parquet("stats_files")
player_personal=spark.read.parquet("player_personal_files")
tweet_counts= spark.read.parquet("tweet_counts_files")
tweets = spark.read.parquet("tweets_files")

players.write.jdbc(url = "jdbc:postgresql://football1.cgjkytjtagwe.eu-west-2.rds.amazonaws.com:5432/football1", 
table = "football.players", 
mode = "append", 
properties = {"user":"postgres", 
"partition": "1",
"password": "qwerty123", 
"driver": "org.postgresql.Driver" })

player_info.write.jdbc(url = "jdbc:postgresql://football1.cgjkytjtagwe.eu-west-2.rds.amazonaws.com:5432/football1", 
table = "football.player_info", 
mode = "append", 
properties = {"user":"postgres", 
"partition": "1",
"password": "qwerty123", 
"driver": "org.postgresql.Driver" })

stats.write.jdbc(url = "jdbc:postgresql://football1.cgjkytjtagwe.eu-west-2.rds.amazonaws.com:5432/football1", 
table = "football.stats", 
mode = "append", 
properties = {"user":"postgres", 
"partition": "1",
"password": "qwerty123", 
"driver": "org.postgresql.Driver" })

player_personal.write.jdbc(url = "jdbc:postgresql://football1.cgjkytjtagwe.eu-west-2.rds.amazonaws.com:5432/football1", 
table = "football.player_personal", 
mode = "append", 
properties = {"user":"postgres", 
"partition": "1",
"password": "qwerty123", 
"driver": "org.postgresql.Driver" })

tweet_counts.write.jdbc(url = "jdbc:postgresql://football1.cgjkytjtagwe.eu-west-2.rds.amazonaws.com:5432/football1", 
table = "football.tweet_counts", 
mode = "append", 
properties = {"user":"postgres", 
"partition": "1",
"password": "qwerty123", 
"driver": "org.postgresql.Driver" })

tweets.write.jdbc(url = "jdbc:postgresql://football1.cgjkytjtagwe.eu-west-2.rds.amazonaws.com:5432/football1", 
table = "football.tweets", 
mode = "append", 
properties = {"user":"postgres", 
"partition": "1",
"password": "qwerty123", 
"driver": "org.postgresql.Driver" })
