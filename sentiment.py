import pandas as pd
import numpy as np
import json
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

spark = SparkSession.builder \
    .appName("Spark NLP")\
    .master("local[4]")\
    .config("spark.driver.memory","16G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.3")\
    .config("spark.jars", "/project/postgresql-42.3.2.jar") \
    .getOrCreate()
    
tweets = spark.read.jdbc(url = "jdbc:postgresql://football1.cgjkytjtagwe.eu-west-2.rds.amazonaws.com:5432/football1", 
table = "football.tweets", 
properties = {"user":"postgres", 
"partition": "1",
"password": "qwerty123", 
"driver": "org.postgresql.Driver" })

MODEL_NAME='sentimentdl_use_twitter'

tweet_list = tweets.select("Tweet").rdd.flatMap(lambda x: x).collect()

documentAssembler = DocumentAssembler()\
    .setInputCol("text")\
    .setOutputCol("document")
    
use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
 .setInputCols(["document"])\
 .setOutputCol("sentence_embeddings")


sentimentdl = SentimentDLModel.pretrained(name=MODEL_NAME, lang="en")\
    .setInputCols(["sentence_embeddings"])\
    .setOutputCol("sentiment")

nlpPipeline = Pipeline(
      stages = [
          documentAssembler,
          use,
          sentimentdl
      ])
      
empty_df = spark.createDataFrame([['']]).toDF("text")

pipelineModel = nlpPipeline.fit(empty_df)

df = spark.createDataFrame(pd.DataFrame({"text":tweet_list}))
sentiments = pipelineModel.transform(df)

sentiments.select(F.explode(F.arrays_zip('document.result', 'sentiment.result')).alias("cols")) \
.select(F.expr("cols['0']").alias("Tweet"),
        F.expr("cols['1']").alias("sentiment")).show()
        