# Databricks notebook source
# MAGIC %pip install tweepy

# COMMAND ----------

import tweepy
import json
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run /desafio_serasa/functions/to_datetime

# COMMAND ----------

API_KEY = 'mZb7qzABjYfOXj6OsN6T1ypot'
API_KEY_SECRET = 'qX0Vi9GLmQT6GP8zGXeAapc6Q4Ex1JOXYlzFVjyQkjPu0y1fep'
ACCESS_TOKEN = '1561021637704798208-FpXHG8rw3EqBsIAn9mT0CnvO2TYblA'
ACCESS_TOKEN_SECRET = 'nsoC28WdS1knsdtuXHHZBAwfSWvxInPfv29ZmkY3P55TR'

CONSUMER_KEY = 'M01icUxFUFRrZGoxZnBhSUZ0cFU6MTpjaQ'
CONSUMER_SECRET = 'W5s4iq_iR0xqteh4hzLV3g5dvs8i1ND73aLjA3Yn4F5XPscPf3'

# COMMAND ----------

# função para autenticação do usuário
auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
# função para acesso ao app com os tokens
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
# autenticação na API do Twitter
api = tweepy.API(auth)
public_tweets = api.home_timeline()

# COMMAND ----------

# DBTITLE 1,Entendendo as chaves dos tweets retornados
# Verificando as localizações disponíveis para o trends
query_search = '#bitcoin' + '#criptomoedas' + '#eth' + ' -filter:retweets'
tweets = tweepy.Cursor(api.search_tweets,q=query_search).items(5)
placeKeys = None
for tweet in tweets:
  display(tweet._json)
  
tkeys = tweet._json.keys()  

# COMMAND ----------

# DBTITLE 1,Criando uma estrutura para armazenar os Tweets Retornados
tweets_dict = {}
tweets_dict = tweets_dict.fromkeys(['created_at', 'id', 'id_str', 'text', 'metadata', 'source', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 'is_quote_status', 'retweet_count', 'favorite_count', 'favorited', 'retweeted'])

# COMMAND ----------

# DBTITLE 1,Armazenando os tweets retornados
#query_search = '#bitcoin' + '#criptomoedas' + '#eth' + '#covid19' + ' -filter:retweets'
query_search = '#covid19' + ' -filter:retweets'
cursor_tweets = tweepy.Cursor(api.search_tweets,q=query_search).items(1000)

# COMMAND ----------

for tweet in cursor_tweets:
    for key in tweets_dict.keys():
        try:
            twvalue = tweet._json[key]
            tweets_dict[key].append(twvalue)
        except KeyError:
            twvalue = ""
            if(tweets_dict[key] is None):
                tweets_dict[key] = [twvalue]
            else:
                tweets_dict[key].append(twvalue)
        except:
            tweets_dict[key] = [twvalue]

# COMMAND ----------

# DBTITLE 1,Criando Dataframe com os resultados retornados
dfTweets = pd.DataFrame.from_dict(tweets_dict) 

# COMMAND ----------

schema = StructType([
    StructField('created_at', StringType(), True),
	StructField('id', StringType(), True), 
	StructField('id_str', StringType(), True),
	StructField('text', StringType(), True),
	StructField('metadata', StringType(), True), 
	StructField('source', StringType(), True),
	StructField('in_reply_to_status_id', StringType(), True),
	StructField('in_reply_to_status_id_str',StringType(), True), 
	StructField('in_reply_to_user_id', StringType(), True),
	StructField('in_reply_to_user_id_str', StringType(), True),
	StructField('in_reply_to_screen_name', StringType(), True), 	
  
    StructField('user', StructType([ 
          StructField("id", StringType()), 
          StructField("id_str", StringType()),
          StructField("name", StringType()),
          StructField("screen_name", StringType()),
          StructField("location", StringType()),
          StructField("description", StringType()),
          StructField("url", StringType()),
          StructField("followers_count", IntegerType()),
          StructField("friends_count", IntegerType()),
          StructField("created_at", StringType())
          ])),
  
	StructField('geo', StringType(), True),
	StructField('coordinates', StringType(), True),
	StructField('place', StringType(), True),
	StructField('contributors', StringType(), True),
	StructField('is_quote_status', StringType(), True), 
	StructField('retweet_count', IntegerType(), True),
	StructField('favorite_count', IntegerType(), True),
	StructField('favorited',  StringType(), True),
	StructField('retweeted', StringType(), True)
])

vw_tweets_recentes = sqlContext.createDataFrame(dfTweets,schema).createOrReplaceTempView('vw_tweets_recentes')

# COMMAND ----------

# DBTITLE 1,CRIANDO BANCO DE DADOS DESAFIO_SERASA
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS desafio_serasa;
# MAGIC USE desafio_serasa;

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/bronze/desafio_serasa/tweets_recentes', True)

# COMMAND ----------

# DBTITLE 1,CRIANDO ESTRUTURA BRONZE
# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS desafio_serasa.tweets_recentes;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS desafio_serasa.tweets_recentes
# MAGIC (
# MAGIC   created_at date, 
# MAGIC   id string, 
# MAGIC   id_str string, 
# MAGIC   text string, 
# MAGIC   metadata string, 
# MAGIC   source string, 
# MAGIC   in_reply_to_status_id string, 
# MAGIC   in_reply_to_status_id_str string, 
# MAGIC   in_reply_to_user_id string, 
# MAGIC   in_reply_to_user_id_str string, 
# MAGIC   in_reply_to_screen_name string, 
# MAGIC   u_created_at date, 
# MAGIC   u_id string, 
# MAGIC   u_id_str string, 
# MAGIC   u_name string, 
# MAGIC   u_screen_name string, 
# MAGIC   u_location string, 
# MAGIC   u_description string, 
# MAGIC   u_url string, 
# MAGIC   u_followers_count integer, 
# MAGIC   u_friends_count integer, 
# MAGIC   geo string, 
# MAGIC   coordinates string, 
# MAGIC   place string, 
# MAGIC   contributors string, 
# MAGIC   is_quote_status string, 
# MAGIC   retweet_count integer, 
# MAGIC   favorite_count integer, 
# MAGIC   favorited string, 
# MAGIC   retweeted string
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/bronze/desafio_serasa/tweets_recentes"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC TRUNCATE TABLE desafio_serasa.tweets_recentes;
# MAGIC 
# MAGIC INSERT INTO desafio_serasa.tweets_recentes
# MAGIC SELECT
# MAGIC         to_datetime(created_at) as created_at, 
# MAGIC         id, 
# MAGIC         id_str, 
# MAGIC         text, 
# MAGIC         metadata, 
# MAGIC         source, 
# MAGIC         case when in_reply_to_status_id = 'NaN' or ifnull(in_reply_to_status_id, '') = '' then '' else in_reply_to_status_id end as in_reply_to_status_id, 
# MAGIC         case when in_reply_to_status_id_str = 'NaN' or ifnull(in_reply_to_status_id_str, '') = '' then '' else in_reply_to_status_id_str end as in_reply_to_status_id_str, 
# MAGIC         case when in_reply_to_user_id = 'NaN' or ifnull(in_reply_to_user_id, '') = '' then '' else in_reply_to_user_id end as in_reply_to_user_id, 
# MAGIC         case when in_reply_to_user_id_str = 'NaN' or ifnull(in_reply_to_user_id_str, '') = '' then '' else in_reply_to_user_id_str end as in_reply_to_user_id_str,
# MAGIC         in_reply_to_screen_name,   
# MAGIC         to_datetime(user.created_at) as u_created_at, 
# MAGIC         user.id as u_user_id, 
# MAGIC         user.id_str as u_id_str, 
# MAGIC         user.name as u_name, 
# MAGIC         user.screen_name as u_screen_name, 
# MAGIC         user.location as u_location, 
# MAGIC         user.description as u_description, 
# MAGIC         user.url as u_url, 
# MAGIC         user.followers_count as u_followers_count, 
# MAGIC         user.friends_count as u_friends_count, 
# MAGIC         geo, 
# MAGIC         coordinates, 
# MAGIC         place, 
# MAGIC         contributors, 
# MAGIC         is_quote_status, 
# MAGIC         retweet_count, 
# MAGIC         favorite_count, 
# MAGIC         favorited, 
# MAGIC         retweeted 
# MAGIC from vw_tweets_recentes;

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE desafio_serasa.tweets_recentes;
# MAGIC VACUUM desafio_serasa.tweets_recentes RETAIN 168 HOURS;

# COMMAND ----------

