# Databricks notebook source
# MAGIC %md 
# MAGIC ##PREPARA ESTRUTURA SILVER

# COMMAND ----------

from datetime import datetime
from dateutil.parser import parse
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##SOURCE

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS desafio_serasa.source;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS desafio_serasa.source
# MAGIC (
# MAGIC   source string, 
# MAGIC   url string
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/silver/desafio_serasa/source"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view vw_source
# MAGIC as
# MAGIC select distinct replace(substring(source, charindex('>', source)+1, charindex('</a>', source)),'</a>','') as source,
# MAGIC                 substring(source, charindex('"', source)+1, charindex('rel', source)-12) as url
# MAGIC   from desafio_serasa.tweets_recentes

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO desafio_serasa.source AS fd
# MAGIC USING vw_source nv 
# MAGIC     on (fd.source = nv.source)
# MAGIC   WHEN MATCHED THEN 
# MAGIC       UPDATE SET *	
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC      INSERT *

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE desafio_serasa.source;
# MAGIC VACUUM desafio_serasa.source RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC   from desafio_serasa.source

# COMMAND ----------

# MAGIC %md
# MAGIC ##PERÍODO

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS desafio_serasa.periodo;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS desafio_serasa.periodo
# MAGIC (
# MAGIC   id_periodo string, 
# MAGIC   date date,
# MAGIC   year integer,
# MAGIC   month integer,
# MAGIC   day integer
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/silver/desafio_serasa/periodo"

# COMMAND ----------

# DBTITLE 0,PERÍODO
# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view vw_periodo
# MAGIC as
# MAGIC select distinct 
# MAGIC         date_format(u_created_at, 'yyyyMMdd') as id_periodo, 
# MAGIC         u_created_at as date, 
# MAGIC         year(u_created_at) as year, 
# MAGIC         month(u_created_at) as month, 
# MAGIC         day(u_created_at) as day
# MAGIC   from desafio_serasa.tweets_recentes

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO desafio_serasa.periodo AS pr
# MAGIC USING vw_periodo vw 
# MAGIC     on (pr.id_periodo = vw.id_periodo)
# MAGIC   WHEN MATCHED THEN 
# MAGIC       UPDATE SET *	
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC      INSERT *

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE desafio_serasa.periodo;
# MAGIC VACUUM desafio_serasa.periodo RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC   from desafio_serasa.periodo

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCREENS

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS desafio_serasa.screens;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS desafio_serasa.screens
# MAGIC (
# MAGIC   screen_name string,
# MAGIC   name string
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/silver/desafio_serasa/screens"

# COMMAND ----------

# DBTITLE 0,SCREENS
# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view vw_screens
# MAGIC as
# MAGIC select distinct u_screen_name as screen_name, u_name as name
# MAGIC     from desafio_serasa.tweets_recentes

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO desafio_serasa.screens AS sc
# MAGIC USING vw_screens vw 
# MAGIC     on (sc.screen_name = vw.screen_name)
# MAGIC   WHEN MATCHED THEN 
# MAGIC       UPDATE SET *	
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC      INSERT *

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE desafio_serasa.screens;
# MAGIC VACUUM desafio_serasa.screens RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC   from desafio_serasa.screens

# COMMAND ----------

dbutils.fs.rm('dbfs:/silver/desafio_serasa/tweets', True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS desafio_serasa.tweets;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS desafio_serasa.tweets
# MAGIC (
# MAGIC   id_periodo string
# MAGIC   ,id string
# MAGIC   ,text string
# MAGIC   ,source string
# MAGIC   ,screen_name string	 	
# MAGIC   ,location	string	
# MAGIC   ,description string		
# MAGIC   ,url string		
# MAGIC   ,followers_count integer		
# MAGIC   ,friends_count integer
# MAGIC   ,retweet_count integer		
# MAGIC   ,favorite_count integer		
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/silver/desafio_serasa/tweets"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view vw_tweets
# MAGIC as
# MAGIC select  
# MAGIC   date_format(created_at, 'yyyyMMdd') as id_periodo	
# MAGIC   ,id		
# MAGIC   ,text		
# MAGIC   ,replace(substring(source, charindex('>', source)+1, charindex('</a>', source)),'</a>','') as source		
# MAGIC   ,u_screen_name as screen_name	 	
# MAGIC   ,u_location as location		
# MAGIC   ,u_description as description		
# MAGIC   ,u_url as url		
# MAGIC   ,u_followers_count as followers_count		
# MAGIC   ,u_friends_count as friends_count
# MAGIC   ,retweet_count		
# MAGIC   ,favorite_count		
# MAGIC from desafio_serasa.tweets_recentes

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO desafio_serasa.tweets AS sc
# MAGIC USING vw_tweets vw 
# MAGIC     on (sc.id = vw.id)
# MAGIC   WHEN MATCHED THEN 
# MAGIC       UPDATE SET *	
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC      INSERT *

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE desafio_serasa.tweets;
# MAGIC VACUUM desafio_serasa.tweets RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select *
# MAGIC   from desafio_serasa.tweets

# COMMAND ----------

