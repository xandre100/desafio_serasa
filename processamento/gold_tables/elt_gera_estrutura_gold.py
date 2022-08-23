# Databricks notebook source
# MAGIC %md 
# MAGIC ##PREPARA ESTRUTURA GOLD

# COMMAND ----------

from datetime import datetime
from dateutil.parser import parse
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS dm_desafio_serasa;
# MAGIC USE dm_desafio_serasa;

# COMMAND ----------

# MAGIC %md 
# MAGIC ##DIM_SOURCE

# COMMAND ----------

spark.sql(""" DROP TABLE IF EXISTS dm_desafio_serasa.dim_source """)

dbutils.fs.rm("/gold/dm_desafio_serasa/dim_source", True)

spark.sql("""
  CREATE TABLE dm_desafio_serasa.dim_source
  USING PARQUET
  LOCATION "/gold/dm_desafio_serasa/dim_source"
  AS
  select * from desafio_serasa.source  
  """)  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dm_desafio_serasa.dim_source

# COMMAND ----------

# MAGIC %md
# MAGIC ##DIM_PERIODO

# COMMAND ----------

spark.sql(""" DROP TABLE IF EXISTS dm_desafio_serasa.dim_periodo""")

dbutils.fs.rm("/gold/dm_desafio_serasa/dim_periodo", True)

spark.sql("""
  CREATE TABLE dm_desafio_serasa.dim_periodo
  USING PARQUET
  LOCATION "/gold/dm_desafio_serasa/dim_periodo"
  AS
  select * from desafio_serasa.periodo  
  """)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC   from dm_desafio_serasa.dim_periodo

# COMMAND ----------

# MAGIC %md
# MAGIC ##DIM_SCREEN

# COMMAND ----------

spark.sql(""" DROP TABLE IF EXISTS dm_desafio_serasa.dim_screen""")

dbutils.fs.rm("/gold/dm_desafio_serasa/dim_screen", True)

spark.sql("""
  CREATE TABLE dm_desafio_serasa.dim_screen
  USING PARQUET
  LOCATION "/gold/dm_desafio_serasa/dim_screen"
  AS
  select * from desafio_serasa.screens  
  """) 

# COMMAND ----------

# DBTITLE 0,SCREENS
# MAGIC %sql
# MAGIC 
# MAGIC select * from dm_desafio_serasa.dim_screen

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##FAT_TWEETS

# COMMAND ----------

spark.sql(""" DROP TABLE IF EXISTS dm_desafio_serasa.fat_tweets""")

dbutils.fs.rm("/gold/dm_desafio_serasa/fat_tweets", True)

spark.sql("""
  CREATE TABLE dm_desafio_serasa.fat_tweets
  USING PARQUET
  LOCATION "/gold/dm_desafio_serasa/fat_tweets"
  AS
  select * from desafio_serasa.tweets  
  """)  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dm_desafio_serasa.fat_tweets

# COMMAND ----------

# MAGIC %md
# MAGIC ##FAT_SOURCE

# COMMAND ----------

spark.sql(""" DROP TABLE IF EXISTS dm_desafio_serasa.fat_source""")

dbutils.fs.rm("/gold/dm_desafio_serasa/fat_source", True)

spark.sql("""
  CREATE TABLE dm_desafio_serasa.fat_source
  USING PARQUET
  LOCATION "/gold/dm_desafio_serasa/fat_source"
  AS
  select source, count(*) as registers
    from dm_desafio_serasa.fat_tweets
   group by source 
  """)

display(spark.sql("""select *
  from dm_desafio_serasa.fat_source"""))

# COMMAND ----------

spark.sql(""" DROP TABLE IF EXISTS dm_desafio_serasa.fat_tweets_by_screen_name""")

dbutils.fs.rm("/gold/dm_desafio_serasa/fat_tweets_by_screen_name", True)

spark.sql("""
  CREATE TABLE dm_desafio_serasa.fat_tweets_by_screen_name
  USING PARQUET
  LOCATION "/gold/dm_desafio_serasa/fat_tweets_by_screen_name"
  AS
  select  screen_name, 
          sum(followers_count) as followers, 
          sum(friends_count) as friends, 
          sum(retweet_count) as retweet, 
          sum(favorite_count) as favorite_count
  from dm_desafio_serasa.fat_tweets
group by screen_name
  """)

display(spark.sql("""select *
  from dm_desafio_serasa.fat_tweets_by_screen_name"""))


# COMMAND ----------

spark.sql(""" DROP TABLE IF EXISTS dm_desafio_serasa.fat_tweets_by_date""")

dbutils.fs.rm("/gold/dm_desafio_serasa/fat_tweets_by_date", True)

spark.sql("""
  CREATE TABLE dm_desafio_serasa.fat_tweets_by_date
  USING PARQUET
  LOCATION "/gold/dm_desafio_serasa/fat_tweets_by_date"
  AS
  select  p.date, s.name, count(f.screen_name) as tweets
  from dm_desafio_serasa.fat_tweets f inner join dm_desafio_serasa.dim_periodo p 
        on f.id_periodo = p.id_periodo
       inner join dm_desafio_serasa.dim_screen s on s.screen_name = f.screen_name
  group by p.date, s.name
  """)

display(spark.sql("""select *
  from dm_desafio_serasa.fat_tweets_by_date"""))

# COMMAND ----------

