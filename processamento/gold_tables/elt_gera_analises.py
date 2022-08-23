# Databricks notebook source
# MAGIC %md 
# MAGIC ##VAMOS GERAR ALGUMAS ANÁLISES

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
# MAGIC use dm_desafio_serasa;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##TWEETS POR SOURCE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC   from fat_source

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##TWEETS SCREEN_NAME

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  *
# MAGIC   from fat_tweets_by_screen_name

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  *
# MAGIC   from fat_tweets_by_date

# COMMAND ----------

