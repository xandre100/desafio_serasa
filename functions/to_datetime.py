# Databricks notebook source
# DBTITLE 1,Tratamento de datas
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col

from datetime import datetime, timedelta
from email.utils import parsedate_tz
from dateutil.parser import parse

def to_datetime(datestring):
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt - timedelta(seconds=time_tuple[-1])
  
spark.udf.register("to_datetime", to_datetime, DateType())

# COMMAND ----------

