# Databricks notebook source
# MAGIC %pip install databricks-test

# COMMAND ----------

# MAGIC %pip install apache-airflow-providers-databricks

# COMMAND ----------

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "estudos_databricks_operator"

with DAG(
    dag_id=DAG_ID,
    schedule_interval='@daily',
    start_date=datetime(2022, 8, 23),
    tags=['estudos'],
    catchup=False,
) as dag:
    # [START howto_operator_databricks_json]
    # Example of using the JSON parameter to initialize the operator.
    new_cluster = {
        'spark_version': '7.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'aws_attributes': {'availability': 'ON_DEMAND'},
        'num_workers': 8,
    }    
    
    ingestion_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': '/desafio_serasa/ingestao/ingestion_tweets',
        },
    }

    ingestion_task = DatabricksSubmitRunOperator(task_id='ingestion_task', json=ingestion_task_params)
        
    elt_gera_estrutura_silver_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': '/desafio_serasa/processamento/silver_tables/elt_gera_estrutura_silver',
        },
    }    
    
    elt_gera_estrutura_silver_task = DatabricksSubmitRunOperator(task_id='elt_gera_estrutura_silver_task', json=elt_gera_estrutura_silver_task_params)       
    
    
    elt_gera_estrutura_gold_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': '/desafio_serasa/processamento/gold_tables/elt_gera_estrutura_gold',
        },
    }    
    
    elt_gera_estrutura_gold_task = DatabricksSubmitRunOperator(task_id='elt_gera_estrutura_gold_task', json=elt_gera_estrutura_gold_task_params)
    
    
    elt_gera_analises_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': '/desafio_serasa/processamento/gold_tables/elt_gera_analises',
        },
    }    
    
    elt_gera_analises_task = DatabricksSubmitRunOperator(task_id='elt_gera_analises_task', json=elt_gera_analises_task_params)
    
    ##Orquestracao
    ingestion_task >> elt_gera_estrutura_silver_task >> elt_gera_estrutura_gold_task >> elt_gera_analises_task


# COMMAND ----------

