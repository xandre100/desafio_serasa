# Databricks notebook source
# MAGIC %pip install databricks-test

# COMMAND ----------

# MAGIC %pip install apache-airflow-providers-databricks

# COMMAND ----------

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example DAG which uses the DatabricksSubmitRunOperator.
In this example, we create two tasks which execute sequentially.
The first task is to run a notebook at the workspace path "/test"
and the second task is to run a JAR uploaded to DBFS. Both,
tasks use new clusters.

Because we have set a downstream dependency on the notebook task,
the spark jar task will NOT run until the notebook task completes
successfully.

The definition of a successful run is if the run has a result_state of "SUCCESS".
For more information about the state of a run refer to
https://docs.databricks.com/api/latest/jobs.html#runstate
"""

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
        'spark_version': '11.1.x-scala2.12',
        'node_type_id': 'Standard_DS3_v2',
        'driver_node_type_id': 'Standard_DS3_v2',
        'azure_attributes': {'availability': 'ON_DEMAND_AZURE', 'first_on_demand': 1, 'spot_bid_max_price': -1 },
        'custom_tags': {'ResourceClass': 'SingleNode'}
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
    
    # [END howto_operator_databricks_json]
    # [START howto_operator_databricks_named]
    # Example of using the named parameters of DatabricksSubmitRunOperator
    # to initialize the operator.
    #spark_jar_task = DatabricksSubmitRunOperator(
    #    task_id='spark_jar_task',
    #    new_cluster=new_cluster,
    #    spark_jar_task={'main_class_name': 'com.example.ProcessData'},
    #    libraries=[{'jar': 'dbfs:/lib/etl-0.1.jar'}],
    #)
    #
    # [END howto_operator_databricks_named]
    
    ingestion_task >> elt_gera_estrutura_silver_task >> elt_gera_estrutura_gold_task

    #from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    #list(dag.tasks) >> watcher()

#from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
#test_run = get_test_run(dag)

# COMMAND ----------

