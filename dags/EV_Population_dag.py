from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import requests
import json
from pathlib import Path
import sys
#from Airflow.code.load_data_mongo import load_data_mongo
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator


# define the callback function
def on_failure_callback(context):
    """Function that will be triggered when a task fails."""
    print(f"Task failed: {context['task_instance'].task_id}")

default_args = {
    'owner': 'shafik',
    'start_date': datetime(2023, 4, 8),
    'on_failure_callback': on_failure_callback  # set the callback function
}

dag = DAG(
    'EV_Population_Pipeline',
    default_args=default_args,
    schedule_interval=None
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

mongo_task = BashOperator(
    task_id='load_data_mongo',
    #bash_command= 'mkdir C:/Users/Admin/Desktop/DA_Project/DAP/Airflow/test',
    bash_command='python /opt/airflow/code/load_data_mongo.py',
    dag=dag
)
postgre_task = BashOperator(
         task_id='load_data_postgre',
         bash_command='python /opt/airflow/code/load_data_postgre.py',
         dag=dag
     )
postgre_task2 = BashOperator(
         task_id='Transformation',
         bash_command='python /opt/airflow/code/transformation.py',
         dag=dag
     )

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

start_task >> mongo_task >> postgre_task >> postgre_task2 >> end_task
