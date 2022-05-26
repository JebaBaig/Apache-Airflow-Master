from airflow  import DAG 
from airflow.operators.python import PythonOperator
from elasticsearch_plugin.operators.postgres_to_elastic import PostgresToElasticOperator

from elasticsearch_plugin.hooks.elastichook import ElasticHook

from datetime import datatime

default_args = {
    'start_date' :datetime(2020,1,1)
} 

def print_es_info():
    hook = ElasticHook()
    print(hook.info)()

with DAG('elasticsearch_dag', schedule_interval='@daily', default_args= default_args,catchup= False)

    print_es_info =PythonOperator(
        task_id =' print_es_info',
        python_callable = print_es_info
    )

    connections_to_es = PostgresToElasticOperator(
        task_id='connections_to_es',
        sql =' SELECT * FROM connection',
        index ='connections'
    )

    print_es_info >> connections_to_es