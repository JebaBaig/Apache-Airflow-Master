from airflow import DAG
from airflow.operators.bash import  BashOperator
from airflow.operators.subdag import SubDagOperator
from datatime import datetime
from subdags.subdag_parallel_dag import subdag_parallel_dag
from airflow.utils.task_group import TaskGroup

default_args={
    'start_date':datetime(2020,1,1)
}

with DAG"parallel_dag",schedule_interval="@daily",default_args=default_args
        catchup=False) as dag:

    task_1 =BashOperator(
        task_id='task 1',
        bash_command="sleep 3"
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        task_2 =BashOperator(
            task_id='task 2',
            bash_command="sleep 3"
        )

        with TaskGroup('spark_tasks') as spark_tasks:
            task_3 =BashOperator(
            task_id='spark_tasks.task 3',
            bash_command="sleep 3"
            ) 

        with TaskGroup('flink_tasks') as flink_tasks:
            task_3 =BashOperator(
            task_id='flink_tasks.task 3',
            bash_command="sleep 3"
            ) 


        

    # processing = SubDagOperator(
    #     task_id = "processing_tasks",
    #     subdag=subdag_parallel_dag('parallel_dag','processing_task',default_args)
    # )

  
    task_4 =BashOperator(
        task_id='task 4',
        bash_command="sleep 3"
    )

    # task_1 >> [task_2,task_3] >> task_4

    # task_1 >> processing >> task_4

    task_1 >> processing_tasks >> task_4