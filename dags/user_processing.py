from airflow.model import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensors
from airflow.providers.http.operators.http import SimpleHttpOperators
from datatime import datetime
default_args={
    'start_date':datetime(2020,1,1)
}

with DAG("user_processing",schedule_interval="@daily",

        default_args=default_args
        catchup=False) as dag:
    creating_table = SqliteOperator(
        task_id = 'creating_table',
        sqlite_conn__id = 'db_sqlite',
        sql = '''
        CREATE TABLE IF NOT EXIST user(
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL PRIMARY KEY
        );
        '''
    )

    is_api-available = HttpSensors(
        task_id = 'is_api_available',
        http_conn_id = 'user_api',
        endpoint='api/'
    )

    extracting_user= SimpleHttpOperators(
        task_id='extracting_user'
,       http_conn_id = 'user_api',  
        endpoint='api/',
        method='GET',
        response_fileter= lambda response: json.loads(response.text)  ,
        log_response =True

    )

    processing_user =PythonOperator(
        task_id='processing_user',
        python_collable = processing_user
    )
    
    storing_user = BashOperator(
        task_id='storing_user',
        bash_command ='echo -e ".separator","\n.import /tmp/processed_user.csv users" | sqlite3 / home/airflow/airflow/airflow.db'
    )


    creating_table>> is_api_available >>extracting_user>>processing_user>>storing_user

