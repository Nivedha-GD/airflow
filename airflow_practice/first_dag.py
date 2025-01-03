from datetime import datetime,timedelta

from airflow import DAG 
from airflow.operators.bash import BashOperator


default_arg1 ={
    'owner' : 'airflow',
    'retries' : 5 ,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='dag1',
    default_args=default_arg1,
    description='created sample dag ',
    start_date=datetime(2024,12,18,11,55),
    schedule_interval='@daily'

) as dag:
    
    task1 = BashOperator(
    task_id = "first_task",
    bash_command="echo created sample dag 1"

    
)
    task2=BashOperator(
        task_id = "second_task",
        bash_command="echo this is dag 1"
    )

    task1.set_downstream(task2)
