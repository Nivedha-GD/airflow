from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


config = {
    'dag_id_1': {'schedule_interval': '@daily', "start_date": datetime(2024, 1, 1), "table_name": "table_name_1"},
    'dag_id_2': {'schedule_interval': '@hourly', "start_date": datetime(2024, 2, 1), "table_name": "table_name_2"},
    'dag_id_3': {'schedule_interval': None, "start_date": datetime(2024, 3, 1), "table_name": "table_name_3"}
}

def log_start_processing(dag_id, table_name):
    
    print(f"{dag_id} start processing tables in database: {table_name}")

def check_table_exist(table_name):
    
    print(f"Checking if {table_name} exists...")
    if True:  
        return 'insert_new_row'
    return 'create_table'

for dag_id, params in config.items():
    with DAG(
        dag_id=dag_id,
        schedule_interval=params['schedule_interval'],
        start_date=params['start_date'],
        catchup=False
    ) as dag:

        start_processing = PythonOperator(
            task_id='start_processing',
            python_callable=log_start_processing,
            op_args=[dag_id, params['table_name']]
        )

        check_table = BranchPythonOperator(
            task_id='check_table_exist',
            python_callable=check_table_exist,
            op_args=[params['table_name']]
        )

        insert_new_row = EmptyOperator(
            task_id='insert_new_row',
            trigger_rule=TriggerRule.NONE_FAILED  
        )

        create_table = EmptyOperator(
            task_id='create_table',
            trigger_rule=TriggerRule.NONE_FAILED  
        )

        query_the_table = EmptyOperator(
            task_id='query_the_table',
            trigger_rule=TriggerRule.NONE_FAILED   if it’s skipped upstream
        )

        start_processing >> check_table
        check_table >> create_table >> insert_new_row >> query_the_table
        check_table >> insert_new_row >> query_the_table  


    globals()[dag_id] = dag  




        


