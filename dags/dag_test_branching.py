import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException






default_args = {
    'owner': 'benesh',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}
@dag(dag_id='dag_test_branching_v01',
     default_args=default_args,
     start_date=datetime(2024, 4, 23),
     schedule=timedelta(hours=1)
     )
def dag_test_branching_v01():


    @task
    def first_task():
        print('first_task')
        return 'first_task'
    @task
    def second_task():
        print('second_task')
        return 'second_task'
    @task
    def third_task():
        print('third_task')
        return 'third_task'