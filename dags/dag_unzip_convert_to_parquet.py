from datetime import datetime, timedelta
from airflow.decorators import dag, task



default_args = {
    'owner': 'benesh',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id='dag_unzip_convert_to_parquet_v02',
     default_args=default_args,
     start_date=datetime(2024, 4, 20),
     schedule_interval='@daily')

def unzip_data_file():
    @task(multiple_outputs=True)
    def get_list_from_db():
        return {
            'first_name': 'Jerry',
            'last_name': 'Fridman'
        }

    @task()
    def data_unzipping_convert_to_parquet():
        return 19

    @task()
    def data_convert_to_parquet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name} "
              f"and I am {age} years old!")

    name_dict = get_list_from_db()
    age = data_unzipping_convert_to_parquet()
    data_convert_to_parquet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
          age=age)


greet_dag = unzip_data_file()