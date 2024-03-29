import sys
sys.path.append('/opt/airflow/dags')
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task,dag
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
from sqlalchemy import create_engine
from handler_services.webparser_services import  WebDriverSetup,BykeDataPage,LinksFile


default_args={"owner": "benesh",
              'retries':1,
              'retry_delay':timedelta(minutes=5)
              }


def get_links_from_web(ti):
    """
    get all links from web page https://s3.amazonaws.com/tripdata/index.html
    :return: links from web page
    :rtype: list
    """
    links = None
    with WebDriverSetup() as parse_driver:
        try:
            parse_driver.driver.get("https://s3.amazonaws.com/tripdata/index.html")
            page = BykeDataPage(parse_driver.driver)
            links = page.get_all_links_files_data()
        except Exception as e:
            raise e
    ti.xcom_push(key='links', value=links)
    # print(links)
def insert_url_to_db(ti):
    # postgres_conn_id = "postgress_conn"
    # hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    # conn_string = f"postgresql://{hook.login}:{hook.password}@{hook.host}:{hook.port}/{hook.schema}"

    conn_string = 'postgresql://admin:admin123@postgres-app:5432/project_airflow'
    engine = create_engine(conn_string)
    links = ti.xcom_pull(task_ids='get_links_from_web', key='links')
    df = pd.DataFrame(data=links,columns=LinksFile._fields)
    df.to_sql(name='data_info_v3', con=engine, if_exists='append', index=False)


with DAG(
    dag_id='listing_data_files_v17',
    default_args=default_args,
    start_date=datetime(2024, 2, 1),
    schedule_interval=timedelta(hours=1),
    catchup=False) as dag :

    # init_process = PostgresOperator(
    #     task_id='init_process',
    #     postgres_conn_id='postgress_conn',
    #     sql='sql/script_creat_table_data_info.sql'
    # )

    process_get_urls = PythonOperator(
        task_id='get_links_from_web',
        python_callable=get_links_from_web
    )
    put_links_to_postgress= PythonOperator(
        task_id='put_links_to_postgress',
        python_callable=insert_url_to_db
    )

    process_get_urls>> put_links_to_postgress

