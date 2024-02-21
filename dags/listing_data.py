import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy.orm import  Session

from dbinstance import DBInstance, DataInfo




def process():
    logging.basicConfig(filename='myapp.log', level=logging.INFO)
    # Getting urls from website
    urls_downloaded = parsing_links()
    # Creating db instance
    dbobject = DBInstance()
    # Creating access
    engine = dbobject.get_engine()
    with Session(engine) as session:
        urls_existed = get_links_existed(session)
        # filter urls already existed
        urls_to_insert = filter_links(urls_downloaded,urls_existed)
        # Base.metadata.create_all(engine)
        # insert filtered urls
        insert_links(urls_to_insert, session)
        queried_data = get_links_existed(session)
        for query in queried_data:
            print(f"Queried User: {query.url}, {query.file_name}")

process()



default_args={"owner": "benesh",
              'retries':1,
              'retry_delay':timedelta(minutes=5)
              }

with DAG(
    dag_id='listing_data_files_v12',
    default_args=default_args,
    start_date=datetime(2024, 2, 1),
    schedule_interval=timedelta(minutes=2),
    catchup=False) as dag :

    processUrl = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=process
    )

processUrl

