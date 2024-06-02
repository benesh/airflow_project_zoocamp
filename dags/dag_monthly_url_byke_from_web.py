import json
import logging
import sys
sys.path.append('/opt/airflow/dags')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from handler_services.webparser_services.webparser_services import  WebDriverSetup, BykeDataPage
from handler_services.db_postgres_services.dbinstance import DBInstance
from handler_services.reader_config import FactoryReaderConfig,FactoryReaderFile
from settings import POSTGRES_MINIO_FILE,POSTGRES_ATTRIBUT,URL_DATA_DYKE
from handler_services.data_bike_services.data_file_info import DataBikeUrlsClass
from handler_services.db_postgres_services.steps_db_queries import StepsGetAllUrlDataBike,StepsInsertBikeUrls
from handler_services.data_bike_services.utils_bike_data import filter_links

default_args={"owner": "benesh",
              'retries':2,
              'retry_delay':timedelta(minutes=2)
              }

def task_init_db():
    db_instance = DBInstance(reader_config=FactoryReaderConfig.CONFIG_POSTGRES,
                             reader_file=FactoryReaderFile.YAML,
                             path_config=POSTGRES_MINIO_FILE,
                             attribut=POSTGRES_ATTRIBUT)
    db_instance.init_database()

def get_links_from_web(ti):
    """
    get all links from web page https://s3.amazonaws.com/tripdata/index.html
    :return: links from web page
    :rtype: list
    """
    links = None
    with WebDriverSetup() as parse_driver:
        try:
            parse_driver.driver.get(URL_DATA_DYKE)
            page = BykeDataPage(parse_driver.driver)
            links = page.get_all_links_files_data()
        except Exception as e:
            raise e
    list_json_dump = [url.model_dump_json() for url in links ]
    ti.xcom_push(key='links_from_web', value=list_json_dump)
    # print(links)

def get_urls_from_db(ti):
    db_instance = DBInstance(reader_config=FactoryReaderConfig.CONFIG_POSTGRES,
                             reader_file=FactoryReaderFile.YAML,
                             path_config=POSTGRES_MINIO_FILE,
                             attribut=POSTGRES_ATTRIBUT)
    engine = db_instance.engine
    step_get_data = StepsGetAllUrlDataBike(engine)
    data_byke_urls = step_get_data.run()
    list_json_dump = [url.model_dump_json() for url in data_byke_urls]
    ti.xcom_push(key='links_from_db', value=list_json_dump)

def sort_links(ti):
    links_from_web = ti.xcom_pull(key='links_from_web', task_ids=['get_links_from_web'])
    links_from_db = ti.xcom_pull(key='links_from_db', task_ids=['get_urls_from_db'])
    from_db = [DataBikeUrlsClass.parse_obj(json.loads(link)) for link in links_from_db[0]]
    print(links_from_web)
    print(links_from_db)
    from_web = [DataBikeUrlsClass.parse_obj(json.loads(link)) for link in links_from_web[0]]
    links = filter_links(from_web, from_db)
    if links is not None:
        links_json = [url.model_dump_json() for url in links]
        print(links)
        ti.xcom_push(key='to_db', value=links_json)
    else:
        ti.xcom_push(key='to_db', value="stop_dag")


def insert_url_to_db(ti):
    urls_1 = ti.xcom_pull(key='to_db', task_ids=['sort_links'])
    print('Url data byketo insert in db',urls_1)
    if urls_1[0] == "stop_dag":
        logging.info('All URLs have been stored in databas')
    else:
        urls_2 = [DataBikeUrlsClass.parse_obj(json.loads(link)) for link in urls_1[0]]
        data_to_db = [url_one.get_db() for url_one in urls_2]
        print(data_to_db)
        db_instance = DBInstance(reader_config=FactoryReaderConfig.CONFIG_POSTGRES,
                                 reader_file=FactoryReaderFile.YAML,
                                 path_config=POSTGRES_MINIO_FILE,
                                 attribut=POSTGRES_ATTRIBUT)
        engine = db_instance.engine
        insert_data = StepsInsertBikeUrls(engine, data_to_db)
        insert_data.run()

with DAG(
    dag_id='listing_data_files_v29',
    default_args=default_args,
    start_date=datetime(2024, 2, 1),
    schedule_interval=timedelta(hours=1),
    catchup=False) as dag :

    # init_process = PostgresOperator(
    #     task_id='init_process',
    #     postgres_conn_id='postgress_conn',
    #     sql='sql/script_creat_table_data_info.sql'
    # )

    init_db = PythonOperator(
        task_id='init_db',
        python_callable=task_init_db,
    )

    task_get_urls_web = PythonOperator(
        task_id='get_links_from_web',
        python_callable=get_links_from_web
    )
    task_get_urls_db = PythonOperator(
        task_id='get_urls_from_db',
        python_callable=get_urls_from_db
    )
    task_sort_links = PythonOperator(
        task_id='sort_links',
        python_callable=sort_links,
    )
    put_links_to_postgress= PythonOperator(
        task_id='put_links_to_postgress',
        python_callable=insert_url_to_db
    )

    init_db >> ( task_get_urls_web, task_get_urls_db ) >> task_sort_links >> put_links_to_postgress

