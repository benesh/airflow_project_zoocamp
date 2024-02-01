
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker, Session

from dbinstance import DBInstance, DataInfo, StatusFile
from webparser_services import WebParser, PageBykeData


def parsing_links():
    links=None
    with WebParser() as parse_driver:
        try :
            parse_driver.driver.get("https://s3.amazonaws.com/tripdata/index.html")
            page = PageBykeData(parse_driver.driver)
            links = page.get_all_links_files_data()
            print(links)

        except Exception as e:
            raise e
    return links

def get_links_existed(session):
    stmt = select(DataInfo)
    links = session.scalars(stmt)
    return links

def filter_links( urls_downloaded , urls_existed ):
    list_to_insert = []
    # if databas has already urls so we have to filter them
    if isinstance(urls_existed, list) and len(urls_existed) > 0:
        file_names_existed = [data_info.file_name for data_info in urls_existed]
        file_names_dwonloaded = [tuple_url[1] for tuple_url in urls_downloaded]
        difference1 = list(set(file_names_dwonloaded) - set(file_names_existed))
        for count in range(5):
            if difference1[count] == urls_downloaded[count]:
                list_to_insert.append(urls_downloaded[count])
    else: # the has nathing
        for count in range(5):
            list_to_insert.append(urls_downloaded[count])
    return list_to_insert

def insert_links(urls_to_insert, session):
    for link in urls_to_insert:
        session.add(DataInfo.from_arry(link))
    session.commit()


def process():
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
              'retries':5,
              'retry_delay':timedelta(minutes=5)
              }

with DAG(
    dag_id='kafka_stream_v4',
    default_args=default_args,
    start_date=datetime(2024, 1, 17),
    schedule_interval=timedelta(minutes=5),
    catchup=False) as dag :

    processUrl = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=process
    )

processUrl





