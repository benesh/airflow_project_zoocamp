import sys
sys.path.append('/opt/airflow/dags')

import re
from handler_services.webparser_services import WebDriverSetup, BykeDataPage
from sqlalchemy import select
import logging
from handler_services.data_file_info import Byke_Info


def parsing_links():
    links=None
    with WebDriverSetup() as parse_driver:
        try :
            parse_driver.driver.get("https://s3.amazonaws.com/tripdata/index.html")
            page = BykeDataPage(parse_driver.driver)
            links = page.get_all_links_files_data()
            print(links)

        except Exception as e:
            raise e
    return links

def get_links_existed(session):
    stmt = select(Byke_Info)
    # links = session.execute(stmt).fetchall()
    result = session.query(Byke_Info).all()
    return result

def filter_links(urls_from_web:(str, str), urls_from_db:list[Byke_Info]):
    logging.info("in method filter")
    # if databas has already urls so we have to filter them
    list_to_insert = None
    if isinstance(urls_from_db, list) and len(urls_from_db) > 0:
        logging.info("in filter links")
        list_to_insert = [tuple_url for tuple_url in urls_from_web if tuple_url[1] not in [data_info.file_name for data_info in urls_from_db]]
        result = list_to_insert[0]
    else:
        result = urls_from_web[0]
    # logging.info("row to instert",*result)
    return list_to_insert

def insert_links(urls_to_insert, session):
    # logging.info("row in method insert", urls_to_insert)
    if urls_to_insert == list:
        for link in urls_to_insert:
            session.add(Byke_Info.from_tupple(link))
    else:
        session.add(Byke_Info.from_tupple(urls_to_insert))
    session.commit()


#
# def get_links_from_db_and_filter(**kwargs) :
#     task_instance = kwargs['ti']
#     links_2 = task_instance.xcom_pull(key='links')
#     # links_form_web = ti.xcom_pull(task_id='get_links_from_web',key='links')
#     config = read_config("/opt/airflow/dags/conf/configPostgress.yaml",0)
#     db_object = DBInstance(config)
#     enginedb = db_object.get_engine()
#     stmt=text ("SELECT * FROM public.data_info")
#     result=None
#     list_data = []
#     with Session(enginedb) as session:
#         result = session.execute(stmt)
#         for row in result:
#             print(row)
#             list_data.append(Byke_Info.from_tupple(row))
#     # links_3 = filter_links(links_2, result)
#     # print("links filterred",links_3)
#     print(list_data)



# def process():
#     # Getting urls from website
#     urls_downloaded = parsing_links()
#     # Creating db instance
#     dbobject = DBInstance()
#     # Creating access
#     engine = dbobject.get_engine()
#     with Session(engine) as session:
#         urls_existed = get_links_existed(session)
#         # filter urls already existed
#         urls_to_insert = filter_links(urls_downloaded,urls_existed)
#         # Base.metadata.create_all(engine)
#         # insert filtered urls
#         insert_links(urls_to_insert, session)
#         queried_data = get_links_existed(session)
#         for query in queried_data:
#             print(f"Queried User: {query.url}, {query.file_name}")

# print(get_links_from_web())
# get_links_from_web()
