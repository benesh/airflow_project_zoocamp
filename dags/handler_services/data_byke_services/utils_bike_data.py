from datetime import datetime
import logging

def filter_links(urls_from_web:list, urls_from_db:list):
    """
    A methode that retrun a list or url that not being in the database
    :param urls_from_web: from list in web
    :param urls_from_db: from list in db
    :return: list that not in the db
    """
    logging.info("in method filter")
    list_url_db = [item.url for item in urls_from_db]
    list_to_insert = [tuple_url for tuple_url in urls_from_web if tuple_url.url not in list_url_db]
    if len(list_to_insert)==0:
        return None
    else:
        return [list_to_insert[0]]

def get_current_time():
    return datetime.now()

def get_month(file_name:str) -> str:
    month = str(int(file_name[4:6])) if file_name[:6].isdigit() else None
    return month
def get_year(file_name:str) -> str:
    year = str(int(file_name[:4])) if file_name[:4].isdigit() else None
    return year