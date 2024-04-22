import sys
from datetime import datetime

import logging
from handler_services.data_file_info import DataBykeUrlsClass


def filter_links(urls_from_web:list[DataBykeUrlsClass], urls_from_db:list[DataBykeUrlsClass]):
    logging.info("in method filter")
    list_url_db = [item.url for item in urls_from_db]
    list_to_insert = [tuple_url for tuple_url in urls_from_web if tuple_url.url not in list_url_db]
    if len(list_to_insert)==0:
        return None
    else:
        return [list_to_insert[0]]

def get_current_time():
    return datetime.now()


