import re
from webparser_services import WebParser, PageBykeData
from webparser_services import WebParser, PageBykeData
from sqlalchemy import select


def get_year_from_list(filename:str) -> int:
    temp = re.findall(r'\d+',filename) # get all digit from the file name
    year = int(temp[0][0:4])
    return year

def get_month_from_list(filename:str) -> int:
    temp = re.findall(r'\d+',filename) # get all digit from the file name
    return int(temp[0][4:])

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
    # links = session.execute(stmt).fetchall()
    result = session.query(DataInfo).all()
    return result

def filter_links( urls_downloaded:(str,str) , urls_existed:list[DataInfo] ):
    logging.info("in method filter")
    # if databas has already urls so we have to filter them
    if isinstance(urls_existed, list) and len(urls_existed) > 0:
        list_to_insert = [tuple_url for tuple_url in urls_downloaded if tuple_url[1] not in [data_info.file_name for data_info in urls_existed]]
        result = list_to_insert[0]
    else:
        result = urls_downloaded[0]
    # logging.info("row to instert",*result)
    return result

def insert_links(urls_to_insert, session):
    # logging.info("row in method insert", urls_to_insert)
    if urls_to_insert == list:
        for link in urls_to_insert:
            session.add(DataInfo.from_tupple(link))
    else:
        session.add(DataInfo.from_tupple(urls_to_insert))
    session.commit()
