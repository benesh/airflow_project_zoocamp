from handler_services.db_postgres_services.models import DataBykeUrlsDB
from handler_services.data_file_info import DataBykeUrlsClass
from sqlalchemy.orm import Session
from sqlalchemy import select


class StepsInsertBykeUrls:
    def __init__(self, engine, list_data_urls : list[DataBykeUrlsDB]):
        self.engine = engine
        self.list_urls = list_data_urls
    def run(self):
        with Session(self.engine) as session:
            session.add_all(self.list_urls)
            session.commit()

class StepsGetAllUrlDataByke:
    def __init__(self, engine):
        self.engine = engine
        # self.list_urls = list_data_urls

    def run(self):
        with Session(self.engine) as session:
            stmt = select(DataBykeUrlsDB)
            list_form_database = []
            for data_url in session.scalars(stmt):
                list_form_database.append(DataBykeUrlsClass.from_database(data_url))
            return list_form_database
