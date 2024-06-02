import logging

from handler_services.db_postgres_services.models import DataBikeUrlsDB
from handler_services.data_bike_services.data_file_info import DataBikeUrlsClass ,StatusFile
from sqlalchemy.orm import Session
from sqlalchemy import select

class StepsInsertBikeUrls:
    def __init__(self, engine, list_data_urls : list[DataBikeUrlsDB]):
        self.engine = engine
        self.list_urls = list_data_urls
    def run(self):
        with Session(self.engine) as session:
            session.add_all(self.list_urls)
            session.commit()

class StepsGetAllUrlDataBike:
    def __init__(self, engine):
        self.engine = engine
        # self.list_urls = list_data_urls
    def run(self):
        with Session(self.engine) as session:
            stmt = select(DataBikeUrlsDB)
            list_form_database = []
            for data_url in session.scalars(stmt):
                list_form_database.append(DataBikeUrlsClass.from_database(data_url))
            return list_form_database
class StepsGetAllUrlDataBikeClassWithStatus:
    def __init__(self, engine,param_status:StatusFile):
        self.engine = engine
        self.param_status = param_status
    def run(self):
        list_form_database = []
        with Session(self.engine) as session:
            stmt = select(DataBikeUrlsDB).where(DataBikeUrlsDB.status == self.param_status.value)
            for data_url in session.scalars(stmt):
                list_form_database.append(DataBikeUrlsClass.from_database(data_url))
            return list_form_database
class StepsUpdateStatusDataUrls:
    def run(self, engine, list_data_urls : list[DataBikeUrlsDB]):
        logging.info('Updating status of data in DB')
        with Session(engine) as session:
            [session.query(DataBikeUrlsDB).filter_by(url=data_byke.url).
             update({DataBikeUrlsDB.status: data_byke.status, DataBikeUrlsDB.updated_at:data_byke.updated_at}) for data_byke in list_data_urls]
            session.commit()


