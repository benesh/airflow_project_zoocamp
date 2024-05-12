from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from handler_services.db_postgres_services.models import DataBikeUrlsDB
from handler_services.data_byke_services.config_class import StatusFile
from handler_services.data_byke_services.utils_bike_data import get_current_time,get_year,get_month

class DataBikeUrlsClass(BaseModel):
    file_name:str
    url:str
    month:Optional[str] = None
    year:Optional[str] = None
    status: Optional[StatusFile] = None
    created_at:Optional[datetime] = None
    updated_at:Optional[datetime] = None
    id:Optional[int] = None

    @classmethod
    def from_database(cls, data_byke_url:DataBikeUrlsDB):
        return DataBikeUrlsClass(file_name=data_byke_url.file_name,
                                 url=data_byke_url.url,
                                 month=data_byke_url.month,
                                 year=data_byke_url.year,
                                 status= StatusFile(data_byke_url.status),
                                 created_at=data_byke_url.created_at,
                                 updated_at=data_byke_url.updated_at,
                                 id=data_byke_url.id)
    @classmethod
    def from_tuple_link_file_name(cls,link:str,file_name:str):
        return DataBikeUrlsClass(file_name=file_name,
                                 url=link,
                                 month=get_month(file_name),
                                 year=get_year(file_name),
                                 status=StatusFile('CREATED'),
                                 created_at=get_current_time(),
                                 updated_at=get_current_time())
    def get_db(self):
        return DataBikeUrlsDB(
            file_name=self.file_name,
            url=self.url,
            month=self.month,
            year=self.year,
            status=str(self.status.value),
            created_at=self.created_at,
            updated_at=self.updated_at,
            id=self.id
        )

    def set_status(self,status : StatusFile):
        self.status = status
    @property
    def get_path_raw(self):
        if self.month is None:
            return f'{self.year}/{self.file_name}'
        return f'{self.year}/{self.month}/{self.file_name}'
    def update_date(self):
        self.updated_at = get_current_time()
