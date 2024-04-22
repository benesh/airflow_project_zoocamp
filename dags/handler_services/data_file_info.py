import sys
from collections import namedtuple
from datetime import datetime
# from handler_services.utils_byke_data import StatusFile
from typing import Optional
from pydantic.dataclasses import dataclass
from pydantic import RootModel,BaseModel,validator
from dataclasses_json import dataclass_json
from handler_services.db_postgres_services.models import DataBykeUrlsDB ,StatusFile


class DataBykeUrlsClass(BaseModel):
    file_name:str
    url:str
    month:Optional[str] = None
    year:Optional[str] = None
    status: Optional[StatusFile] = None
    created_at:Optional[datetime] = None
    updated_at:Optional[datetime] = None

    @classmethod
    def from_database(csl,data_byke_url:DataBykeUrlsDB):
        return DataBykeUrlsClass(file_name=data_byke_url.file_name,
                                 url=data_byke_url.url,
                                 month=data_byke_url.month,
                                 year=data_byke_url.year,
                                 status=data_byke_url.status,
                                 created_at=data_byke_url.created_at,
                                 updated_at=data_byke_url.updated_at)
    def get_db(self):
        return DataBykeUrlsDB(
            file_name=self.file_name,
            url=self.url,
            month=self.month,
            year=self.year,
            status=self.status,
            created_at=self.created_at,
            updated_at=self.updated_at
        )