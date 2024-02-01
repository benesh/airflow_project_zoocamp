import uuid
from enum import Enum
import re
from sqlalchemy import create_engine, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy import String
import yaml
from pydantic import BaseModel
import os


class StatusFile(Enum):
    DOWNLOADED = "DOWNLOADED"
    ERROR = "ERROR"
    UNZIPPED = "UNZIPPED"
    PARQUETED = "PARQUET"
    CREATED = "CREATED"


LINKS : (str,str)

class Config(BaseModel):
    config_name:str
    user:str
    password:str
    hostname:str
    port : str
    database:str


class DBInstance:
    def __init__(self,path_config="/opt/airflow/dags/configPostgress.yaml"):
        # file_path = os.path.abspath(path_config)
        with open(path_config, "r") as file:
            yaml_data = yaml.safe_load(file)
            self.conf_list = [Config(**conf) for conf in yaml_data]

    def get_engine(self,):
        conf : Config = self.conf_list[0]
        return create_engine(f'postgresql://{conf.user}:{conf.password}@{conf.hostname}:{conf.port}/{conf.database}')

def get_year_from_list(filename:str) -> int:
    temp = re.findall(r'\d+',filename) # get all digit from the file name
    year = int(temp[0][0:4])
    return year


def get_month_from_list(filename:str) -> int:
    temp = re.findall(r'\d+',filename) # get all digit from the file name
    return int(temp[0][4:])

Base = declarative_base()

class DataInfo(Base):
    __tablename__ = "data_info"

    id = Column(String(50), primary_key=True)
    url=Column(String(30))
    file_name = Column(String(30))
    year=Column(Integer)
    month = Column(Integer)
    status = Column(String(30))

    def __init__(self, id: str, url: str, file_name: str, year: int, month: int, status: str):
        self.id = id
        self.year = year
        self.month = month
        self.url = url
        self.file_name = file_name
        self.status = status

    @classmethod
    def from_arry(cls, tuple : (str, str)):
        return cls(
            id=f'{uuid.uuid4()}'
            , url=tuple[0]
            , file_name=tuple[1]
            , year= get_year_from_list(tuple[1])
            , month= get_month_from_list(tuple[1])
            , status=StatusFile.DOWNLOADED.value
        )