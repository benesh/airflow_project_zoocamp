from enum import Enum
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy import String,Integer
import uuid
from enum import Enum
from utils import get_year_from_list, get_month_from_list

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


Base = declarative_base()

class DataInfo(Base):
    __tablename__ = "data_info"

    id = Column(String(50), primary_key=True)
    url = Column(String(30))
    file_name = Column(String(30))
    year =Column(Integer)
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
    def from_tupple(cls, tuple : (str, str)):
        return cls(
            id=f'{uuid.uuid4()}'
            , url=tuple[0]
            , file_name=tuple[1]
            , year= get_year_from_list(tuple[1])
            , month= get_month_from_list(tuple[1])
            , status=StatusFile.DOWNLOADED.value
        )