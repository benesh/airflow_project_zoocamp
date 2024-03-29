import sys
from collections import namedtuple

sys.path.append('/opt/airflow/dags')

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy import String,Integer
import uuid
from enum import Enum
# from handler_services.common_utils import get_year_from_list, get_month_from_list

class StatusFile(Enum):
    DOWNLOADED = "DOWNLOADED"
    ERROR = "ERROR"
    UNZIPPED = "UNZIPPED"
    PARQUETED = "PARQUET"
    CREATED = "CREATED"

LinksFile = namedtuple('LinksFile', ['links','file_name','year','month','status'])
