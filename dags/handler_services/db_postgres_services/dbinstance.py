import sys
sys.path.append('/opt/airflow/dags')

from sqlalchemy import create_engine
from pydantic.dataclasses import dataclass, BaseModel
from pydantic import BaseModel
from handler_services.utils_read_config import Config


class ConfigPostgres(BaseModel):
    config_name:str
    user:str
    password:str
    hostname:str
    port : str
    database:str

class DBInstance:
    def __init__(self,param_config:Config):
        self.config = param_config
        # file_path = os.path.abspath(path_config) "/opt/airflow/dags/configPostgress.yaml"

    def get_engine(self):
        return create_engine(f'postgresql://{self.config.user}:{self.config.password}@{self.config.hostname}:{self.config.port}/{self.config.database}')

