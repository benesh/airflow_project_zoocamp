import sys
sys.path.append('/opt/airflow/dags')
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from handler_services.reader_config import runner_read_config ,FactoryReaderConfig , FactoryReaderFile
from handler_services.db_postgres_services.utils_db_instance import ConfigPostgres
from typing import Optional


Base = declarative_base()

class DBInstance:
    def __init__(self, reader_config:FactoryReaderConfig, reader_file : FactoryReaderFile, path_config, attribut:Optional[str]):
        self._config :ConfigPostgres = None
        self._engine =None
        self.path_config = path_config
        self.reader_conf = reader_config
        self.reader_file = reader_file
        self.attribut = attribut

    @property
    def config(self):
        if self._config is None:
            self._config = runner_read_config(reader_config=self.reader_conf,
                                              reader_file=self.reader_file ,
                                              path_config= self.path_config ,
                                              attribut=self.attribut)
        return self._config

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(f'postgresql://{self.config.user}:{self.config.password}@{self.config.hostname}:{self.config.port}/{self.config.database}')
        return self._engine
    def init_database(self):
        from handler_services.db_postgres_services import  models
        Base.metadata.create_all(self.engine)