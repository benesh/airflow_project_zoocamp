
from abc import ABC
import yaml
from enum import Enum
from pydantic import ValidationError
from handler_services.db_postgres_services.utils_db_instance import ConfigPostgres
from handler_services.storage_services.config_storage import MinioCredential
from yaml import safe_load
import json
from typing import Optional

class ReaderFile(ABC):
    file_opened = None
    def open_file(self,path):
        try:
            self.file_opened = open(path,'r')
        except FileNotFoundError as e:
            print(f'File {path} not found : {e}')
        except PermissionError as e:
            print(f'Permission denied: {e}')
        except Exception as e:
            print(f'Unexpected error: {e}')
    def read_file(self,path:str ) -> dict:
        raise NotImplementedError
    def __exit__(self):
        self.file_opened.close()

class ReaderJsonFile(ReaderFile):
    def read_file(self,path:str) -> dict:
        self.open_file(path)
        json_conf = json.load(self.file_opened.read())
        self.__exit__()
        return json_conf
class ReaderYamlFile(ReaderFile):
    def read_file(self,path:str ) -> dict:
        self.open_file(path)
        yaml_conf = safe_load(self.file_opened)
        self.__exit__()
        return yaml_conf

class FactoryReaderFile(Enum):
    JSON ="json"
    YAML = "yaml"

    @property
    def reader(self)-> ReaderFile:
        return {
            self.JSON : ReaderJsonFile(),
            self.YAML : ReaderYamlFile()
        }[self]
def run_reader_file(reader_config : FactoryReaderFile,path_file)->dict:
    reader = reader_config.reader
    data = reader.read_file( path_file )
    return data

class ReaderConfig(ABC):
    def read_config(self, path_config :str, reader_file :FactoryReaderFile, attribut :Optional[str]):
        raise NotImplementedError

class ReaderConfigMinio(ReaderConfig):
    def read_config(self,path_config :str,reader_file :FactoryReaderFile, attribut :Optional[str]=None):
        try:
            data = run_reader_file(reader_file,path_config)
            if attribut is not None: data = data[attribut]
            minio = MinioCredential.parse_obj(data)
            return minio
        except ValidationError as e:
            print(f"Error parsing YAML: {e}")

class ReaderConfigPostgress(ReaderConfig):
    def read_config(self,path_config : str,reader_file:FactoryReaderFile,attribut :Optional[str]=None):
        try:
            data = run_reader_file(reader_file,path_config)
            if attribut is not None: data = data[attribut]
            conf_postgres = ConfigPostgres.parse_obj(data)
            return conf_postgres
        except ValidationError as e:
            print(f"Error parsing JSON: {e}")
class FactoryReaderConfig(Enum):
    CONFIG_POSTGRES="postgres"
    CONFIG_MINIO="minio"
    @property
    def config(self) -> ReaderConfig:
        return {
            self.CONFIG_POSTGRES: ReaderConfigPostgress(),
            self.CONFIG_MINIO:ReaderConfigMinio()
        }[self]

def runner_read_config(reader_config:FactoryReaderConfig,
                       reader_file:FactoryReaderFile,
                       path_config:str,
                       attribut: Optional[str]=None):
    reader = reader_config.config
    conf = reader.read_config(path_config,reader_file,attribut)
    return conf