import sys
sys.path.append('/opt/airflow/dags')

from abc import ABC
import yaml
from enum import Enum
from pydantic import ValidationError
from dags.handler_services.db_postgres_services.dbinstance import ConfigPostgres
from dags.handler_services.storage_services.minio_services import MinioServices


class ReaderConfig(ABC):
    def read_config(self,path_config : str):
        raise NotImplementedError
class ReaderConfig_Minio(ReaderConfig):
    def read_config(self,path_config : str):
        try:
            with open(path_config, 'r') as stream:
                yaml_data = yaml.safe_load(stream)
            minion_conf = MinioServices(**yaml_data)
            return minion_conf
        except ValidationError as e:
            print(f"Error parsing YAML: {e}")


class ReaderConfig_postgress(ReaderConfig):
    def read_config(self,path_config : str):
        try:
            with open(path_config, 'r') as stream:
                yaml_data = yaml.safe_load(stream)
            postgres_conf = ConfigPostgres(**yaml_data)
            return postgres_conf
        except ValidationError as e:
            print(f"Error parsing YAML: {e}")
class FatcoryConfig(Enum):
    CONFIG_POSTGRES="postgres"
    CONFIG_MINIO="minio"

    @property
    def config(self) -> ReaderConfig:
        return {
            self.CONFIG_POSTGRES: ReaderConfig_postgress(),
            self.CONFIG_MINIO:ReaderConfig_Minio()
        }[self]

def read_config(reader_config : ReaderConfig,path_config : str):
    config = reader_config .read_config(path_config)
    return config

def read_config(path_config:str,index_conf:int):
    #file_path = os.path.abspath(path_config) "/opt/airflow/dags/configPostgress.yaml"
    with open(path_config, "r") as file:
        yaml_data = yaml.safe_load(file)
        conf_list = [ConfigPostgres(**conf) for conf in yaml_data]
        return conf_list[index_conf]
def getconf(key:str, conf_list:list):
    def recursiveIteartion(count:int):
        if len(conf_list) == count + 1:
            return None
        if key in conf_list[count]:
            return conf_list[count].get(key)
        recursiveIteartion(count + 1)
    conf=recursiveIteartion(0)
    return conf