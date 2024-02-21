from sqlalchemy import create_engine, Integer
from data_file_info import Config
import yaml
import os


class DBInstance:
    def __init__(self,path_config="/opt/airflow/dags/configPostgress.yaml"):
        # file_path = os.path.abspath(path_config) "/opt/airflow/dags/configPostgress.yaml"
        with open(path_config, "r") as file:
            yaml_data = yaml.safe_load(file)
            self.conf_list = [Config(**conf) for conf in yaml_data]

    def get_engine(self):
        conf : Config = self.conf_list[0]
        return create_engine(f'postgresql://{conf.user}:{conf.password}@{conf.hostname}:{conf.port}/{conf.database}')
    def get_session(self):

