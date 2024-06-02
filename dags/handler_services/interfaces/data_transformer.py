from abc import ABC, abstractmethod
from handler_services.data_bike_services.data_file_info import DataBikeUrlsClass
from typing import Optional
from pandas import DataFrame


class FileTransformer(ABC):
    @abstractmethod
    def run(self, data_byke:DataBikeUrlsClass, config:Optional[dict]=None) -> str:
        raise NotImplementedError
class DataTransformer(ABC):
    @abstractmethod
    def run(self,data:DataFrame,config:dict) -> DataFrame:
        raise NotImplementedError