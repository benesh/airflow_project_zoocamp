from abc import ABC, abstractmethod
from typing import Optional

from pandas import DataFrame

from handler_services.data_byke_services.data_file_info import DataBykeUrlsClass
class DataTransformer:
    @abstractmethod
    def run(self,data_byke:DataBykeUrlsClass,config:Optional[dict]=None,data:Optional[DataFrame]=None)-> DataFrame:
        ...
class RenameColumTransformer(DataTransformer):
    def run(self, data_byke: DataBykeUrlsClass, config: Optional[dict] = None,data: Optional[DataFrame] = None) -> DataFrame:
        ...

class ConvertToTimestampDataTransformer(DataTransformer):
    def run(self, data_byke: DataBykeUrlsClass, config: Optional[dict] = None, data: Optional[DataFrame] = None) -> DataFrame:
        ...

class CleanDirDataDataTransformer(DataTransformer):
    def run(self, data_byke: DataBykeUrlsClass, config: Optional[dict] = None,data: Optional[DataFrame] = None) -> DataFrame:
        ...

class ReadDataTransformer(DataTransformer):
    def run(self, data_byke: DataBykeUrlsClass, config: Optional[dict] = None,data: Optional[DataFrame] = None) -> DataFrame:
        ...