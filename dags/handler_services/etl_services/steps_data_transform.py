from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel
from settings import key_column_renanme,key_col_to_datetime
from handler_services.data_byke_services.data_file_info import DataBikeUrlsClass
import pandas as pd
from pandas import DataFrame

class DataTransformer:
    @abstractmethod
    def run(self, data_bike:DataBikeUrlsClass,
            config:Optional[dict]=None,
            data:Optional[DataFrame]=None)-> DataFrame:
        ...
class RenameColumTransformer(DataTransformer):
    def run(self, data_bike: DataBikeUrlsClass, config: Optional[dict] = None, data: Optional[DataFrame] = None) -> DataFrame:
        list_column_to_rename_col = config[key_column_renanme]
        data.rename(columns=list_column_to_rename_col,inplace=True)
        return data
class ConvertDataToDateTimeTransformer(DataTransformer):
    def run(self, data_bike: DataBikeUrlsClass, config: Optional[dict] = None, data: Optional[DataFrame] = None) -> DataFrame:
        list_column_to_convert_col = config[key_col_to_datetime]
        for col in list_column_to_convert_col:
            data[col] = pd.to_datetime(data[col])
        return data

class FactoryDataTransformer(Enum):
    RENAME_COL='rename_col'
    CONVERT_TO_DATETIME='convert_to_datetime'
    @property
    def get_data_tranformer(self):
        return {
            self.RENAME_COL:RenameColumTransformer(),
            self.CONVERT_TO_DATETIME:ConvertDataToDateTimeTransformer()
        }[self]
class Transformer(BaseModel):
    transformer : FactoryDataTransformer
    config:dict
def runner_transformer_data(data_bike : DataBikeUrlsClass,
                            catalogue_transformer:List[Transformer],
                            data:Optional[DataFrame]=None):
    for element in catalogue_transformer:
        transformer = element.transformer.get_data_tranformer
        data = transformer.run(data_bike=data_bike, config=element.config, data=data)
    return data

