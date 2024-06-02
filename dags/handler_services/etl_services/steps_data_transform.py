import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, List

import numpy as np
from pydantic import BaseModel
from settings import key_column_renanme, key_col_to_datetime, key_column_to_drop, key_column_to_string, \
    key_column_gender, key_column_rideable_type, key_column_member
from handler_services.data_bike_services.data_file_info import DataBikeUrlsClass
import pandas as pd
from pandas import DataFrame
from handler_services.interfaces.data_transformer import DataTransformer


class RenameColumTransformer(DataTransformer):
    def run(self, data:DataFrame ,config:dict) -> DataFrame:
        dick_columns_to_rename_col = config[key_column_renanme]
        data = data.rename(columns=dick_columns_to_rename_col)
        logging.info("in Renaming data frame")
        return data
class ConvertColunmToString(DataTransformer):
    def run(self, data:DataFrame ,config:dict) -> DataFrame:
        list_columns_to_string = config[key_column_to_string]
        for column in list_columns_to_string:
            data[column] = data[column].apply(str)
        return data
class ConvertDataToDateTimeTransformer(DataTransformer):
    def run(self,data:DataFrame ,config:dict ) -> DataFrame:
        list_column_to_convert_col = config[key_col_to_datetime]
        for column in list_column_to_convert_col:
            data[column] = pd.to_datetime(data[column])
        return data
class DropColumnsTransformer(DataTransformer):
    def run(self,data:DataFrame ,config:dict) -> DataFrame:
        try:
            # get the columns that really exist in dataframe
            list_data_from_list = config[key_column_to_drop]
            list_column_to_drop = [ column for column in data.columns if column in list_data_from_list]
            if list_column_to_drop is not None:
                data = data.drop(columns= list_column_to_drop,axis= 1)
                logging.info(f'Successfully dropped columns: {list_column_to_drop}')
            return data
        except KeyError as e:
            logging.exception(f'Key column {config[key_column_to_drop]} not found in data: {e} ')
        except Exception as e:
            logging.exception(f'Failed to drop columns: {e}')

class GenderTransformer(DataTransformer):
    def run(self,data:DataFrame ,config:dict) -> DataFrame:
        if config[key_column_gender] in data.columns:
            data[config[key_column_gender]] = data[config[key_column_gender]].map(self.gender_transform)
        else:
            data[config[key_column_gender]] = 'UNKNOW'
        return data
    def gender_transform(self,gender:int):
        if gender == 2:
            return 'FEMME'
        elif gender == 1:
            return 'HOMME'
        else:
            return 'UNKNOWN'

class BikeTypeTransformer(DataTransformer):
    def run(self,data:DataFrame ,config:dict) -> DataFrame:
        if not config[key_column_rideable_type] in data.columns:
            data[config[key_column_rideable_type]] = 'classic_bike'
        return data

class MemberTransformer(DataTransformer):
    def run(self,data:DataFrame ,config:dict) -> DataFrame:
        if config[key_column_member] in data.columns:
            data[config[key_column_member]] = data[config[key_column_member]].map(self.member_transform)
        return data
    def member_transform(self,member:str):
        if member == 'Customer':
            return 'casual'
        elif member == 'Subscriber':
            return 'member'
        return member
class FactoryDataTransformer(Enum):
    RENAME_COL='rename_column'
    CONVERT_TO_DATETIME='convert_to_datetime'
    DROP_COLUMNS='drop_columns'
    CONVERT_TO_STRING='convert_to_string'
    GENDER_TRANSFORMER = 'gender_transformer'
    BIKE_TYPE_TRANSFORMER = 'bike_type_transformer'
    MEMBER_TRANSFORMER = 'member_new_format'

    @property
    def get_data_tranformer(self):
        return {
            self.RENAME_COL:RenameColumTransformer(),
            self.CONVERT_TO_DATETIME:ConvertDataToDateTimeTransformer(),
            self.DROP_COLUMNS:DropColumnsTransformer(),
            self.CONVERT_TO_STRING:ConvertColunmToString(),
            self.GENDER_TRANSFORMER:GenderTransformer(),
            self.BIKE_TYPE_TRANSFORMER : BikeTypeTransformer(),
            self.MEMBER_TRANSFORMER : MemberTransformer()
        }[self]
class DataTransformerObject(BaseModel):
    transformer : FactoryDataTransformer
    config:dict
def runner_transformer_data(catalogue_data_transformer:List[DataTransformerObject], data:DataFrame):
    for element in catalogue_data_transformer:
        transformer = element.transformer.get_data_tranformer
        data = transformer.run(data=data,config=element.config)
    return data

