import logging
import os
import shutil
from abc import ABC, abstractmethod
from typing import Optional, List
from pandas import DataFrame
from enum import Enum
import pandas as pd

from pydantic import BaseModel
# from settings import
from handler_services.data_byke_services.data_file_info import DataBikeUrlsClass
from settings import WORKDIR_DATA, key_path_to_extract, key_path_to_file, list_folder_to_remove_defautl


class TransformerFile(ABC):
    @abstractmethod
    def run(self, data_byke:DataBikeUrlsClass, config:Optional[dict]=None) -> str:
        raise NotImplementedError

class UnzipTransformer(TransformerFile):
    def run(self,data_byke,config:Optional[dict]=None) :
        try:
            shutil.unpack_archive(filename= config[key_path_to_file] , extract_dir= config[key_path_to_extract] )
            os.listdir(config[key_path_to_extract])

        except Exception as e:
            logging.error(f'Unexpected Error during Unziping : {e}')
class CleanDirectoryTransformer(TransformerFile):
    def run(self, data_byke:DataBikeUrlsClass, config:Optional[dict]=None):
        #remove list folders default extract __MACOSX
        self.delete_extra_dir(data_byke,config)

        #remove other file in the out of the monthly folder
        if data_byke.month is None:
            self.delete_files_out_of_folder(data_byke,config)

    def delete_files_out_of_folder(self, data_byke:DataBikeUrlsClass, config:Optional[dict]=None):
        list_element_extracted = os.listdir(config[key_path_to_file])
        # Get the folder of data bike yearly
        folder_of_data_bike = next(filter(lambda element: data_byke.year in element, list_element_extracted), None)
        list_elem_in_folder_data = os.listdir(f'{config[key_path_to_extract]}/{folder_of_data_bike}')
        try:
            for elem in list_elem_in_folder_data:
                if os.path.isfile(f"{config[key_path_to_file]}/{folder_of_data_bike}/{elem}"):
                    os.remove(f"{config[key_path_to_file]}/{folder_of_data_bike}/{elem}")
                    logging.info(f'Deleted Successfully {elem}')
        except FileNotFoundError as e: logging.error(f'Unexpected Error during Deleting files : {e}')
        except OSError as e: logging.error(f'Unexpected Error during Deleting files : {e}')
        except Exception as e: logging.error(f'Unexpected Error during Deleting files : {e}')

    def delete_extra_dir(self, data_byke:DataBikeUrlsClass, config:Optional[dict]=None):
        list_dir_in_extract = os.listdir(f'{config[key_path_to_extract]}')

        for folder in list_dir_in_extract:
            try:
                if data_byke.year not in folder:
                    shutil.rmtree(f'{config[key_path_to_extract]}/{folder}')
            except FileNotFoundError as e: logging.error(f'Unexpected Error during Deleting files : {e}')
            except OSError as e: logging.error(f'Unexpected Error during Deleting files : {e}')
            except Exception as e: logging.error(f'Unexpected Error during Deleting files : {e}')

class DataRestructuredMonthly(TransformerFile):
    def run(self, data_byke:DataBikeUrlsClass, config:Optional[dict]=None):
        if data_byke.month is not None:
            self.recreat_folder_month_year(data_byke,config)
        else:
            self.rename_folder_year(data_byke,config)
    def recreat_folder_month_year(self, data_bike:DataBikeUrlsClass, config:Optional[dict]=None):
        for data_file in os.listdir(f'{config[key_path_to_extract]}'):
            try:
                shutil.move(src=f'{config[key_path_to_extract]}/{data_file}',
                            dst=f'{config[key_path_to_extract]}/{data_bike.year}/{data_bike.month:02}/{data_file}')
            except Exception as e:
                logging.error(f'Unexpected Error during Recreating folder : {e}')
    def rename_folder_year(self, data_bike:DataBikeUrlsClass, config:Optional[dict]=None):
        list_dir_in_extract = os.listdir(f'{config[key_path_to_extract]}')
        shutil.move(src=f'{config[key_path_to_extract]}/{list_dir_in_extract[0]}'
                    ,dst=f'{config[key_path_to_extract]}/{data_bike.year}')



class FactoryTransformerFile(Enum):
    UNZIP ='UNZIP_FILE'
    CLEAN_DIR = 'CLEAN_DIR'

    @property
    def get_transformer_file(self) -> TransformerFile:
        return {
            self.UNZIP : UnzipTransformer(),
            self.CLEAN_DIR : CleanDirectoryTransformer()
        }[self]

class TransformerFileJson(BaseModel):
    transformer_file: FactoryTransformerFile
    config:dict

def runner_transformer_file(data_byke:DataBikeUrlsClass, catalogue_transformer:List[TransformerFileJson]):
    for transformer in catalogue_transformer:
        transform = transformer.transformer_file.get_transformer_file
        transform.run(data_byke,transformer.config)
def get_list_dir(path:str) -> List[str]:
    list_elem = os.listdir(path)
    list_dir = [elem for elem in list_elem if os.path.isdir(f'{path}/{elem}') ]
    return list_dir

def get_list_files(path:str) -> List[str]:
    list_elem = os.listdir(path)
    list_file = [elem for elem in list_elem if not os.path.isdir(f'{path}/{elem}')]
    return list_file

def reader_csv_file_for_dataframe(path_dir:str) -> DataFrame:
    list_file_to_read = get_list_files(path_dir)
    list_dataframe = [pd.read_csv(f'{path_dir}/{path_file}') for path_file in list_file_to_read]
    df = pd.concat(list_dataframe)
    return df

def writer_dataframe_to_parquet(df:DataFrame,path:str) -> DataFrame:
    try:
        df.to_parquet(path)
        return df
    except Exception as e:
        logging.exception(f'Failed to write dataframe to parquet {path}: {e}')

