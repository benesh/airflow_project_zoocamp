import logging
import os
import shutil
from abc import ABC, abstractmethod
from typing import Optional, List
from pandas import DataFrame
from enum import Enum
import pandas as pd
from pydantic import BaseModel
from handler_services.data_bike_services.data_file_info import DataBikeUrlsClass
from settings import WORKDIR_DATA, key_path_to_extract, key_path_to_file, list_folder_to_remove_defautl, \
    columns_to_datetime
from handler_services.interfaces.data_transformer import FileTransformer


class UnzipTransformer(FileTransformer):
    def run(self,data_byke,config:Optional[dict]=None) :
        try:
            shutil.unpack_archive(filename= config[key_path_to_file] , extract_dir= config[key_path_to_extract] )
            logging.info(f'list of extracted files : {os.listdir(config[key_path_to_extract])}')
        except Exception as e:
            logging.error(f'Unexpected Error during Unziping : {e}')

class CleanDirectoryTransformer(FileTransformer):
    def run(self, data_bike:DataBikeUrlsClass, config:Optional[dict]=None):
        #remove list folders default extract __MACOSX
        self.delete_extra_dir(data_bike, config)
        #remove other file in the out of the monthly folder
        if data_bike.month is None:
            self.delete_files_out_of_folder(data_bike, config)

    def delete_files_out_of_folder(self, data_bike:DataBikeUrlsClass, config:Optional[dict]=None):
        # Get the folder of files
        list_element_extracted =  get_list_dir(config[key_path_to_extract])
        # Get the folder of data bike yearly
        # dir_data_bike = [data_dir for data_dir in list_element_extracted if data_byke.year in data_dir ][0]
        folder_of_data_bike = next(filter(lambda element: data_bike.year in element, list_element_extracted), None)
        print(folder_of_data_bike)
        list_file_out_of_folder = get_list_files(f'{config[key_path_to_extract]}/{folder_of_data_bike}')
        try:
            for elem in list_file_out_of_folder:
                os.remove(f"{config[key_path_to_extract]}/{folder_of_data_bike}/{elem}")
                logging.info(f'File Successfully Deleted {elem}')
        except FileNotFoundError as e: logging.error(f'Unexpected Error during Deleting files : {e}')
        except OSError as e: logging.error(f'Unexpected Error during Deleting files : {e}')
        except Exception as e: logging.error(f'Unexpected Error during Deleting files : {e}')

    def delete_extra_dir(self, data_bike:DataBikeUrlsClass, config:Optional[dict]=None):
        list_dir_in_extract = get_list_dir(f'{config[key_path_to_extract]}')
        for dir_extrat in list_dir_in_extract:
            try:
                if data_bike.year not in dir_extrat:
                    shutil.rmtree(f'{config[key_path_to_extract]}/{dir_extrat}')
                    logging.info(f'Deleted Successfully {dir_extrat}')
            except FileNotFoundError as e: logging.error(f'Unexpected Error during Deleting files : {e}')
            except OSError as e: logging.error(f'Unexpected Error during Deleting files : {e}')
            except Exception as e: logging.error(f'Unexpected Error during Deleting files : {e}')

class RestructuredFolderFiles(FileTransformer):
    def run(self, data_bike:DataBikeUrlsClass, config:Optional[dict]=None):
        if data_bike.month is not None:
            self.recreat_folder_month_year(data_bike, config)
        else:
            self.rename_folder_year(data_bike, config)
    def recreat_folder_month_year(self, data_bike:DataBikeUrlsClass, config:Optional[dict]=None):
        month=int(data_bike.month)
        for data_file in os.listdir(f'{config[key_path_to_extract]}'):
            try:
                if not os.path.exists(f'{config[key_path_to_extract]}/{data_bike.year}/{month:02}'):
                    os.makedirs(f'{config[key_path_to_extract]}/{data_bike.year}/{month:02}')
                shutil.move(src=f'{config[key_path_to_extract]}/{data_file}',
                            dst=f'{config[key_path_to_extract]}/{data_bike.year}/{month:02}/{data_file}')
                logging.info(f'the file {data_file} successfully muved to  {data_bike.year}/{month:02}')
            except Exception as e:
                logging.error(f'File {data_file}, Unexpected Error during Recreating folder : {e}')
    def rename_folder_year(self, data_bike:DataBikeUrlsClass, config:Optional[dict]=None):
        list_dir_in_extract = os.listdir(f'{config[key_path_to_extract]}')
        try:
            shutil.move(src=f'{config[key_path_to_extract]}/{list_dir_in_extract[0]}'
                        ,dst=f'{config[key_path_to_extract]}/{data_bike.year}')
        except Exception as e: logging.error(f'Unexpected Error during Renaming folder : {e}')

class FactoryTransformerFile(Enum):
    UNZIP ='UNZIP_FILE'
    CLEAN_DIR = 'CLEAN_DIR'
    RESTRUCTURE_FOLDER='RESTRUCTURE_FOLDER'
    @property
    def get_transformer_file(self) -> FileTransformer:
        return {
            self.UNZIP : UnzipTransformer(),
            self.CLEAN_DIR : CleanDirectoryTransformer(),
            self.RESTRUCTURE_FOLDER : RestructuredFolderFiles()
        }[self]

class TransformerFileObject(BaseModel):
    transformer_file: FactoryTransformerFile
    config:dict

def runner_transformer_file(data_byke:DataBikeUrlsClass, catalogue_transformer:List[TransformerFileObject]):
    for transformer in catalogue_transformer:
        transform = transformer.transformer_file.get_transformer_file
        transform.run(data_byke,transformer.config)

def get_list_dir(path:str) -> List[str]:
    list_elem = os.listdir(path)
    list_dir = [elem for elem in list_elem if os.path.isdir(f'{path}/{elem}') ]
    return list_dir

def get_list_files(path:str) -> List[str]:
    list_elem = os.listdir(path)
    list_file = [elem for elem in list_elem if os.path.isfile(f'{path}/{elem}')]
    return list_file

def reader_csv_file_for_dataframe(path_dir:str) -> DataFrame:
    try:
        list_file_to_read = get_list_files(path_dir)
        list_dataframe = [pd.read_csv(f'{path_dir}/{path_file}') for path_file in list_file_to_read]
        df = pd.concat(list_dataframe)
        logging.info(f'Files in {path_dir} successfully read')
        return df
    except Exception as e:
        logging.error(f'Unexpected Error during Reading csv file : {e}')

def writer_dataframe_to_parquet(df:DataFrame,path:str,filename:str) -> None:
    try:
        if not os.path.exists(path):
            os.makedirs(path)
        df.to_parquet(path=f'{path}/{filename}',index=False)
    except Exception as e:
        logging.error(f'Failed to write dataframe to parquet {path}: {e}')