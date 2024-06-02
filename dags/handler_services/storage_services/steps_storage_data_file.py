import logging
import os
from typing import Optional

from minio import Minio

from handler_services.storage_services.config_storage import MinioCredential
from handler_services.data_bike_services.data_file_info import DataBikeUrlsClass,StatusFile
import requests
from abc import ABC

from settings import key_minio_credential, key_bucket_name, key_path_file, key_path_to_extract, key_path_to_dest, \
    key_path


# class Step
class StepsDataSinkFileFromWeb:
    def __init__(self, data_bykes:[DataBikeUrlsClass], minio_credential:MinioCredential, bucket_name:str, path:Optional[str]):
        self.data_bykes_list = data_bykes
        self.minio_creds = minio_credential
        self.bucket_name = bucket_name
        self.path = path
    def run(self):
        list_result_data_sink=[]
        try:
            for data_byke in self.data_bykes_list:
                list_result_data_sink.append( self.run_upload_on_data(data_byke))
        except Exception as e:
            logging.error(f"Unexpected Error in Uploading File: {e}")
        return list_result_data_sink
    def run_upload_on_data(self, data_byke:DataBikeUrlsClass):
        try :
            response_head = requests.head(data_byke.url)
            file_size = int(response_head.headers.get('Content-Length', 0))
            with requests.get(data_byke.url, stream=True) as response:
                # Ensure successful response
                response.raise_for_status()
                # Upload the file-like object directly
                logging.info(f'Uploading file {data_byke.file_name}')
                self.minio_creds.get_minio_client().put_object(self.bucket_name,data_byke.file_name, response.raw, length=file_size)
                self.minio_creds.get_minio_client().fput_object()
            data_byke.set_status(StatusFile.UPLOADED_FILE)
        except Exception as e:
            logging.error(f"Unexpected Error with the file {data_byke.file_name} explication: {e}")
            data_byke.set_status(StatusFile.ERROR_UPLOADING)
        data_byke.update_date()
        return data_byke
class DataBikeUrlStorageServices(ABC):
    def run(self, data_bykes:[DataBikeUrlsClass], config:dict):
        raise NotImplementedError
class StepGetDataFromMinioS3(DataBikeUrlStorageServices):
    def run(self, data_bike :DataBikeUrlsClass, config:dict):
        minio_config:MinioCredential = config[key_minio_credential]
        try:
            minio_cred = minio_config.get_minio_client()
            minio_cred.fget_object(bucket_name = config[key_bucket_name],
                                   # object_name = f'{config[key_path_file]}/{data_bike.file_name}',
                                   object_name = f'{data_bike.file_name}',
                                   file_path = f'{config[key_path_to_dest]}/{data_bike.file_name}')
                                   # file_path = f'{data_bike.file_name}')
            logging.info(f'File {data_bike.file_name} Successfully download from minio')
        except Exception as e:
            logging.error(f' Unexpected Error in get_minio_client: {e}')

class StepDataUploadToMinioS3(DataBikeUrlStorageServices):
    def run(self, data_bike :DataBikeUrlsClass, config:dict):
        minio_cred = config[key_minio_credential].get_minio_client()
        self.upload_file(path_file=config[key_path],minio_cred=minio_cred,bucket_name=config[key_bucket_name])
    def upload_file(self, path_file:str, minio_cred, bucket_name:str, prefix_file_object:str=None):
        list_elem = os.listdir(path_file)
        for elem in list_elem:
            if os.path.isfile(f'{path_file}/{elem}'):
                minio_cred.fput_object(bucket_name=bucket_name,
                                       object_name=f'{prefix_file_object or ""}/{elem}',
                                       file_path=f'{path_file}/{elem}')
                logging.info(f'File {elem} successfully uploaded to minio')
            else:
                self.upload_file(f'{path_file}/{elem}',minio_cred,bucket_name,elem)