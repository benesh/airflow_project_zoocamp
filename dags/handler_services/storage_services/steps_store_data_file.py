import logging
from typing import Optional
from handler_services.storage_services.config_storage import MinioCredential
from handler_services.data_byke_services.data_file_info import DataBykeUrlsClass,StatusFile
import requests
from abc import ABC

class StepsDataSinkFileFromWeb:
    def __init__(self, data_bykes:[DataBykeUrlsClass], minio_credential:MinioCredential, bucket_name:str, path:Optional[str]):
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
    def run_upload_on_data(self,data_byke:DataBykeUrlsClass):
        try :
            response_head = requests.head(data_byke.url)
            file_size = int(response_head.headers.get('Content-Length', 0))
            with requests.get(data_byke.url, stream=True) as response:
                # Ensure successful response
                response.raise_for_status()
                # Upload the file-like object directly
                logging.info(f'Uploading file {data_byke.file_name}')
                self.minio_creds.get_minio_client().put_object(self.bucket_name,data_byke.file_name, response.raw, length=file_size)
            data_byke.set_status(StatusFile.UPLOADED_FILE)
        except Exception as e:
            logging.error(f"Unexpected Error with the file {data_byke.file_name} explication: {e}")
            data_byke.set_status(StatusFile.ERROR_UPLOADING)
        data_byke.update_date()
        return data_byke


class DataBykeUrlStorageServices(ABC):
    def run(self, data_bykes:[DataBykeUrlsClass], config:dict):
        raise NotImplementedError

class StepGetDataFromMinioS3(DataBykeUrlStorageServices):
    def run(self,data_byke :DataBykeUrlsClass, config:dict):
        minio_credential:MinioCredential = config.get["minio_credential"]
        bucket_name:str = config["bucket_name"]
        path:str = config["path"]
        ...

