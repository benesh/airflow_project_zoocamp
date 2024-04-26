from minio import Minio
import requests
from handler_services.reader_config import runner_read_config,FactoryReaderConfig,FactoryReaderFile
from handler_services.storage_services.config_storage import MinioCredential


class MinioServices:
    def __init__(self, reader_config:FactoryReaderConfig, reader_file:FactoryReaderFile, path_config:str, attrubute_config:str):
        self.reader_config: FactoryReaderConfig = reader_config
        self.reader_file: FactoryReaderFile = reader_file
        self.path_config: str = path_config
        self.attribute_config = attrubute_config
        self._minio_credentials : MinioCredential = None

    def get_minio_client(self)-> Minio:
        return self.minio_credential.get_minio_client()
    @property
    def minio_credential(self):
        if self._minio_credentials is None:
            self._minio_credentials = runner_read_config(reader_config=self.reader_config,
                                                         reader_file=self.reader_file,
                                                         path_config=self.path_config,
                                                         attribut=self.attribute_config)
        return self._minio_credentials


# def wondload_data_file_into_cloud(minio_client : Minio,file_data: DataBykeUrlsClass, bucket_name : str,bucket_path:str):
#     response_head = requests.head(file_data.url)
#     file_size = int(response_head.headers.get('Content-Length', 0))
#     with requests.get(file_data.url, stream=True) as response:
#         # Ensure successful response
#         response.raise_for_status()
#         # Upload the file-like object directly
#         minio_client.put_object(bucket_name, file_data.file_name, response.raw, length=file_size)

# if __name__ == "__main__":
#     client = Minio('127.0.0.1:9000', 'cjQ0eMWujedmdqAFcpss', 'twWetAXmR4USG91alyMPDy5pZFQk613j0JAxJxX2', secure=False)
#     bucket_name='python-test-bucket'
#     file_url ='https://s3.amazonaws.com/tripdata/202402-citibike-tripdata.csv.zip'
#     file_name = '2013/2013-citibike-tripdata.zip'
#     response = requests.head(file_url)
#     file_size = int(response.headers.get('Content-Length', 0))
#     with requests.get(file_url, stream=True) as response:
#         # Ensure successful response
#         response.raise_for_status()
#         # Upload the file-like object directly
#         client.put_object(bucket_name, file_name, response.raw, length=file_size)
