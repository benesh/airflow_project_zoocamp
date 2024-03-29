from minio import Minio
from pydantic.dataclasses import dataclass
from pydantic import BaseModel
from minio.error import S3Error
import requests
from dags.handler_services.data_utlis_version_2 import LinksFile


class MinioServices(BaseModel):
    endpoint:str
    accesskey:str
    secretkey:str

    def get_minio_client(self)-> Minio:
        return Minio(endpoint=self.endpoint,
                     access_key=self.accesskey,
                     secret_key=self.secretkey,
                     secure=False)

def wondload_data_file_into_cloud(minio_client : Minio,file_data: LinksFile, bucket_name : str,bucket_path:str):
    response_head = requests.head(file_data.links)
    file_size = int(response_head.headers.get('Content-Length', 0))
    with requests.get(file_data.links, stream=True) as response:
        # Ensure successful response
        response.raise_for_status()
        # Upload the file-like object directly
        minio_client.put_object(bucket_name, file_data.file_name, response.raw, length=file_size)

#
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
