from minio import Minio
from pydantic import BaseModel


class MinioCredential(BaseModel):
    url:str
    accessKey:str
    secretKey:str
    api: str
    path: str
    host:str = None
    port:str = None

    def get_minio_client(self)-> Minio:
        return Minio(endpoint= f'{self.host}:{self.port}',
                     access_key=self.accessKey,
                     secret_key=self.secretKey,
                     secure=False)