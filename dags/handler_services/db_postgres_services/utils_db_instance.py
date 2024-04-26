
from pydantic import BaseModel

class ConfigPostgres(BaseModel):
    config_name:str
    user:str
    password:str
    hostname:str
    port : str
    database:str

