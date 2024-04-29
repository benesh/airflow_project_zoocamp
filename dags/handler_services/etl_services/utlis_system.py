import shutil
from abc import ABC, abstractmethod
from typing import Optional, List
from pandas import DataFrame
from enum import Enum

from pydantic import BaseModel

from handler_services.data_byke_services.data_file_info import DataBykeUrlsClass
from settings import WORKDIR_DATA


class TransformerFile(ABC):
    @abstractmethod
    def run(self,data_byke:DataBykeUrlsClass,config:Optional[dict]=None) -> str:
        raise NotImplementedError

class UnzipTransformer(TransformerFile):
    def run(self,data_byke,config:Optional[dict]=None) :
        shutil.unpack_archive(filename= config['file_path'] , extract_dir= config['destination'] )
class CleanDirectoryTransformer(TransformerFile):
    def run(self,data_byke:DataBykeUrlsClass,config:Optional[dict]=None):
        for directory in config['dir_to_rm'] :  shutil.rmtree(directory)
        ...

class FactoryTransformerFile(Enum):
    UNZIP ='UNZIP_FILE'
    CLEANDIR = CleanDirectoryTransformer()

    @property
    def getTransformer(self) -> TransformerFile:
        return {
            self.UNZIP : UnzipTransformer(),
            self.CLEANDIR : CleanDirectoryTransformer()
        }[self]

class TransformerFileJson(BaseModel):
    transformer_file: FactoryTransformerFile
    config:dict

def runner_transformer(data_byke:DataBykeUrlsClass,catalogue_transformer:List[TransformerFileJson]):
    for transformer in catalogue_transformer:
        transform = transformer.transformer_file.getTransformer
        transform.run(data_byke,transformer.config)
