from abc import ABC, abstractmethod
from collections import namedtuple


class DataUtilsVersion2(ABC):
    ...
class DataTransform(DataUtilsVersion2):
    ...
class DataTransform2(DataUtilsVersion2):
    ...


LinksFile = namedtuple('LinksFile', ['links','file_name','year','month','status'])
