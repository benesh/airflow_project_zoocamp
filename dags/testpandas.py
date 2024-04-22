from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from handler_services.webparser_services.webparser_services import  WebDriverSetup, BykeDataPage
from handler_services.db_postgres_services.dbinstance import DBInstance
from handler_services.reader_config import FactoryConfig,FactoryReaderFile
from settings import POSTGRES_MINIO_FILE,POSTGRES_ATTRIBUT,URL_DATA_DYKE
from handler_services.data_file_info import DataBykeUrlsClass
from handler_services.db_postgres_services.steps import StepsGetAllUrlDataByke,StepsInsertBykeUrls
from handler_services.reader_config import runner_read_config , run_reader_file , ReaderYamlFile
from yaml import safe_load
from handler_services.utils_byke_data import filter_links
import json


# conf = runner_read_config(reader_config=FactoryConfig.CONFIG_POSTGRES,reader_file=FactoryReaderFile.YAML,path_config=POSTGRES_MINIO_FILE,attribut=POSTGRES_ATTRIBUT)
# reader = ReaderYamlFile()
# data = reader.read_file(POSTGRES_MINIO_FILE)
# # print(POSTGRES_MINIO_FILE)
#
# file = open('dags/conf/conf2.yaml','r')
# data = safe_load(file)
# print(data)

from_web = ['{"file_name":"2013-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2013-citibike-tripdata.zip","month":null,"year":"2013","status":"CREATED"}', '{"file_name":"2014-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2014-citibike-tripdata.zip","month":null,"year":"2014","status":"CREATED","create_at":"2024-04-21T14:13:06.007826","update_at":"2024-04-21T14:13:06.007826"}', '{"file_name":"2015-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2015-citibike-tripdata.zip","month":null,"year":"2015","status":"CREATED","create_at":"2024-04-21T14:13:06.007848","update_at":"2024-04-21T14:13:06.007848"}', '{"file_name":"2016-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2016-citibike-tripdata.zip","month":null,"year":"2016","status":"CREATED","create_at":"2024-04-21T14:13:06.007856","update_at":"2024-04-21T14:13:06.007856"}', '{"file_name":"2017-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2017-citibike-tripdata.zip","month":null,"year":"2017","status":"CREATED","create_at":"2024-04-21T14:13:06.007863","update_at":"2024-04-21T14:13:06.007863"}', '{"file_name":"2018-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2018-citibike-tripdata.zip","month":null,"year":"2018","status":"CREATED","create_at":"2024-04-21T14:13:06.007871","update_at":"2024-04-21T14:13:06.007871"}', '{"file_name":"2019-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2019-citibike-tripdata.zip","month":null,"year":"2019","status":"CREATED","create_at":"2024-04-21T14:13:06.007878","update_at":"2024-04-21T14:13:06.007878"}', '{"file_name":"2020-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2020-citibike-tripdata.zip","month":null,"year":"2020","status":"CREATED","create_at":"2024-04-21T14:13:06.007885","update_at":"2024-04-21T14:13:06.007885"}', '{"file_name":"2021-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2021-citibike-tripdata.zip","month":null,"year":"2021","status":"CREATED","create_at":"2024-04-21T14:13:06.007892","update_at":"2024-04-21T14:13:06.007892"}', '{"file_name":"2022-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2022-citibike-tripdata.zip","month":null,"year":"2022","status":"CREATED","create_at":"2024-04-21T14:13:06.007899","update_at":"2024-04-21T14:13:06.007899"}', '{"file_name":"2023-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2023-citibike-tripdata.zip","month":null,"year":"2023","status":"CREATED","create_at":"2024-04-21T14:13:06.007907","update_at":"2024-04-21T14:13:06.007907"}', '{"file_name":"202401-citibike-tripdata.csv.zip","url":"https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.csv.zip","month":"202401","year":"2024","status":"CREATED","create_at":"2024-04-21T14:13:06.007915","update_at":"2024-04-21T14:13:06.007915"}', '{"file_name":"202402-citibike-tripdata.csv.zip","url":"https://s3.amazonaws.com/tripdata/202402-citibike-tripdata.csv.zip","month":"202402","year":"2024","status":"CREATED","create_at":"2024-04-21T14:13:06.007925","update_at":"2024-04-21T14:13:06.007925"}', '{"file_name":"202403-citibike-tripdata.csv.zip","url":"https://s3.amazonaws.com/tripdata/202403-citibike-tripdata.csv.zip","month":"202403","year":"2024","status":"CREATED","create_at":"2024-04-21T14:13:06.007932","update_at":"2024-04-21T14:13:06.007932"}']
from_web1 = from_web[1]
databyke_dict = from_web[1]
print(json.loads(databyke_dict))

data = DataBykeUrlsClass.parse_obj( json.loads(from_web1))
# # data = [DataBykeUrlsClass.from_json(link) for link in from_web1]
# # data_sub = [data[0],data[1],data[2],data[3],data[4],data[5]]
#
# # data_filtered = filter_links(data,data_sub)
print(data)
#
# from pydantic import BaseModel
#
# from pydantic.dataclasses import dataclass
# import json
#
#
# class Datatest(BaseModel):
#     file_name:str
#     update_day : datetime
#     age : int
#     status:str
# #
# # databis = Datatest('file_test_name',datetime.now(),12,'CREATED')
# # print(databis)
#
# dict_data = {
#     'file_name': 'file_test_name_1',
#     'update_day': '2011-11-04T00:05:23',
#     'age': 13,
#     'status': 'CREATED'}
#
# # test_dict = Datatest(databis.file_name,databis.update_day,databis.age,databis.status)
# test1 = Datatest.parse_obj(dict_data)
#
# json_data = test1.model_dump_json()
#
# print(json_data)
# test2 = Datatest.parse_obj(json.loads(json_data))
# print(test2)
#
# # test3 = Datatest.parse_obj('file_test_name','2011-11-04T00:05:23',12,'CREATED')
# test3 = Datatest(file_name='file_test_name_2',update_day='2011-11-04T00:05:23',age=18,status='CREATED')
# print(test3)