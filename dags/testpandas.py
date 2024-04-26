import json
import sys

from sqlalchemy.orm import Session

sys.path.append('/opt/airflow/dags')
from handler_services.db_postgres_services.dbinstance import DBInstance
from handler_services.reader_config import FactoryReaderConfig,FactoryReaderFile
from settings import POSTGRES_MINIO_FILE, POSTGRES_ATTRIBUT
from handler_services.data_byke_services.data_file_info import DataBykeUrlsClass
# from handler_services.db_postgres_services.models import DataBykeUrlsDB ,StatusFile



json_dump_data = ['{"file_name":"2013-citibike-tripdata.zip","url":"https://s3.amazonaws.com/tripdata/2013-citibike-tripdata.zip","month":null,"year":"2013","status":"CREATED","created_at":"2024-04-24T20:53:39.144375","updated_at":"2024-04-24T20:53:39.144375"}']
dataclass_data = [DataBykeUrlsClass.parse_obj(json.loads(json_dump)) for json_dump in json_dump_data ]
print(dataclass_data)
#
# db_instance = DBInstance(reader_config=FactoryReaderConfig.CONFIG_POSTGRES,
#                          reader_file=FactoryReaderFile.YAML,
#                          path_config=POSTGRES_MINIO_FILE,
#                          attribut=POSTGRES_ATTRIBUT)


# stmt = (
#     update(user_table).
#     where(user_table.c.id == 5).
#     values(name='user #5')
# )

# print(dataclass_data[0].url)
# with Session(db_instance.engine) as session:
#     session.query(DataBykeUrlsDB).filter_by(url=dataclass_data[0].url).update({DataBykeUrlsDB.status: StatusFile.UPLOADED_FILE})
#     session.commit()
#     session.close()

# Close the session

# s3_minio = MinioServices(reader_config=FactoryReaderConfig.CONFIG_MINIO,
#                          reader_file=FactoryReaderFile.YAML,
#                          path_config='conf/conf2.yaml',
#                          attrubute_config=MINIO_ATTRIBUT)
#
# minio_cred = s3_minio.minio_credential
# minio_cred.get_minio_client()
#
# step_uplaod_run = StepsDataSinkFileFromWeb(data_bykes=dataclass_data,
#                                            minio_credential=s3_minio.minio_credential,
#                                            bucket_name='bucket-test',
#                                            path='path/to/file')
# list_result_1 = step_uplaod_run.run()
# print(list_result_1)
# list_json_dump = [url.model_dump_json() for url in list_result_1]
# print(list_json_dump)

