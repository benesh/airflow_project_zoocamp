import json
from datetime import datetime, timedelta
from airflow.decorators import task,dag

from handler_services.reader_config import FactoryReaderConfig, FactoryReaderFile
from settings import POSTGRES_MINIO_FILE, MINIO_ATTRIBUT,POSTGRES_ATTRIBUT,DIRECTORY_DATA_BYKE_RAW,BUCKET_DATA_BYKE_ARCHIVE,DIRECTORY_DATA_BYKE_REPORT
from handler_services.storage_services.minio_services import MinioServices
from handler_services.storage_services.steps_store_data_file import StepsDataSinkFileFromWeb
from handler_services.db_postgres_services.dbinstance import DBInstance
from handler_services.db_postgres_services.steps import StepsGetAllUrlDataBykeClassWithStatus
from handler_services.data_byke_services.config_class import StatusFile
from handler_services.data_byke_services.data_file_info import DataBykeUrlsClass
from handler_services.db_postgres_services.steps import StepsUpdateStatusDataUrls


default_args = {
    'owner': 'ben_omar',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='download_storage_databyke_in_minio_v7',
     default_args=default_args,
     schedule=timedelta(hours=1),
     start_date=datetime(2022, 12, 1),
     catchup=False)
def dag_updaload_file_to_s3():
    @task()
    def get_all_links_with_status_created():
        db_instance = DBInstance(reader_config=FactoryReaderConfig.CONFIG_POSTGRES,
                                 reader_file=FactoryReaderFile.YAML,
                                 path_config=POSTGRES_MINIO_FILE,
                                 attribut=POSTGRES_ATTRIBUT)

        steps = StepsGetAllUrlDataBykeClassWithStatus(engine=db_instance.engine, param_status=StatusFile.CREATED)
        list_url_db = steps.run()
        list_json_dump = [url.model_dump_json() for url in list_url_db]
        return list_json_dump
    @task()
    def upload_from_web_to_minio_s3(list_urls):
        """
        methode to upload file from web to minio
        :param list_urls: list of url from db
        :return: list result of the upload url
        """
        if len(list_urls) > 0 :
            list_convert = [DataBykeUrlsClass.parse_obj(json.loads(url_json)) for url_json in list_urls]
            print(list_convert)
            s3_minio = MinioServices(reader_config=FactoryReaderConfig.CONFIG_MINIO,
                                     reader_file=FactoryReaderFile.YAML,
                                     path_config=POSTGRES_MINIO_FILE,
                                     attrubute_config=MINIO_ATTRIBUT)
            step_uplaod_run = StepsDataSinkFileFromWeb(data_bykes=[list_convert[0]],
                                                       minio_credential=s3_minio.minio_credential,
                                                       bucket_name=BUCKET_DATA_BYKE_ARCHIVE,
                                                       path=None)
            list_result_1 = step_uplaod_run.run()
            print(list_result_1)
            list_json_dump = [url.model_dump_json() for url in list_result_1]
            return list_json_dump
        else:
            print('no list_data_urls, so task skipped')
            return []

    @task()
    def insert_meta_data_to_db(list_json):
        if len(list_json) > 0 :
            list_convert = [DataBykeUrlsClass.parse_obj(json.loads(url_json)) for url_json in list_json]
            lis_url_db = [data_url.get_db() for data_url in list_convert]
            step_update = StepsUpdateStatusDataUrls()
            db_instance = DBInstance(reader_config=FactoryReaderConfig.CONFIG_POSTGRES,
                                     reader_file=FactoryReaderFile.YAML,
                                     path_config=POSTGRES_MINIO_FILE,
                                     attribut=POSTGRES_ATTRIBUT)
            step_update.run(engine=db_instance.engine,list_data_urls=lis_url_db)
        else:
            print('no list_data_urls, so task skipped')


    list_urls_to_download = get_all_links_with_status_created()
    list_result = upload_from_web_to_minio_s3(list_urls_to_download)
    insert_meta_data_to_db(list_result)

dag_updaload_file_to_s3()


