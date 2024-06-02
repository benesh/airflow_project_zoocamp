import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from handler_services.data_bike_services.config_class import StatusFile
from handler_services.data_bike_services.data_file_info import DataBikeUrlsClass
from handler_services.db_postgres_services.dbinstance import DBInstance
from handler_services.db_postgres_services.steps_db_queries import StepsGetAllUrlDataBike, \
    StepsGetAllUrlDataBikeClassWithStatus, StepsUpdateStatusDataUrls
from handler_services.etl_services.step_transfomr_data_bike import runner_transformer_data_bike
from handler_services.etl_services.steps_data_transform import DataTransformerObject, FactoryDataTransformer
from handler_services.etl_services.steps_utils_system import FactoryTransformerFile, TransformerFileObject, \
    runner_transformer_file
from handler_services.reader_config import FactoryReaderConfig, FactoryReaderFile, runner_read_config
from handler_services.storage_services.steps_storage_data_file import StepGetDataFromMinioS3, StepDataUploadToMinioS3
from settings import WORKDIR_DATA, POSTGRES_MINIO_FILE, POSTGRES_ATTRIBUT, MINIO_ATTRIBUT, key_minio_credential, \
    key_bucket_name, BUCKET_DATA_BIKE_ARCHIVE, key_path_to_extract, FOLDER_ARCHIVES_FILES, key_path_to_dest, \
    key_path_file, \
    key_path_to_file, FOLDER_UNPACKED_FILES, key_column_renanme, dict_column_to_rename, key_col_to_datetime, \
    columns_to_datetime, list_column_to_remove, key_column_to_drop, key_column_to_string, column_to_string, \
    key_column_gender, key_column_member, key_column_rideable_type, key_path, BUCKET_DATA_BIKE_RAW

"""
attribute to input the de data in the dictionary
dir_to_rm
path_file
path_extrac_file
"""

default_args = {
    'owner': 'benesh',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}
@dag(dag_id='dag_unzip_convert_to_parquet_v08',
     default_args=default_args,
     start_date=datetime(2024, 4, 23),
     schedule=timedelta(hours=1)
     )

def unzip_data_file():
    @task.branch()
    def get_data_file_from_db():
        db_instance = DBInstance(reader_config=FactoryReaderConfig.CONFIG_POSTGRES,
                                 reader_file=FactoryReaderFile.YAML,
                                 path_config=POSTGRES_MINIO_FILE,
                                 attribut=POSTGRES_ATTRIBUT)
        engine = db_instance.engine
        step_get_data = StepsGetAllUrlDataBikeClassWithStatus(engine,StatusFile.UPLOADED_FILE)
        data_bike_urls = step_get_data.run()
        list_json_dump = [url.model_dump_json() for url in data_bike_urls]
        if len(list_json_dump) == 0:
            raise AirflowException("Skipping remaining tasks and ending DAG. The is no Data to Download")
        data_bike = [data_bike_urls[0]]
        list_json_dump = [data.model_dump_json() for data in data_bike]
        return list_json_dump

    # @task
    # def no_data_to_upload(xcom):



    @task()
    def data_bike_download_file(xcom_data_bike):
        data_bike = [DataBikeUrlsClass.parse_obj(json.loads(url_json)) for url_json in xcom_data_bike]
        s3_minio = runner_read_config(reader_config=FactoryReaderConfig.CONFIG_MINIO,
                                       reader_file=FactoryReaderFile.YAML,
                                       path_config=POSTGRES_MINIO_FILE,
                                       attribut=MINIO_ATTRIBUT)
        config = {
            key_minio_credential:s3_minio,
            key_bucket_name : BUCKET_DATA_BIKE_ARCHIVE,
            key_path_to_dest:f'{WORKDIR_DATA}/{FOLDER_ARCHIVES_FILES}',
            key_path_file:''
                  }
        uplaod_runner =StepGetDataFromMinioS3()
        uplaod_runner.run(data_bike[0],config)
        data_bike_json = [data.model_dump_json() for data in data_bike]
        return data_bike_json
    @task
    def unpack_file_to_csv(xcom_data_bike):
        data_bike = [DataBikeUrlsClass.parse_obj(json.loads(url_json)) for url_json in xcom_data_bike]
        config={
            key_path_to_file:f'{WORKDIR_DATA}/{FOLDER_ARCHIVES_FILES}/{data_bike[0].file_name}',
            key_path_to_extract:f'{WORKDIR_DATA}/{FOLDER_UNPACKED_FILES}',
        }
        catalog_transform = [
            TransformerFileObject(transformer_file=FactoryTransformerFile.UNZIP,config=config),
            TransformerFileObject(transformer_file=FactoryTransformerFile.CLEAN_DIR,config=config),
            TransformerFileObject(transformer_file=FactoryTransformerFile.RESTRUCTURE_FOLDER,config=config),
        ]

        runner_transformer_file(data_bike[0],catalog_transform)
        data_bike_json = [data.model_dump_json() for data in data_bike]
        return data_bike_json
    @task
    def transform_to_parquet(xcom_data_bike):
        data_bike = [DataBikeUrlsClass.parse_obj(json.loads(url_json)) for url_json in xcom_data_bike]
        catalaog_data_ransform = [
            DataTransformerObject(transformer=FactoryDataTransformer.DROP_COLUMNS,config={key_column_to_drop:list_column_to_remove}),
            DataTransformerObject(transformer=FactoryDataTransformer.RENAME_COL,config={key_column_renanme:dict_column_to_rename}),
            DataTransformerObject(transformer=FactoryDataTransformer.CONVERT_TO_DATETIME,config={key_col_to_datetime:columns_to_datetime}),
            DataTransformerObject(transformer=FactoryDataTransformer.CONVERT_TO_STRING,config={key_column_to_string:column_to_string}),
            DataTransformerObject(transformer=FactoryDataTransformer.BIKE_TYPE_TRANSFORMER,config={key_column_rideable_type:'RIDEABLE_TYPE'}),
            DataTransformerObject(transformer=FactoryDataTransformer.MEMBER_TRANSFORMER,config={key_column_member:'USER_TYPE'}),
            DataTransformerObject(transformer=FactoryDataTransformer.GENDER_TRANSFORMER,config={key_column_gender:'GENDER'})
        ]
        path = runner_transformer_data_bike(catalogue_data_transformer=catalaog_data_ransform,
                                            path_data_files=f'{WORKDIR_DATA}/{FOLDER_UNPACKED_FILES}',
                                            data_bike=data_bike[0])
        data = data_bike[0]
        data_return_json =  [data.model_dump_json(), path]
        return data_return_json

    @task
    def upload_parquet_file_to_s3(xcom_data_bike):
        data_bike = DataBikeUrlsClass.parse_obj(json.loads(xcom_data_bike[0]))
        path_file_parquet = xcom_data_bike[1]
        print(path_file_parquet)
        s3_minio = runner_read_config(reader_config=FactoryReaderConfig.CONFIG_MINIO,
                                      reader_file=FactoryReaderFile.YAML,
                                      path_config=POSTGRES_MINIO_FILE,
                                      attribut=MINIO_ATTRIBUT)
        runner = StepDataUploadToMinioS3()
        config={
            key_minio_credential:s3_minio,
            key_path:path_file_parquet,
            key_bucket_name:BUCKET_DATA_BIKE_RAW
        }
        runner.run(data_bike,config)

        db_instance = DBInstance(reader_config=FactoryReaderConfig.CONFIG_POSTGRES,
                                 reader_file=FactoryReaderFile.YAML,
                                 path_config=POSTGRES_MINIO_FILE,
                                 attribut=POSTGRES_ATTRIBUT)
        engine = db_instance.engine
        sql_update = StepsUpdateStatusDataUrls()
        data_bike_db = data_bike.get_db()
        sql_update.run(engine=engine,list_data_urls=[data_bike_db])



    data_bike_file = get_data_file_from_db()
    data_bike_file_1 = data_bike_download_file(data_bike_file)
    data_bike_file_2 = unpack_file_to_csv(data_bike_file_1)
    data_bike_file_3 = transform_to_parquet(data_bike_file_2)
    upload_parquet_file_to_s3(data_bike_file_3)

    get_data_file_from_db() >> data_bike_download_file(data_bike_file ) >> unpack_file_to_csv(data_bike_file_1) >> transform_to_parquet(data_bike_file_2) >> upload_parquet_file_to_s3(data_bike_file_3) >> get_data_file_from_db()

unzip_data_file()