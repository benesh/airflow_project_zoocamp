from handler_services.etl_services.steps_utils_system import *
from handler_services.etl_services.steps_data_transform import runner_transformer_data, DataTransformerObject, \
    ConvertColunmToString
from handler_services.etl_services.steps_utils_system import reader_csv_file_for_dataframe,writer_dataframe_to_parquet
from settings import key_column_to_string
import os

def runner_transformer_data_bike(catalogue_data_transformer:List[DataTransformerObject],path_data_files:str,data_bike:DataBikeUrlsClass):
    for folder in get_list_dir(f'{path_data_files}/{data_bike.year}'):
        if len(get_list_files(f'{path_data_files}/{data_bike.year}/{folder}')) > 0:
            df = reader_csv_file_for_dataframe(f'{path_data_files}/{data_bike.year}/{folder}')
            df = runner_transformer_data(catalogue_data_transformer=catalogue_data_transformer,data=df)
            writer_dataframe_to_parquet(df=df,path=f'{path_data_files}/parquest/{data_bike.year}/{folder}',filename='data_file.parquet')
    return f'{path_data_files}/parquest'

