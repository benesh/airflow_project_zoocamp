from handler_services.etl_services.steps_utils_system import *
from handler_services.data_byke_services.data_file_info import DataBikeUrlsClass
from handler_services.etl_services.steps_data_transform import runner_transformer_data,FactoryDataTransformer
from handler_services.etl_services.steps_utils_system import runner_transformer_file,FactoryTransformerFile,reader_csv_file_for_dataframe,writer_dataframe_to_parquet
from settings import WORKDIR_DATA,ARCHIVES_FILES,UNPACK_FILES
#
# def run_transform_process(data_bike:DataBikeUrlsClass,
#                           catalog_transformer_file:List[FactoryTransformerFile],
#                           catalog_transformer_data:List[FactoryDataTransformer]):
#     if data_bike.month is not None:
#         runner_transformer_file(data_byke=)
#
# def run_tranformation_for_yearly_dir(data_byke:DataBikeUrlsClass,):
#
# def run_transformation_for_monthly_dir(data_bike:DataBikeUrlsClass,
#                                        catalog_transformer_file:List[FactoryTransformerFile],
#                                        catalog_transformer_data:List[FactoryDataTransformer]):
#     runner_transformer_file(data_byke=data_bike,catalog_transformer_file=catalog_transformer_file)
#     reader_csv_file_for_dataframe(f'')
#     runner_transformer_file(data_byke=)