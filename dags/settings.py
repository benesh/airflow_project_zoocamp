POSTGRES_MINIO_FILE='dags/conf/conf2.yaml'
POSTGRES='postgres'
MINIO = 'minio'
FORMAT_JSON = 'json'
FORMAT_YAML = 'yaml'
POSTGRES_ATTRIBUT='postgres_db_user'
MINIO_ATTRIBUT='minio_S3'
URL_DATA_DYKE='https://s3.amazonaws.com/tripdata/index.html'
BUCKET_DATA_BYKE_ARCHIVE = 'data-file-byke-nc-arhive'
DIRECTORY_DATA_BYKE_REPORT= '/report'
DIRECTORY_DATA_BYKE_RAW= '/raw'
BUCKET_DATA_BYKE= 'data-byke-nc'
WORKDIR_DATA='/home/airflow/workdir'
ARCHIVES_FILES = 'archives'
UNPACK_FILES ='unpack_files'

"""
-------list attribute 
"""

key_column_renanme= 'col_rename'
key_col_to_datetime= 'col_datetime'
key_path_to_file='path_to_file'
key_path_to_extract = 'path_to_extract'


"""
------- list dict for renaming column
"""
dict_column_to_rename={
                        'tripduration':'TRIP_DURATION',
                        'starttime':'START_AT',
                        'stoptime':'STOP_AT',
                        'start station id':'START_STATION_ID',
                        'start station name':'START_STATION_NAME',
                        'start station latitude':'START_STATION_LATITUDE',
                        'start station longitude':'START_STATION_LONGITUDE',
                        'end station id':'END_STATION_ID',
                        'end station name':'END_STATION_NAME',
                        'end station latitude':'END_STATION_LATITUDE',
                        'end station longitude':'END_STATION_LONGITUDE',
                        'bikeid':'BIKE_ID',
                        'usertype':'USER_TYPE',
                        'birth year':'BIRTH_YEAR',
                        'rideable_type':'RIDEABLE_TYPE',
                        'started_at':'START_AT',
                        'ended_at':'STOP_AT',
                        'start_station_name':'START_STATION_NAME',
                        'start_station_id':'START_STATION_ID',
                        'end_station_name':'END_STATION_NAME',
                        'end_station_id':'END_STATION_ID',
                        'start_lat':'START_STATION_LATITUDE',
                        'start_lng':'START_STATION_LONGITUDE',
                        'end_lat':'END_STATION_LATITUDE',
                        'end_lng':'END_STATION_LONGITUDE',
                        'member_casual':'USER_TYPE'
                       }

"""
list of column 
"""

list_column_to_remove=['ride_id','gender']
columns_to_datetime=['START_AT','STOP_AT']


"""
list of folder to remove
"""

list_folder_to_remove_defautl =['__MACOSX']