import pandas as pd
from sqlalchemy import create_engine
from handler_services.webparser_services import LinksFile

conn_string = 'postgresql://admin:admin123@postgres-app:5432/project_airflow'
engine = create_engine(conn_string)
link1 = LinksFile('https://s3.amazonaws.com/tripdata/2013-citibike-tripdata.zip', '2013-citibike-tripdata.zip', 2013, None, 'CREATED')
link2 = LinksFile('https://s3.amazonaws.com/tripdata/2014-citibike-tripdata.zip', '2014-citibike-tripdata.zip', 2014, None, 'CREATED')
listlink = [link1,link2]
df = pd.DataFrame(data=listlink, columns=LinksFile._fields)
with engine.connect() as connection:
    result = df.to_sql(name='data_info_v3', con=connection, if_exists='append', index=False)
print("hello",result)