from storage_services.db_services import DbEngine
from minio import Minio
import requests


engine = create_engine('postgresql://postgres:postgres@postgres_container:5432/db')

def get_files_to_local():
    query = 'SELECT * FROM "files_list"'
    user: str = "postgres"
    password: str = "postgres"
    host: str = "postgres_container"
    port: str ="5432"
    db: str ="db"

    db_engine = DbEngine(user,password,host,port,db)
    db_engine.create_engine()

client =  Minio("127.0.0.1:9000"
           ,"e2fZZVmTAR3YNwDz6cZ9"
           ,"hq7WATPaGdnKcFgk09qTfP5rmcxXBVjdtmNEDWln"
           ,secure=False)

print(client.list_buckets())

url = 'https://s3.amazonaws.com/tripdata/201307-citibike-tripdata.zip'
file_name ="201307-citibike-tripdata.zip"
source_file = "/home/benomar/data/"+file_name

r = requests.get(url, allow_redirects=True)

open(source_file, 'wb').write(r.content)

# The file to upload, change this path if needed

# The destination bucket and filename on the MinIO server
bucket_name = "python-test-bucket"
destination_file = "/data/temp/2013/07/"+file_name

# Make the bucket if it doesn't exist.
found = client.bucket_exists(bucket_name)
if not found:
    client.make_bucket(bucket_name)
    print("Created bucket", bucket_name)
else:
    print("Bucket", bucket_name, "already exists")

# Upload the file, renaming it in the process
client.fput_object(
    bucket_name, destination_file, source_file,
)
print(
    source_file, "successfully uploaded as object",
    destination_file, "to bucket", bucket_name,
)

def printfile():
    for i in range(52):
        print(i,"element in flow")

# # config.yaml
# query: |
#   SELECT * FROM my_table
#   WHERE column_name = :param_value;
# params:
#   param_value: example_value
#
# import yaml
# from sqlalchemy import create_engine, text
#
# def load_query_from_config(config_file):
#     with open(config_file, 'r') as file:
#         config = yaml.safe_load(file)
#     return config['query'], config['params']
#
# def execute_query(query, params):
#     # Your code to execute the query using SQLAlchemy
#     engine = create_engine("your_database_connection_string")
#     with engine.connect() as connection:
#         result = connection.execute(text(query), params).fetchall()
#         print(result)
#
# if __name__ == "__main__":
#     config_file_path = 'config.yaml'
#     sql_query, parameters = load_query_from_config(config_file_path)
#
#     execute_query(sql_query, parameters)
# pip install PyYAML SQLAlchemy  # or other database library


from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# Create a SQLite database in-memory engine for this example
engine = create_engine('sqlite:///:memory:', echo=True)

# Declarative base class for declarative models
Base = declarative_base()

# Define the User class as an ORM model
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    username = Column(String(50), unique=True)
    email = Column(String(50))

# Create the table in the database
Base.metadata.create_all(engine)

# Create a new user instance
new_user = User(username='john_doe', email='john.doe@example.com')

# Add the user to the session and commit to the database
Session = sessionmaker(bind=engine)
session = Session()
session.add(new_user)
session.commit()

# Query the user from the database
queried_user = session.query(User).filter_by(username='john_doe').first()

# Print the user information
print(f"Queried User: {queried_user.username}, {queried_user.email}")



if __name__ == "__main__":
    printfile()