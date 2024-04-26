from sqlalchemy import Column , Integer, String,DateTime
from handler_services.db_postgres_services.dbinstance import Base


class DataBykeUrlsDB(Base):
    __tablename__ = 'data_byke_urls_v4'
    id = Column (Integer, primary_key=True, autoincrement=True)
    file_name = Column (String(100))
    url = Column (String(100))
    month = Column (String(10))
    year = Column (String(10))
    status = Column(String(50))
    created_at = Column (DateTime)
    updated_at = Column (DateTime)
    def __repr__(self):
        return (f"Data_Byke_urlsDB('id = {self.id},file_name = {self.file_name},"
                f" url = {self.url}, month ={self.month}, year = {self.year}, "
                f"status = {self.status}, created_at = {self.created_at}, updated_at = {self.updated_at} ")