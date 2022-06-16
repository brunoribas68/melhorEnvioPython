from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(f"mysql+pymysql://{os.getenv('USER_NAME')}:{os.getenv('USER_PASSWORD')}@"
                       f"{os.getenv('HOST_NAME')}/{os.getenv('DATABASE')}", echo=True)
Base = declarative_base()

class Latencies(Base):
    __tablename__ = 'latencies'
    id = Column(Integer, primary_key=True)
    request = Column(String(255))
    proxy = Column(Integer)
    kong = Column(Integer)
    service_id = Column(String(255))
    consumer_id = Column(String(255))
    client_ip = Column(String(255))
    route = Column(String(255))


Base.metadata.create_all(engine)
#new_data_frame['routes'].to_sql('routes',engine, if_exists='append', index=False, index_label='id')
#new_data_frame['request'].to_sql('requests',engine, if_exists='append', index=True, index_label='request')
#new_data_frame['latencies'].to_sql('latencies',engine, if_exists='append', index=False, index_label='id')
#new_data_frame['services'].to_sql('services',engine, if_exists='append', index=False, index_label='id')
