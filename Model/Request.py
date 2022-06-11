from dotenv import load_dotenv
import os
from eloquent import DatabaseManager, Model

load_dotenv()
host_name = os.getenv('HOST_NAME', default=None)
user_name = os.getenv('USER_NAME', default=None)
user_password = os.getenv('USER_PASSWORD', default=None)
database = os.getenv('DATABASE', default=None)
driver = os.getenv('DRIVER', default=None)
prefix = os.getenv('DRIVER', default=None)

config = {
    'mysql': {
        'driver': driver,
        'host': host_name,
        'database': database,
        'username': user_name,
        'password': user_password,
        'prefix': prefix
    }
}


class Request(Model):
    __table__ = 'request'
