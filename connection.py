import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import os

load_dotenv()
host_name = os.getenv('HOST_NAME', default=None)
user_name = os.getenv('USER_NAME', default=None)
user_password = os.getenv('USER_PASSWORD', default=None)
database = os.getenv('DATABASE', default=None)


def create_connection():
    try:
        db_connection = mysql.connector.connect(host=host_name, user=user_name, password=user_password,
                                                database=database)
        print("Database connection made!")
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database doesn't exist")
        elif error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("User name or password is wrong")
        else:
            print(error)

    return db_connection

