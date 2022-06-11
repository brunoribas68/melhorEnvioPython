import pandas as pd
import connection as conn

def importLog():
    data_frame = pd.read_json('logs.txt', lines=True)
    return data_frame


if __name__ == '__main__':
    connection = conn.Connection.create_server_connection()
    print(connection)
    # data_frame = importLog()
    # print(data_frame.dtypes)
