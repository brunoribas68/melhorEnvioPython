from typing import Union, Any, Dict, SupportsIndex
import pandas as pd


class Data:
    @classmethod
    def ArrumaJson(cls, data_frame: Dict[str, object]) -> dict:
        # criação do novo dataframe com os dados convertidos de json para dataframe
        # iniciamente pensei em gravar todos esses dados no banco
        # porem fui informado que não era necessario
        new_data_frame = {
            'latencies': pd.json_normalize(data_frame['latencies']),
            'requests': pd.json_normalize(data_frame['request']),
            'responses': pd.json_normalize(data_frame['response']),
            'authenticated_entity': pd.json_normalize(data_frame['authenticated_entity']),
            'routes': pd.json_normalize(data_frame['route']),
            'services': pd.json_normalize(data_frame['service']),
            'client_ip': data_frame['client_ip'],
            'upstream_uri': data_frame['upstream_uri'],
            'started_at': data_frame['started_at']
        }

        return cls.OrganizaDataFrame(new_data_frame)

    @classmethod
    def OrganizaDataFrame(cls, dados: dict) -> dict:
        # organizando o lantencia pois ele que usaremos para os relatorios
        dados['latencies']['service_id'] = dados['services']['id']
        dados['latencies']['consumer_id'] = dados['authenticated_entity']
        dados['latencies']['client_ip'] = dados['client_ip']
        dados['latencies']['route'] = dados['routes']['id']
        dados['latencies']['service'] = dados['services']['name']

        # dizendo par ao DataFrame que o id é o index de routes
        dados['routes'].set_index('id', inplace=True)

        # renomeando a coluna para que não tenha .
        dados['routes'].rename(columns={'service.id': 'service_id'}, inplace=True)

        # os pop's a seguir são para limpar variaveis que os dados se repetem
        # para descobrir quais são fiz um:
        # new_data_frame[nomeDoDataFrame][headerQueValidei].value_counts()
        dados['requests'].drop(['headers.user-agent', 'uri', 'querystring', 'headers.accept'], axis=1, inplace=True)
        dados['routes'].drop(['regex_priority', 'preserve_host', 'strip_path', 'protocols', 'paths', 'methods'],
                             axis=1, inplace=True)
        dados['services'].drop(
            ['path', 'port', 'protocol', 'read_timeout', 'retries', 'write_timeout', 'connect_timeout'],
            axis=1, inplace=True)
        return dados

    @staticmethod
    def ImportLog() -> Dict[str, object]:
        return pd.read_json('logs.txt', lines=True)
