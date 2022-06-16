import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def ImportLog():
    return pd.read_json('logs.txt', lines=True)


def OrganizaDataFrame(dados):
    # organizando o lantencia pois ele que usaremos para os relatorios
    dados['latencies']['service_id'] = dados['services'].id
    dados['latencies']['consumer_id'] = dados['authenticated_entity']
    dados['latencies']['client_ip'] = dados['client_ip']
    dados['latencies']['route'] = dados['routes'].id

    #dizendo par ao DataFrame que o id é o index de routes
    dados['routes'].set_index('id', inplace=True)

    # renomeando a coluna para que não tenha .
    dados['routes'].rename(columns={'service.id': 'service_id'}, inplace=True)

    # os pop's a seguir são para limpar variaveis que os dados se repetem
    # para descobrir quais são fiz um:
    # new_data_frame[nomeDoDataFrame][headerQueValidei].value_counts()
    dados['requests'].drop(['headers.user-agent', 'uri', 'querystring', 'headers.accept'], axis=1, inplace=True)
    dados['routes'].drop(['regex_priority', 'preserve_host', 'strip_path', 'protocols', 'paths', 'methods'],
                         axis=1, inplace=True)
    dados['services'].drop(['path', 'port', 'protocol', 'read_timeout', 'retries', 'write_timeout', 'connect_timeout'],
                           axis=1, inplace=True)
    return dados


def ArrumaJson(data_frame):
    # criação do novo dataframe com os dados convertidos de json para dataframe
    # iniciamente pensei em gravar todos esses dados no banco
    # porem fui informado que não era necessario
    new_data_frame = {
        'latencies': pd.json_normalize(data_frame.latencies),
        'requests': pd.json_normalize(data_frame.request),
        'responses': pd.json_normalize(data_frame.response),
        'authenticated_entity': pd.json_normalize(data_frame.authenticated_entity),
        'routes': pd.json_normalize(data_frame.route),
        'services': pd.json_normalize(data_frame.service),
        'client_ip': data_frame.client_ip,
        'upstream_uri': data_frame.upstream_uri,
        'started_at': data_frame.started_at
    }

    return OrganizaDataFrame(new_data_frame)


def RelatorioConsumidor(consumers):
    # organizar por consumidor
    relatorio_por_consumidor = consumers.groupby(by='consumer_id')
    csv = {}
    os.makedirs('customers', exist_ok=True)
    for customer, dados in relatorio_por_consumidor:
        csv[customer] = dados
        csv[customer].to_csv(f"./customers/{customer}.csv", sep=";")
    return relatorio_por_consumidor


def RelatorioServico(servico):
    # organizar por serviço
    relatorio_por_servico = new_data_frame['services'].groupby(by='id')
    csv = {}
    os.makedirs('services', exist_ok=True)
    for services, dados in relatorio_por_servico:
        csv[services] = dados
        csv[services].to_csv(f"./services/{dados.iloc[0]['name']}.csv", sep=";")
    return relatorio_por_servico


def RelatorioMediaLatencia(latencies):
    # organizar pelo consumidor com as médias
    relatorio_medias_consumidor = latencies.groupby('consumer_id').mean()
    relatorio_medias_consumidor.to_csv('relatorio_medias_consumidor.csv', sep=';')
    return relatorio_medias_consumidor


def CriarBase(base):
    engine = create_engine(f"mysql+pymysql://{os.getenv('USER_NAME')}:{os.getenv('USER_PASSWORD')}@"
                           f"{os.getenv('HOST_NAME')}/{os.getenv('DATABASE')}", echo=True)
    # para o proposito dessa atividade deixei para que ele recrie a base se ela já existir
    # acredito que esse não seja o recomendado, porem para o objetivo da tarefa é o mais indicado
    new_data_frame['routes'].to_sql('routes',engine, if_exists='replace', index=False, index_label='id')
    new_data_frame['requests'].to_sql('requests',engine, if_exists='replace', index=True, index_label='request')
    new_data_frame['latencies'].to_sql('latencies',engine, if_exists='replace', index=False, index_label='id')
    new_data_frame['services'].to_sql('services',engine, if_exists='replace', index=False, index_label='id')

if __name__ == '__main__':
    load_dotenv()
    data_frame = ImportLog()
    new_data_frame = ArrumaJson(data_frame)
    RelatorioConsumidor(new_data_frame['latencies'])
    RelatorioServico(new_data_frame['latencies'])
    RelatorioMediaLatencia(new_data_frame['latencies'])
    CriarBase(new_data_frame)
