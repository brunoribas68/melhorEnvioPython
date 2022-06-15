import pandas as pd
import os
# import connection as conn


def importLog():
    data_frame = pd.read_json('logs.txt', lines=True)
    return data_frame


def arrumaJson(data_frame):
    new_data_frame = {
        'lantencies': pd.json_normalize(data_frame.latencies),
        'request': pd.json_normalize(data_frame.request),
        'response': pd.json_normalize(data_frame.response),
        'authenticated_entity': pd.json_normalize(data_frame.authenticated_entity),
        'route': pd.json_normalize(data_frame.route),
        'service': pd.json_normalize(data_frame.service),
        'client_ip': data_frame.client_ip,
        'upstream_uri': data_frame.upstream_uri,
        'started_at': data_frame.started_at
    }

    ## organizando o lantencia pois ele que usaremos para os relatorios
    new_data_frame['lantencies']['service_id'] = new_data_frame['service'].id
    new_data_frame['lantencies']['service'] = new_data_frame['service'].name
    new_data_frame['lantencies']['customer_id'] = new_data_frame['authenticated_entity']
    new_data_frame['lantencies']['client_ip'] = new_data_frame['client_ip']
    new_data_frame['lantencies']['route'] = new_data_frame['route'].id

    return new_data_frame


def RelatorioConsumidor(new_data_frame):
    relatorio_por_consumidor = pd.DataFrame()
    ## organizar por consumidor
    relatorio_por_consumidor = new_data_frame['lantencies'].groupby(by='customer_id')
    csv = {}
    os.makedirs('customers', exist_ok=True)
    for customer, dados in relatorio_por_consumidor:
        csv[customer] = dados
        csv[customer].to_csv(f"./customers/{customer}.csv", sep=";")
    return relatorio_por_consumidor


def RelatorioServico(new_data_frame):
    relatorio_por_servico = pd.DataFrame()
    ## organizar por serviço
    relatorio_por_servico = new_data_frame['lantencies'].groupby(by='service')
    csv = {}
    os.makedirs('services', exist_ok=True)
    for services, dados in relatorio_por_servico:
        csv[services] = dados
        csv[services].to_csv(f"./services/{services}.csv", sep=";")
    return relatorio_por_servico


def RelatorioMediaLatencia(new_data_frame):
    relatorio_medias_consumidor = pd.DataFrame()
    ## organizar pelo consumidor com as médias
    relatorio_medias_consumidor = new_data_frame['lantencies'].groupby('customer_id').mean()
    relatorio_medias_consumidor.to_csv('relatorio_medias_consumidor.csv', sep=';')
    return relatorio_medias_consumidor

if __name__ == '__main__':
    # connection = conn.Connection.create_server_connection()
    data_frame = importLog()
    new_data_frame = arrumaJson(data_frame)
    relatorio_por_consumidor = RelatorioConsumidor(new_data_frame)
    relatorio_por_servico = RelatorioServico(new_data_frame)
    relatorio_medias_consumidor = RelatorioMediaLatencia(new_data_frame)
