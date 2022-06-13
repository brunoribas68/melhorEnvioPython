import pandas as pd
import connection as conn


def importLog():
    data_frame = pd.read_json('logs.txt', lines=True)
    return data_frame


if __name__ == '__main__':
    # connection = conn.Connection.create_server_connection()
    data_frame = importLog()
    relatorio_por_servico = pd.DataFrame()
    relatorio_por_consumidor = pd.DataFrame()
    relatorio_medias_consumidor = pd.DataFrame()
    new_data_frame = {'lantencies': pd.json_normalize(data_frame.latencies),
                      'request': pd.json_normalize(data_frame.request),
                      'response': pd.json_normalize(data_frame.response),
                      'authenticated_entity': pd.json_normalize(data_frame.authenticated_entity),
                      'route': pd.json_normalize(data_frame.route), 'service': pd.json_normalize(data_frame.service)}

    ## organizando o lantencia pois ele que usaremos para os relatorios
    new_data_frame['lantencies']['service'] = new_data_frame['service'].id
    new_data_frame['lantencies']['customer_id'] = new_data_frame['authenticated_entity']

    ## organizar por serviço
    relatorio_por_servico = new_data_frame['lantencies'].sort_values(['service'])

    ## organizar por consumidor
    relatorio_por_consumidor = new_data_frame['lantencies'].sort_values(by='customer_id')

    ## organizar pelo consumidor com as médias
    relatorio_medias_consumidor = new_data_frame['lantencies'].groupby('customer_id').mean()
