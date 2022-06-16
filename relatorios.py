import os
import pandas as pd


class Relatorios:
    @staticmethod
    def RelatorioConsumidor(consumers: pd.DataFrame):
        # organizar por consumidor
        relatorio_por_consumidor = consumers.groupby(by='consumer_id')
        csv = {}
        os.makedirs('customers', exist_ok=True)
        for customer, dados in relatorio_por_consumidor:
            csv[customer] = dados
            csv[customer].to_csv(f"./customers/{customer}.csv", sep=";")
        return relatorio_por_consumidor

    @staticmethod
    def RelatorioServico(servicos: pd.DataFrame):
        # organizar por serviço
        relatorio_por_servico = servicos.groupby(by='service_id')
        csv = {}
        os.makedirs('services', exist_ok=True)
        for services, dados in relatorio_por_servico:
            csv[services] = dados
            csv[services].to_csv(f"./services/{dados.iloc[0]['service']}.csv", sep=";")
        return relatorio_por_servico

    @staticmethod
    def RelatorioMediaLatencia(latencies: pd.DataFrame):
        # organizar pelo consumidor com as médias
        relatorio_medias_consumidor = latencies.groupby('consumer_id').mean()
        relatorio_medias_consumidor.to_csv('relatorio_medias_consumidor.csv', sep=';')
        return relatorio_medias_consumidor
