from dotenv import load_dotenv
from database import BancoDeDados
from data import Data
from relatorios import Relatorios

if __name__ == '__main__':
    load_dotenv()
    data_frame = Data.ImportLog()
    new_data_frame = Data.ArrumaJson(data_frame)
    Relatorios.RelatorioConsumidor(new_data_frame['latencies'])
    Relatorios.RelatorioServico(new_data_frame['latencies'])
    Relatorios.RelatorioMediaLatencia(new_data_frame['latencies'])
    BancoDeDados.CriarBase(self=BancoDeDados, base=new_data_frame)
