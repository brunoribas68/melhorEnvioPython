import os
from typing import Any, Dict

from sqlalchemy import create_engine
from dotenv import load_dotenv


class BancoDeDados:
    load_dotenv()
    engine = create_engine(f"mysql+pymysql://"
                           f"{os.getenv('USER_NAME')}:"
                           f"{os.getenv('USER_PASSWORD')}@"
                           f"{os.getenv('HOST_NAME')}/"
                           f"{os.getenv('DATABASE')}", echo=True)

    def CriarBase(self, base: Dict[Any, Any]):
        # para o proposito dessa atividade deixei
        # para que ele recrie a base se ela já existir
        # acredito que esse não seja o recomendado,
        # porem para o objetivo da tarefa é o mais indicado
        base['routes'].to_sql('routes', self.engine, if_exists='replace',
                              index=False, index_label='id')
        base['requests'].to_sql('requests', self.engine, if_exists='replace',
                                index=True, index_label='request')
        base['latencies'].to_sql('latencies', self.engine, if_exists='replace',
                                 index=False, index_label='id')
        base['services'].to_sql('services', self.engine, if_exists='replace',
                                index=False, index_label='id')
