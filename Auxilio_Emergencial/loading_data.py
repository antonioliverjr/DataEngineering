import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import func, select
from database import database_conn

def tabela_db(file_csv:str) -> str:
    if 'AuxilioEmergencial'in file_csv:
        return 'TB_STG_AUXEMERGENCIAL_BENEFICIOS'
    elif 'anonimos' in file_csv:
        return 'TB_STG_AUXEMERGENCIAL_BENEFICIADOS_ANONIMOS'
    elif 'registrados' in file_csv:
        return 'TB_STG_AUXEMERGENCIAL_BENEFICIADOS_REGISTRADOS'
    elif 'municipios' in file_csv:
        return 'TB_STG_AUXEMERGENCIAL_MUNICIPIOS'

def loading_csv(path_files:str):
    engine = database_conn()
    process = 0
    files = len(os.listdir(path_files))
    for csv_file in os.listdir(path_files):
        tabela = tabela_db(csv_file)
        print(tabela)
        path_csv = os.path.join(path_files, csv_file)
        print(f'Carregando CSV - {csv_file}:')
        df_csv = pd.read_csv(path_csv)
        total_rows = df_csv.shape[0]
        sql = f'TRUNCATE TABLE {tabela}'
        with engine.begin() as conn:
            conn.execute(sql)
        print(f'Inserindo um total de {total_rows} registros.')
        df_csv.to_sql(tabela, con=engine, if_exists='replace', index=False, chunksize=1000000)
        process += 1

    if process == files:
        print('Processo realizado com sucesso!')
'''
    logging.basicConfig(filename="carga_aux_emerg.log", filemode='w', level=logging.DEBUG)

    with engine.connect() as conn:
        result = conn.execute(select([func.count()]).select_from('AUXEMERG.TB_STG_AUXEMERGENCIAL_BENEFICIOS').scalar())
    if result == total_rows: 
        logging.info(f'{datetime.now()} - Sucesso')
        print(f'{datetime.now()} - Sucesso')
    else:
        logging.warning(f'{datetime.now()} - Total de registros não confere!')
        print(f'{datetime.now()} - Total de registros não confere!')
'''
if __name__ == '__main__':
    #loading_csv('csv_aux_emergencial')
    caminho = os.path.dirname(os.path.realpath(__file__))
    lista = os.listdir(caminho+'\\csv_aux_emergencial')
    loading_csv(os.path.join(caminho,'csv_aux_emergencial',lista[1]))