import os
import pandas as pd
import time
from sqlalchemy.sql import text
from database import conexao, store_procedure, engine
from database import TB_BENEFICIOS, TB_REGISTRADOS, TB_ANONIMOS, TB_MUNICIPIOS, TB_DATA
from list_procedures import list_procedures_flow
from log import register_log

def verifica_dt_set(ano:str, mes:str) -> bool:
    sql_sel_dt = text("SELECT COUNT(*) AS DADOS FROM "+ TB_DATA +" WHERE NM_ANO = :year AND NM_MES = :month")
    with conexao() as conn:
        result = conn.execute(sql_sel_dt, {'year': ano,'month': mes}).fetchone()

    if result > 0:
        return True
    return False

def tabela_db(file_csv:str) -> str:
    if 'AuxilioEmergencial'in file_csv:
        return str(TB_BENEFICIOS)
    elif 'anonimos' in file_csv:
        return str(TB_ANONIMOS)
    elif 'registrados' in file_csv:
        return str(TB_REGISTRADOS)
    elif 'municipios' in file_csv:
        return str(TB_MUNICIPIOS)

def loading_csv(path_files:str) -> bool:
    process = 0
    files = len(os.listdir(path_files))
    for csv_file in os.listdir(path_files):
        tabela = tabela_db(csv_file)
        register_log(tabela,'start')
        path_csv = os.path.join(path_files, csv_file)
        print('#############################')
        print(f'Carregando CSV - {csv_file}:')
        df_csv = pd.read_csv(path_csv)
        total_rows = df_csv.shape[0]
        print(f'Limpando {tabela}.')
        sql = text(f'TRUNCATE TABLE {tabela}')
        with conexao() as conn:
            conn.execute(sql)
        print(f'Inserindo um total de {total_rows} registros.')
        df_csv.to_sql(tabela, con=engine, if_exists='replace', index=False, chunksize=1000000)

        sql_check = text(f"SELECT COUNT(*) FROM {tabela}")
        with conexao() as conn:
            result = conn.execute(sql_check).fetchone()
        
        if result[0] == total_rows:
            register_log(tabela, 'end')
            process += 1
        else:
            register_log(tabela, 'error')
            break

    if process == files:
        print('Processo realizado com sucesso!')
        return True
    else:
        print('Falha no processo, quantidade de registros incompletos!')
        return False


def flow_final():
    for seq in list_procedures_flow:
        procedure = list_procedures_flow[seq]
        tabela = procedure.replace('PRC_CARGA_', '')
        if 'TB_' in tabela: register_log(tabela,'start')
        #procedure = text(f'DBO.{procedure}')
        print(f'Executando {procedure}')
        
        try:
            store_procedure(procedure)
        except Exception as err:
            if 'TB_' in tabela: register_log(tabela,'error')
            print(f'Erro - {err}')
            return err
        
        if 'TB_' in tabela: register_log(tabela,'end')
        print(f'{procedure} executada com sucesso!')
        time.sleep(3)

if __name__ == '__main__':
    caminho = os.path.dirname(os.path.realpath(__file__))
    lista = os.listdir(caminho+'\\csv_aux_emergencial')
    loading_csv(os.path.join(caminho,'csv_aux_emergencial',lista[1]))
