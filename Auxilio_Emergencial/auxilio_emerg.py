import os
import pandas as pd
import logging
import shutil
from datetime import datetime
from zipfile import ZipFile
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, func, select

engine = create_engine("mssql+pyodbc://project:Jrdbsql@localhost\\SQLEXPRESS/PANDAS?driver=ODBC+Driver+17+for+SQL+Server")

metadata_obj = MetaData()

stage_auxemergencial = Table(
    'TB_STG_AUXEMERGENCIAL_DADOSCSV'
    ,metadata_obj
    ,Column("ano_mes", Integer)
    ,Column("uf", String)
    ,Column("cod_municipio", Integer)
    ,Column("municipio", String)
    ,Column("nis_benef", String)
    ,Column("cpf", String)
    ,Column("beneficiario", String)
    ,Column("nis_resp", String)    
    ,Column("cpf_resp", String)
    ,Column("nome_resp", String)
    ,Column("enquadrado", String)
    ,Column("parcela", String)
    ,Column("obs", String)
    ,Column("valor", Float)
)

metadata_obj.create_all(engine, checkfirst=True)
print('Tabela criada!')

path_download = r'C:\Users\antoliverjr\Downloads'
path_job = r'C:\Workspace\DataAnalystics\Aux_Emergencial'
path_historico = r'C:\Workspace\DataAnalystics\Aux_Emergencial\historico_zip'
path_files = r'C:\Workspace\DataAnalystics\Aux_Emergencial\csv_aux_emergencial'
logging.basicConfig(filename="carga_files_csv.log", filemode='w', level=logging.DEBUG)

df_colunas = {
    "mes": int
    ,"uf": str
    ,"cod_municipio": float
    ,"municipio": str
    ,"nis_benef": float
    ,"cpf": str
    ,"beneficiario": str
    ,"nis_resp": float
    ,"cpf_resp": str
    ,"nome_resp": str
    ,"enquadra": str
    ,"parc": str
    ,"obs": str
    ,"valor": float
}

df_colunas_replace = {
    "MÊS DISPONIBILIZAÇĂO": "mes"
    ,"UF": "uf"
    ,"CÓDIGO MUNICÍPIO IBGE": "cod_municipio"
    ,"NOME MUNICÍPIO": "municipio"
    ,"NIS BENEFICIÁRIO": "nis_benef"
    ,"CPF BENEFICIÁRIO": "cpf"
    ,"NOME BENEFICIÁRIO": "beneficiario"
    ,"NIS RESPONSÁVEL": "nis_resp"
    ,"CPF RESPONSÁVEL": "cpf_resp"
    ,"NOME RESPONSÁVEL": "nome_resp"
    ,"ENQUADRAMENTO": "enquadra"
    ,"PARCELA": "parc"
    ,"OBSERVAÇĂO": "obs"
    ,"VALOR BENEFÍCIO": "valor"
}

try:
    os.mkdir(path_files)
    os.mkdir(path_historico)
    print('Pastas criadas!')
except FileExistsError as e:
    print(e)

contador = 0

print('Iniciando busca de arquivo zip!')
for files in os.listdir(path_download):
    if '_AuxilioEmergencial.zip' in files:
        print('Arquivo zip encontrado!')
        path_file_origin = os.path.join(path_download, files)
        path_file_origin2 = os.path.join(path_job, files)
        shutil.copy(path_file_origin, path_file_origin2)
        print('Arquivo zip copiado!')
        with ZipFile(path_file_origin2, 'r') as csv:
            for file in csv.namelist():
                print(f'Extraindo {file}!')
                csv.extractall(path_files)
                print(f'{file} Extraido!')
                for csv_extract in os.listdir(path_files):
                    if file in csv_extract:
                        path_file_destino = os.path.join(path_files, csv_extract)
                        print('Carregando CSV no pandas!')
                        df_csv = pd.read_csv(path_file_destino, sep=';', encoding='windows_1258') #UTF-8
                        df_csv.rename(columns=df_colunas_replace, inplace=True)
                        df_csv = df_csv.dropna(subset=['municipio'])
                        pd.to_numeric(df_csv["valor"].str.replace(',','.'))
                        total_rows = df_csv.shape[0]
                        print(f'Transformação realizada, total de {total_rows} registros!')
                        print('Iniciando insert database!')
                        df_csv.to_sql('TB_STG_AUXEMERGENCIAL_DADOSCSV', con=engine, if_exists='replace', index=False, chunksize=1000000)
                        print('Fim de Insert!')

                print('Salvando zip no historico!')        
                shutil.move(path_file_origin2, os.path.join(path_historico, files))
                

                contador += 1

    
    if contador > 0:
        break

print('Processo realizado, checando dados!')

with engine.connect() as conn:
    result = conn.execute(select([func.count()]).select_from(stage_auxemergencial).scalar())
if result == total_rows: 
    logging.info(f'{datetime.now()} - Sucesso')
    print(f'{datetime.now()} - Sucesso')
else:
    logging.warning(f'{datetime.now()} - Total de registros não confere!')
    print(f'{datetime.now()} - Total de registros não confere!')

