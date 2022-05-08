import os
import shutil
import requests
from zipfile import ZipFile
from sqlalchemy.sql import text
from database import conexao, TB_PATH
from log import DATA_HORA_ATUAL


def register_path(nome_file:str, caminho_arquivo:str):
    sql_sel_exist = text("SELECT COUNT(*) FROM "+TB_PATH+" WHERE NM_ARQUIVO_CSV = :file AND NM_CAMINHO_CSV = :path")
    sql_insert = text("INSERT INTO "+TB_PATH+" (NM_ARQUIVO_CSV,NM_CAMINHO_CSV,DT_ATUALIZACAO) VALUES (:file, :path, :dt)")
    
    with conexao() as conn:
        result = conn.execute(sql_sel_exist, {'file':nome_file, 'path':caminho_arquivo}).fetchone()
    
    reg = result[0]

    if reg == 0:
        with conexao() as conn:
            conn.execute(sql_insert, {'file':nome_file, 'path':caminho_arquivo, 'dt':DATA_HORA_ATUAL})

def get_municipios_csv(path_download:str, path_destino:str) -> str:
    path_mun_csv = os.path.join(path_destino, 'base_municipios')
    zip_file = 'main.zip'
    file_name = 'municipios.csv'

    try: os.mkdir(path_download)
    except FileExistsError: pass
    
    try: os.mkdir(path_mun_csv)
    except FileExistsError: pass

    url_get = 'https://github.com/kelvins/Municipios-Brasileiros/archive/refs/heads/main.zip'
    
    files = os.listdir(path_mun_csv)
    local_file = os.path.join(path_download,zip_file)

    if len(files) == 0 or file_name not in os.listdir(path_mun_csv):
        with requests.get(url_get, stream=True) as zip:
            zip.raise_for_status()
            with open(local_file, 'wb') as file:
                for bites in zip.iter_content(chunk_size=8192):
                    file.write(bites)
    
    caminho_arq_final = os.path.join(path_mun_csv,file_name)

    if os.path.exists(local_file):
        with ZipFile(local_file, 'r') as zip:
            for pasta in zip.namelist():
                if file_name in pasta:
                    pasta_csv = pasta
            zip.extract(pasta_csv, path_download)
        
        shutil.copy(os.path.join(path_download,pasta_csv), caminho_arq_final)

    register_path(file_name,str(caminho_arq_final))

if __name__ == '__main__':
    path_caminho = os.path.dirname(os.path.realpath(__file__))
    path_download = os.path.join(path_caminho,'Downloads')
    path_files = os.path.join(path_caminho,'csv_aux_emergencial')
    get_municipios_csv(path_download,path_files)