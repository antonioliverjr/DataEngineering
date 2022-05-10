import os
import time
import shutil
from database import database_conn
from extract_zip import extract_csv
from transform_spark import tratamento_csv
from loading_data import verifica_dt_set ,loading_csv, flow_final

global dataset_temp
dataset_temp = {
    '2020': ['04','05','06','07','08','09','10','11','12']
    ,'2021': ['01','02','03','04','05','06','07','08','09','10','11','12']
    ,'2022': ['01','02']
}

def inicio_job(ano_mes:str):
    path_caminho = os.path.dirname(os.path.realpath(__file__))
    path_download = os.path.join(path_caminho,'Downloads')
    path_historico = os.path.join(path_caminho,'historico_zip')
    path_files = os.path.join(path_caminho,'csv_aux_emergencial')

    if ano_mes is None or len(ano_mes) < 6:
        return "Ano e Mês invalidos!"
    
    ano = ano_mes[:4]
    mes = ano_mes[-2:]

    if ano not in [ano for ano in dataset_temp.keys()]:
        return False

    if mes not in dataset_temp[ano]:
        return False

    try:
        path_file_name = extract_csv(ano_mes, path_download, path_historico, path_files)
        print('Zip Extraído com sucesso')
    except Exception as error:
        print(error)

    if os.path.exists(path_file_name):
        if tratamento_csv(path_file_name, path_files):
            print('Sucesso no processamento')
        else:
            print('Erro processamento CSV')
    else:
        print('Erro processamento CSV')

    pasta_load = os.listdir(path_files)
    if len(pasta_load) > 0:
        ano_mes = pasta_load[0]
    
    path_execute = os.path.join(path_files, ano_mes)
    if not loading_csv(path_execute):
        print('Erro ao inserir dados no Database.')
    
    shutil.rmtree(path_execute)
    print(f'Dados de {ano_mes} carregados na Stage com sucesso!')

    flow_final()


def main():
    ano_ini = min([ano for ano in dataset_temp.keys()])
    while ano_ini <= max([ano for ano in dataset_temp.keys()]):
        for mes in dataset_temp[ano_ini]:
            if verifica_dt_set(ano_ini, mes):
                continue
            inicio_job(f'{ano_ini}{mes}')
            time.sleep(300)
        ano_ini += 1

if __name__ == '__main__':
    database_conn()
    main()