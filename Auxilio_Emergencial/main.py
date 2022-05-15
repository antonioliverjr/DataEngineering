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
        print("Iniciando captura e extração dos arquivos CSV...")
        path_file_name = extract_csv(ano_mes, path_download, path_historico, path_files)
        print('Zip Extraído com sucesso, arquivo csv disponível!')
    except Exception as error:
        print(error)

    if os.path.exists(path_file_name):
        print("Iniciando processo de tratamento do CSV...")
        if tratamento_csv(path_file_name, path_files):
            print('Processamento executado com sucesso!')
        else:
            print('Erro no processamento CSV!')
    else:
        print('Erro no processamento CSV!')

    pasta_load = os.listdir(path_files)
    if len(pasta_load) > 0:
        ano_mes = pasta_load[0]
    
    path_execute = os.path.join(path_files, ano_mes)
    print("Inserindo dados do CSV no Database...")
    if not loading_csv(path_execute):
        print('Erro ao inserir dados no Database!')
    
    print("Limpando pastas...")
    shutil.rmtree(path_execute)
    print(f'Dados de {ano_mes} carregados na Stage com sucesso!')

    print("Iniciando flow final...")
    flow_final()
    print(f'Dados de {ano_mes} carregados nas tabelas principais!')


def main():
    print("Iniciando verificação de periodo da carga...")
    ano_ini = min([ano for ano in dataset_temp.keys()])
    contagem = 0
    while int(ano_ini) <= int(max([ano for ano in dataset_temp.keys()])):
        for mes in dataset_temp[ano_ini]:
            if verifica_dt_set(ano_ini, mes):
                print(f"Carga {ano_ini}{mes} já efetuado!")
                continue
            print(f"Iniciando carga de {ano_ini}{mes}...")
            inicio_job(f'{ano_ini}{mes}')
            contagem += 1
            #time.sleep(300)
            break
        if contagem == 1: break
        ano_ini = str(int(ano_ini)+1)

if __name__ == '__main__':
    database_conn()
    main()