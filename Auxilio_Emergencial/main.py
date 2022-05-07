import os
import shutil
from database import database_conn
from extract_zip import extract_csv
from transform_spark import tratamento_csv
from loading_data import loading_csv, flow_final

def main():
    path_caminho = os.path.dirname(os.path.realpath(__file__))
    path_download = os.path.join(path_caminho,'Downloads')
    path_historico = os.path.join(path_caminho,'historico_zip')
    path_files = os.path.join(path_caminho,'csv_aux_emergencial')
    
    try:
        path_file_name = extract_csv('202004', path_download, path_historico, path_files)
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

if __name__ == '__main__':
    database_conn()
    main()