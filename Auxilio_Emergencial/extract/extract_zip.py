import os
import shutil
import requests
from zipfile import ZipFile
from extract.dados_geometricos import get_municipios_csv


def extract_csv(ano_mes:str, path_download:str, path_historico:str, path_files:str) -> str:
    url_download = 'https://www.portaldatransparencia.gov.br/download-de-dados/auxilio-emergencial/'

    try: os.mkdir(path_download)
    except FileExistsError: pass

    try: os.mkdir(path_files)
    except FileExistsError: pass

    try: os.mkdir(path_historico)
    except FileExistsError: pass

    file_name = ano_mes+'_AuxilioEmergencial.zip'
    path_file_origin = os.path.join(path_download, file_name)
    path_file_job = os.path.join(path_historico, file_name)

    files_historico = os.listdir(path_historico)
    files_download = os.listdir(path_download)

    if len(files_historico) == 0 and len(files_download) == 0:
        print(f"Realizando download {file_name}...")
        url_get = url_download+ano_mes
        local_file = os.path.join(path_download,file_name)
        with requests.get(url_get, stream=True) as zip:
            zip.raise_for_status()
            with open(local_file, 'wb') as file:
                for bites in zip.iter_content(chunk_size=8192):
                    file.write(bites)
        shutil.copy(path_file_origin, path_file_job)
        print(f"Download realizado, {file_name} na {path_historico}!")
    elif len(files_historico) > 0:
        print("Verificando historico de arquivos...")
        for files in files_historico:
            if file_name in files:
                print("Arquivo no historico!")
                break
        else:
            for files in files_download:
                if file_name in files:
                    shutil.move(path_file_origin, path_file_job)
                    print(f"Arquivo já baixado, copia do {file_name} no historico!")
                    break
            else:
                print(f"Realizando download {file_name}...")
                url_get = url_download+ano_mes
                local_file = os.path.join(path_download,file_name)
                with requests.get(url_get, stream=True) as zip:
                    zip.raise_for_status()
                    with open(local_file, 'wb') as file:
                        for bites in zip.iter_content(chunk_size=8192):
                            file.write(bites)
                shutil.move(path_file_origin, path_file_job)
                print(f"Download realizado, {file_name} na {path_historico}!")
    else:
        for files in files_download:
            if file_name in files:                
                shutil.move(path_file_origin, path_file_job)
                print(f"Arquivo já baixado, copia do {file_name} no historico!")
                break
        else:
            print(f"Realizando download {file_name}...")
            url_get = url_download+ano_mes
            local_file = os.path.join(path_download,file_name)
            with requests.get(url_get, stream=True) as zip:
                zip.raise_for_status()
                with open(local_file, 'wb') as file:
                    for bites in zip.iter_content(chunk_size=8192):
                        file.write(bites)        
            shutil.move(path_file_origin, path_file_job)
            print(f"Download realizado, {file_name} na {path_historico}!")
    
    with ZipFile(path_file_job, 'r') as csv:
        for file in csv.namelist():
            csv_name_file = file
        if os.path.exists(os.path.join(path_files, csv_name_file)):
            return os.path.join(path_files, csv_name_file)        
        csv.extractall(path_files)
    print(f"{csv_name_file} extraido com sucesso!")

    print("Atualizando dados de Municipios...")
    get_municipios_csv(path_download, path_files)
    print("Dados de Municipios atualizados!")

    print("Limpando pastas...")
    shutil.rmtree(path_download)
    return os.path.join(path_files, csv_name_file)

if __name__ == '__main__':
    print(extract_csv('202103','Downloads','historico_zip','csv_aux_emergencial'))
