import os
import shutil
import requests
from zipfile import ZipFile

def extract_csv(ano_mes:str, path_download:str, path_historico:str, path_files:str) -> str:
    url_download = 'https://www.portaldatransparencia.gov.br/download-de-dados/auxilio-emergencial/'

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
        url_get = url_download+ano_mes
        local_file = os.path.join(path_download,file_name)
        with requests.get(url_get, stream=True) as zip:
            zip.raise_for_status()
            with open(local_file, 'wb') as file:
                for bites in zip.iter_content(chunk_size=8192):
                    file.write(bites)
        shutil.copy(path_file_origin, path_file_job)
    elif len(files_historico) > 0:
        for files in files_historico:
            if file_name in files:
                break
        else:
            for files in files_download:
                if file_name in files:
                    shutil.move(path_file_origin, path_file_job)
                    break
            else:
                url_get = url_download+ano_mes
                local_file = os.path.join(path_download,file_name)
                with requests.get(url_get, stream=True) as zip:
                    zip.raise_for_status()
                    with open(local_file, 'wb') as file:
                        for bites in zip.iter_content(chunk_size=8192):
                            file.write(bites)
                shutil.move(path_file_origin, path_file_job)
    else:
        for files in files_download:
            if file_name in files:                
                shutil.move(path_file_origin, path_file_job)
                break
        else:
            url_get = url_download+ano_mes
            local_file = os.path.join(path_download,file_name)
            with requests.get(url_get, stream=True) as zip:
                zip.raise_for_status()
                with open(local_file, 'wb') as file:
                    for bites in zip.iter_content(chunk_size=8192):
                        file.write(bites)        
            shutil.move(path_file_origin, path_file_job)
    
    with ZipFile(path_file_job, 'r') as csv:
        for file in csv.namelist():
            csv_name_file = file
        csv.extractall(path_files)

    return os.path.join(path_files, csv_name_file)

if __name__ == '__main__':
    print(extract_csv('202103'
                    ,'C:\\Users\\antoliverjr\\Downloads'
                    ,'C:\\Workspace\\DataAnalytics\\Auxilio_Emergencial\\historico_zip'
                    ,'C:\\Workspace\\DataAnalytics\\Auxilio_Emergencial\\csv_aux_emergencial'
    ))
