import pyodbc
from typing import Union
from data.database import user, senha, host


list_procedures_flow = {
    '1': 'PRC_CARGA_TB_DIM_DATA'
    ,'2': 'PRC_CARGA_TB_AUXEMERGENCIAL_MUNICIPIOS'
    ,'3': 'PRC_CARGA_TB_AUXEMERGENCIAL_BENEFICIOS'
    ,'4': 'PRC_CARGA_TB_AUXEMERGENCIAL_BENEFICIOS_POR_ENQUADRAMENTO'
    ,'5': 'PRC_CARGA_TB_AUXEMERGENCIAL_BENEFICIADOS'
    ,'6': 'PRC_INSERT_BASE_MUNICIPIOS'
    ,'7': 'PRC_LIMPA_STG_AUXEMERGENCIAL'
}

def store_procedure(procedure:str) -> Union[bool,Exception]:
    context = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+host+'\\SQLEXPRESS;DATABASE=GOVBR;UID='+user+';PWD='+senha
        ,autocommit=True
    )

    comando_sql = '{CALL '+procedure+'}'
    cursor = context.cursor()

    try:
        cursor.execute(comando_sql)
    except Exception as err:
        return err
    finally:
        cursor.close()
        context.close()

    return True

if __name__ == '__main__':
    store_procedure('PRC_CARGA_TB_DIM_DATA')