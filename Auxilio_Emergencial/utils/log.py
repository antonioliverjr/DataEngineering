from datetime import datetime
from sqlalchemy.sql import text 
from data.database import conexao, LOG

DATA_HORA_ATUAL = datetime.now()
DATA_ATUAL = str(datetime.date(datetime.now()))
INI = 'PROCESSANDO'
END = 'FINALIZADO'
ERR = 'ERROR'

def register_log(tabela:str, status:str) -> bool:
    data_upt = datetime.now()
    sql_sel_open = text("SELECT COUNT(*) FROM "+LOG+" WHERE TABELA_JOB = :tab AND CAST(DT_INICIO AS DATE) = :dt AND DT_FIM IS NULL")
    sql_insert = text("INSERT INTO "+LOG+" (TABELA_JOB,DT_INICIO,STATUS) VALUES (:tab, :dt, :sts)")
    sql_upt = text("UPDATE "+LOG+" SET STATUS = :sts, DT_FIM = :dtf WHERE TABELA_JOB = :tab AND CAST(DT_INICIO AS DATE) = :dt AND DT_FIM IS NULL")

    with conexao() as conn:
        result = conn.execute(sql_sel_open, {'tab':tabela, 'dt':DATA_ATUAL}).fetchone()
    
    reg = result[0]
    if status.lower() == 'start' and reg > 0:
        with conexao() as conn:
            conn.execute(sql_upt, {'sts': ERR, 'tab':tabela, 'dt':DATA_ATUAL, 'dtf':data_upt})
        with conexao() as conn:
            conn.execute(sql_insert, {'tab':tabela, 'dt':data_upt, 'sts':INI})
    elif status.lower() == 'start' and reg == 0:
        with conexao() as conn:
            conn.execute(sql_insert, {'tab':tabela, 'dt':data_upt, 'sts':INI})
    elif status.lower() == 'end' and reg > 0:
        with conexao() as conn:
            conn.execute(sql_upt, {'sts': END, 'tab':tabela, 'dt':DATA_ATUAL, 'dtf':data_upt})
    elif status.lower() == 'error' and reg > 0:
        with conexao() as conn:
            conn.execute(sql_upt, {'sts': ERR, 'tab':tabela, 'dt':DATA_ATUAL, 'dtf':data_upt})
    else:
        return False
    return True    


if __name__ == '__main__':
    register_log('TB_STG_AUXEMERGENCIAL_BENEFICIOS', 'error')