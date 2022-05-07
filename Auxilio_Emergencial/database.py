import os
from sqlalchemy import create_engine, Table, Column, String, Integer, Float, MetaData, DateTime

TB_BENEFICIOS = 'TB_STG_AUXEMERGENCIAL_BENEFICIOS'
TB_REGISTRADOS = 'TB_STG_AUXEMERGENCIAL_BENEFICIADOS_REGISTRADOS'
TB_ANONIMOS = 'TB_STG_AUXEMERGENCIAL_BENEFICIADOS_ANONIMOS'
TB_MUNICIPIOS = 'TB_STG_AUXEMERGENCIAL_MUNICIPIOS'
LOG = 'TB_LOG'

global dataserver
dataserver = MetaData()

global engine
engine = create_engine("mssql+pyodbc://project:Jrdbsql@localhost\\SQLEXPRESS/GOVBR?driver=ODBC+Driver+17+for+SQL+Server") 

def database_conn():
    stage_beneficios = Table(
        TB_BENEFICIOS
        ,dataserver
        ,Column("ANO_MES", Integer)
        ,Column("COD_MUNICIPIO", Integer)
        ,Column("ENQUADRAMENTO", String(10))
        ,Column("PARCELA", String(3))
        ,Column("TOTAL_PAGO", Float(3))
    )

    stage_registrados = Table(
        TB_REGISTRADOS
        ,dataserver
        ,Column("ANO_MES", Integer)
        ,Column("COD_MUNICIPIO", Integer)
        ,Column("TOTAL_BENEF", Integer)
    )

    stage_anonimos = Table(
        TB_ANONIMOS
        ,dataserver
        ,Column("ANO_MES", Integer)
        ,Column("COD_MUNICIPIO", Integer)
        ,Column("TOTAL_ANONIMOS", Integer)
    )

    stage_municipios = Table(
        TB_MUNICIPIOS
        ,dataserver
        ,Column("COD_MUNICIPIO", Integer)
        ,Column("NOME_MUNICIPIO", String(100))
        ,Column("UF", String(2))
    )

    tabela_log = Table(
        LOG
        ,dataserver
        ,Column("ID", Integer, primary_key=True)
        ,Column("TABELA_JOB", String(50), nullable=False)
        ,Column("DT_INICIO", DateTime)
        ,Column("DT_FIM", DateTime)
        ,Column("STATUS", String(15), nullable=False)
    )

    dataserver.create_all(engine, checkfirst=True)
    
def tables_server():
    for table in dataserver.sorted_tables:
        print(table)

def conexao():
    return engine.connect()

def store_procedure(procedure:str):
    path_caminho = os.path.dirname(os.path.realpath(__file__))
    path_cmd = path_caminho+'\Execucao_Procedures\Execucao_Procedures'
    comando_cmd = f'dotnet run --project {path_cmd} {procedure}'
    try:
        os.system(comando_cmd)
    except Exception as err:
        return err
    return True

if __name__ == '__main__':
    engine = database_conn()
    tables_server()