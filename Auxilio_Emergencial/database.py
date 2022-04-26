from sklearn import datasets
from sqlalchemy import create_engine, Table, Column, String, Integer, Float, MetaData

global dataserver
dataserver = MetaData()

def database_conn():
    engine = create_engine("mssql+pyodbc://project:Jrdbsql@localhost\\SQLEXPRESS/GOVBR?driver=ODBC+Driver+17+for+SQL+Server")
    
    stage_beneficios = Table(
        'TB_STG_AUXEMERGENCIAL_BENEFICIOS'
        ,dataserver
        ,Column("ANO_MES", Integer)
        ,Column("COD_MUNICIPIO", Integer)
        ,Column("ENQUADRAMENTO", String(10))
        ,Column("PARCELA", String(3))
        ,Column("TOTAL_PAGO", Float(3))
    )

    stage_registrados = Table(
        'TB_STG_AUXEMERGENCIAL_BENEFICIADOS_REGISTRADOS'
        ,dataserver
        ,Column("ANO_MES", Integer)
        ,Column("COD_MUNICIPIO", Integer)
        ,Column("TOTAL_BENEF", Integer)
    )

    stage_anonimos = Table(
        'TB_STG_AUXEMERGENCIAL_BENEFICIADOS_ANONIMOS'
        ,dataserver
        ,Column("ANO_MES", Integer)
        ,Column("COD_MUNICIPIO", Integer)
        ,Column("TOTAL_ANONIMOS", Integer)
    )

    stage_municipios = Table(
        'TB_STG_AUXEMERGENCIAL_MUNICIPIOS'
        ,dataserver
        ,Column("COD_MUNICIPIO", Integer)
        ,Column("NOME_MUNICIPIO", String(100))
        ,Column("UF", String(2))
    )

    dataserver.create_all(engine, checkfirst=True)
    return engine
    
def tables_server():
    for table in dataserver.sorted_tables:
        print(table)

if __name__ == '__main__':
    engine = database_conn()
    tables_server()