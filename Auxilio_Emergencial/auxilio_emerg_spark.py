from genericpath import exists
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, count

def tratamento_csv(path_file :str, destino:str):
    spark = SparkSession.builder.getOrCreate()

    if not os.path.exists(path_file): return False

    df = spark.read.csv(path_file, encoding='ISO-8859-1', sep='";"', header=True)
    df = df.toDF(
        'ANO_MES'
        ,'UF'
        ,'COD_MUNICIPIO'
        ,'NOME_MUNICIPIO'
        ,'NIS_BENEF'
        ,'CPF_BENEF'
        ,'NOME_BENEF'
        ,'NIS_RESP'
        ,'CPF_RESP'
        ,'NOME_RESP'
        ,'ENQUADRAMENTO'
        ,'PARCELA'
        ,'OBS'
        ,'VALOR'
    )

    df2 = df.withColumn('ANO_MES', regexp_replace('ANO_MES', '"', ''))\
            .withColumn('VALOR', regexp_replace('VALOR', '"', ''))\
            .withColumn('VALOR', regexp_replace('VALOR', ',', '.'))\
            .withColumn('VALOR', col('VALOR').cast('float'))

    df_final = df2.filter("COD_MUNICIPIO IS NOT NULL")\
                .groupBy(['ANO_MES','COD_MUNICIPIO','ENQUADRAMENTO','PARCELA'])\
                .sum('VALOR')\
                .orderBy(['COD_MUNICIPIO','ENQUADRAMENTO','PARCELA'])

    ano_mes = df_final.select('ANO_MES').limit(1).collect()
    path_destino = destino+'\\'+ano_mes[0][0]

    try: os.mkdir(path_destino)
    except FileExistsError: pass

    file_csv = path_destino+'\\'+'AuxilioEmergencial.csv'
    df_final.toPandas().to_csv(file_csv)

    municipios = df2.select('COD_MUNICIPIO', 'NOME_MUNICIPIO', 'UF')\
                            .distinct().filter("COD_MUNICIPIO IS NOT NULL")

    csv_municipio = path_destino+'\\'+'municipios.csv'
    municipios.toPandas().to_csv(csv_municipio)

    total_beneficiados = df2.filter("COD_MUNICIPIO IS NOT NULL AND CPF_BENEF IS NOT NULL")\
                            .groupBy(['ANO_MES','COD_MUNICIPIO'])\
                            .agg(count('COD_MUNICIPIO').alias('TOTAL_BENEF'))

    csv_beneficiados = path_destino+'\\'+'beneficiados_registrados.csv'
    total_beneficiados.toPandas().to_csv(csv_beneficiados)

    total_anonimos = df2.filter("COD_MUNICIPIO IS NOT NULL AND CPF_BENEF IS NULL")\
                            .groupBy(['ANO_MES','COD_MUNICIPIO'])\
                            .agg(count('COD_MUNICIPIO').alias('TOTAL_ANONIMOS'))

    csv_anonimos = path_destino+'\\'+'beneficiados_anonimos.csv'
    total_anonimos.toPandas().to_csv(csv_anonimos)

    if os.path.exists(file_csv) and os.path.exists(csv_municipio)\
        and os.path.exists(csv_beneficiados) and os.path.exists(csv_anonimos):
        os.remove(path_file)
        return True
    return False