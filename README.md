![](https://www.python.org/static/img/python-logo.png)
# Analise e Desenvolvimento com Python

**Aprimorando o conceito de BI e ETL, com foco em Engenharia de Dados**

## Praticando Spark e Pandas! Aprimorando técnicas de Dataviz!

> *Projeto pessoal para utilizar como **Base Python** na criação de:*

- **ETL**
- **Tratamentos com Spark**
- **Dataviz com WebApp**

# Anotações do projeto - Auxilio Emergencial GOVBR

> Fontes de Dados para Analise
- Dataset: https://www.portaldatransparencia.gov.br/download-de-dados/auxilio-emergencial
- Dados Geométricos: https://github.com/kelvins/Municipios-Brasileiros

> Bibliotecas Utilizadas
- *Anaconda3 (Path)*
- PySpark
- Pandas
- SQLAlchemy

Obs.: Caso queira utilizar interpretador python.org instalar Pandas e SQLAlchemy

> Built-in
- os
- shutil
- request
- zipfile

> Comandos Bash
```bash
conda install -c conda-forge pyspark
```
```bash
conda install -c conda-forge geopandas
```
> Estrutura de Projeto
- tabelas_database.sql (Script T-SQL Tabelas, Procedures e View "SQL Server")
- main.py (Start)
- extract_zip.py (Get dataset e extração para csv)
- transform_spark.py (Extração do dataset para arquivos csv transformados conforme modelagem do DW)
- loading_data.py (Carga dos arquivos csv para stage e execução das procedures de tratamento e carga para tabelas finais)
- dados_geometricos.py (Captação de dados para geolocalização)
- database.py (Engine, Schema e Criação das tabelas stages)
- log.py (Execução de registros do processo de carga)
- list_procedures.py (Arquivo com lista de sequência de procedures a executar)


