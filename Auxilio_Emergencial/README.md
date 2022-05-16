![](https://www.python.org/static/img/python-logo.png)

# Anotações do projeto - Auxilio Emergencial GOVBR

> Fontes de Dados para Analise
- Dataset: https://www.portaldatransparencia.gov.br/download-de-dados/auxilio-emergencial
- Dados Geométricos: https://github.com/kelvins/Municipios-Brasileiros

> Ferramentas utilizadas
- SQL Server Express 2019
- Power BI Desktop
- Editor de Texto (VSCode) - Python

> Bibliotecas Utilizadas
- *Anaconda3 (Path)*
- PySpark
- Pandas
- Pyodbc
- SQLAlchemy
- python-decouple

Obs.: Caso queira utilizar interpretador python.org instalar Pandas e SQLAlchemy

> Built-in
- os
- shutil
- request
- zipfile

> Comandos Terminal
```bash
conda install -c conda-forge pyspark
```

> Estrutura de Projeto
- tabelas_database.sql (Script T-SQL Tabelas, Procedures e View "SQL Server")
- main.py (Start)
- extract_zip.py (Get dataset e extração para csv)
- transform_spark.py (Extração do dataset para arquivos csv transformados conforme modelagem do DW)
- loading_data.py (Carga dos arquivos csv para stage e execução das procedures de tratamento e carga para tabelas finais)
- dados_geometricos.py (Captação de dados para geolocalização)
- database.py (Engine, Schema e Criação das tabelas stages)
- list_procedures.py (Arquivo com lista de sequência de procedures e executor)
- log.py (Execução de registros do processo de carga)
- .env (Arquivo para armazenamento dados sensíveis)


Obs.: Projeto em andamento, principais etapas de ETl construidas e algumas analises preliminares, iniciando etapas de relatórios com criação de KPI e estudos de possíveis agregação de dados relativos para complementação da analise.

Obs2.: Após conclusão do dashboard, será criado aplicação com PyQT5 que será o programada para carga dos dados.
