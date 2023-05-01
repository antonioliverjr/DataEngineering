--########### SCRIPT MODELAGEM SQL SERVER ###########
--############### CRIAÇÃO DE DATABASE ###############
CREATE DATABASE [GOVBR]
GO

--################ SELEÇÃO DATABASE #################
USE [GOVBR]
GO

--############### CRIAÇÃO DE TABELAS PRINCIPAIS ################
CREATE TABLE [TB_AUXEMERGENCIAL_MUNICIPIOS]
(
	[NR_ID] INT IDENTITY(1,1) PRIMARY KEY
	,[NR_COD_MUNICIPIO] BIGINT NOT NULL
	,[NM_NOME_MUNICIPIO] VARCHAR(50) NOT NULL
	,[NM_UF] VARCHAR(2) NOT NULL
	,[NM_MUNICIPIO_UF] VARCHAR(55) NOT NULL
)WITH(DATA_COMPRESSION=PAGE)
CREATE INDEX IDX_AUXEMERGENCIAL_MUNICIPIOS_NR_COD_MUNICIPIO ON TB_AUXEMERGENCIAL_MUNICIPIOS (NR_COD_MUNICIPIO)
GO

CREATE TABLE [TB_AUXEMERGENCIAL_BENEFICIADOS](
	[NR_ID] INT PRIMARY KEY IDENTITY(1,1)
	,[NM_ANO] VARCHAR(4) NOT NULL
	,[NM_MES] VARCHAR(2) NOT NULL
	,[NR_COD_MUNICIPIO] BIGINT NOT NULL
	,[NR_TOTAL_REGISTRADOS] BIGINT NULL
	,[NR_TOTAL_ANONIMOS] BIGINT NULL
)WITH(DATA_COMPRESSION=PAGE)
CREATE INDEX AUXEMERGENCIAL_BENEFICIADOS_NM_ANO_MES ON TB_AUXEMERGENCIAL_BENEFICIADOS (NM_ANO, NM_MES)
GO

CREATE TABLE [TB_AUXEMERGENCIAL_BENEFICIOS](
	[NR_ID] INT PRIMARY KEY IDENTITY(1,1)
	,[NM_ANO] VARCHAR(4) NOT NULL
	,[NM_MES] VARCHAR(2) NOT NULL
	,[NR_COD_MUNICIPIO] BIGINT NOT NULL
	,[NM_ENQUADRAMENTO] VARCHAR(15) NOT NULL
	,[NR_PARCELA] INT NOT NULL
	,[NR_TOTAL_PAGO] DECIMAL(15,5) NOT NULL
)WITH(DATA_COMPRESSION=PAGE)
CREATE INDEX AUXEMERGENCIAL_BENEFICIOS_NM_ANO_MES ON TB_AUXEMERGENCIAL_BENEFICIOS (NM_ANO, NM_MES)
GO

CREATE TABLE [TB_AUXEMERGENCIAL_BENEFICIOS_POR_ENQUADRAMENTO] (
	[NR_ID] INT PRIMARY KEY IDENTITY(1,1)
	,[NM_ANO] VARCHAR(4) NOT NULL
	,[NM_MES] VARCHAR (2) NOT NULL
	,[NR_COD_MUNICIPIO] BIGINT NOT NULL
	,[NM_ENQUADRAMENTO] VARCHAR(15) NOT NULL
	,[NR_TOTAL_PAGO] DECIMAL(15,5) NOT NULL
	,[NR_TOTAL_PAGO_PERCENT] DECIMAL(15,5) NOT NULL
)WITH(DATA_COMPRESSION=PAGE)
CREATE INDEX AUXEMERGENCIAL_BENEFICIOS_POR_ENQUADRAMENTO_NR_ANO_MES ON TB_AUXEMERGENCIAL_BENEFICIOS_POR_ENQUADRAMENTO (NM_ANO, NM_MES)
GO

CREATE TABLE [TB_DIM_DATA](
	[NM_ANO] VARCHAR(4)
	,[NM_MES] VARCHAR(2)
	,[DT_DATA] DATE
)WITH(DATA_COMPRESSION=PAGE)
CREATE CLUSTERED INDEX DIM_DATA_DT_DATA ON TB_DIM_DATA (DT_DATA)
GO

CREATE TABLE [TB_AUX_CAMINHO_CSV] (
	[NR_ID] INT IDENTITY(1,1) PRIMARY KEY
	,[NM_ARQUIVO_CSV] VARCHAR(100)
	,[NM_CAMINHO_CSV] VARCHAR(MAX)
	,[DT_ATUALIZACAO] DATETIME
)
GO

CREATE TABLE [TB_BASE_MUNICIPIOS]
(
	[NR_ID] INT IDENTITY(1,1) PRIMARY KEY
	,[NR_COD_MUNICIPIO] BIGINT NOT NULL
	,[NR_LATITUDE] DECIMAL(15,5) NULL
	,[NR_LONGITUDE] DECIMAL(15,5) NULL
)WITH(DATA_COMPRESSION=PAGE)
CREATE INDEX IDX_BASE_MUNICIPIOS_NR_COD_MUNICIPIO ON TB_BASE_MUNICIPIOS (NR_COD_MUNICIPIO)
GO

CREATE TABLE [TB_BASE_ESTADOS](
	[NR_ID] INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
	[NM_UF_ESTADO] VARCHAR(2) NOT NULL,
	[NR_LATITUDE] DECIMAL(15, 5) NULL,
	[NR_LONGITUDE] DECIMAL(15, 5) NULL,
)
GO

--############### CRIAÇÃO DE PROCEDURES ###############
CREATE PROCEDURE PRC_CARGA_TB_AUXEMERGENCIAL_MUNICIPIOS AS

SET NOCOUNT ON

BEGIN
    INSERT INTO [TB_AUXEMERGENCIAL_MUNICIPIOS]
	SELECT DISTINCT
		[COD_MUNICIPIO]
		,[NOME_MUNICIPIO]
		,[UF]
		,CONCAT([NOME_MUNICIPIO],' - ',[UF]) AS MUNICIPIO_UF
	FROM [TB_STG_AUXEMERGENCIAL_MUNICIPIOS]
	WHERE [COD_MUNICIPIO] NOT IN (SELECT [NR_COD_MUNICIPIO] 
									FROM [TB_AUXEMERGENCIAL_MUNICIPIOS]
								 )
END
GO

CREATE PROCEDURE PRC_CARGA_TB_AUXEMERGENCIAL_BENEFICIOS AS

SET NOCOUNT ON

BEGIN
	DECLARE @ANO VARCHAR(4), @MES VARCHAR(2)
	SET @ANO = (SELECT DISTINCT TOP 1 LEFT([ANO_MES],4) FROM [TB_STG_AUXEMERGENCIAL_BENEFICIOS])
	SET @MES = (SELECT DISTINCT TOP 1 RIGHT([ANO_MES],2) FROM [TB_STG_AUXEMERGENCIAL_BENEFICIOS])

	DELETE FROM [TB_AUXEMERGENCIAL_BENEFICIOS]
	WHERE [NM_ANO] = @ANO AND [NM_MES] = @MES
	
	INSERT INTO [TB_AUXEMERGENCIAL_BENEFICIOS]
	SELECT
		LEFT([ANO_MES],4) AS ANO
		,RIGHT([ANO_MES],2) AS MES
        ,[COD_MUNICIPIO]
        ,[ENQUADRAMENTO]
        ,REPLACE([PARCELA],'ª','') AS PARCELA
        ,CAST([TOTAL_PAGO] AS DECIMAL(15,5)) AS TOTAL_PAGO
	FROM [TB_STG_AUXEMERGENCIAL_BENEFICIOS]
	WHERE [ANO_MES] = CONCAT(@ANO,@MES)
END
GO

CREATE PROCEDURE PRC_CARGA_TB_AUXEMERGENCIAL_BENEFICIADOS AS

SET NOCOUNT ON

BEGIN
	IF OBJECT_ID(N'tempdb..#MUNICIPIOS_BENEFICIADOS', N'U') IS NOT NULL BEGIN DROP TABLE #MUNICIPIOS_BENEFICIADOS END
	SELECT
		LEFT([ANO_MES],4) AS ANO
		,RIGHT([ANO_MES],2) AS MES
		,[COD_MUNICIPIO]
	INTO #MUNICIPIOS_BENEFICIADOS
	FROM [TB_STG_AUXEMERGENCIAL_BENEFICIADOS_REGISTRADOS]

	UNION ALL

	SELECT 
		LEFT([ANO_MES],4) AS ANO
		,RIGHT([ANO_MES],2) AS MES
		,[COD_MUNICIPIO]
	FROM [TB_STG_AUXEMERGENCIAL_BENEFICIADOS_ANONIMOS]


	INSERT INTO [TB_AUXEMERGENCIAL_BENEFICIADOS](
		[NM_ANO]
		,[NM_MES]
		,[NR_COD_MUNICIPIO]
		,[NR_TOTAL_REGISTRADOS]
		,[NR_TOTAL_ANONIMOS]
	)
	SELECT DISTINCT
		A.[ANO]
		,A.[MES]
		,A.[COD_MUNICIPIO]
		,0 AS TOTAL_REGISTRADOS
		,0 AS TOTAL_ANONIMOS
	FROM #MUNICIPIOS_BENEFICIADOS A
	LEFT JOIN [TB_AUXEMERGENCIAL_BENEFICIADOS] B
	ON B.[NR_COD_MUNICIPIO] = A.[COD_MUNICIPIO]
	AND B.[NM_ANO] = A.[ANO] AND B.[NM_MES]=A.[MES]
	WHERE B.[NR_COD_MUNICIPIO] IS NULL

    IF OBJECT_ID(N'tempdb..#MUNICIPIOS_BENEFICIADOS', N'U') IS NOT NULL BEGIN DROP TABLE #MUNICIPIOS_BENEFICIADOS END

	UPDATE A 
	SET A.[NR_TOTAL_REGISTRADOS] = ISNULL(B.[TOTAL_BENEF],0)
	FROM [TB_AUXEMERGENCIAL_BENEFICIADOS] A
	INNER JOIN [TB_STG_AUXEMERGENCIAL_BENEFICIADOS_REGISTRADOS] B
	ON B.[COD_MUNICIPIO]=A.[NR_COD_MUNICIPIO]
	AND LEFT(B.[ANO_MES],4) = A.[NM_ANO] 
	AND RIGHT(B.[ANO_MES],2) = A.[NM_MES]

	UPDATE A
	SET A.[NR_TOTAL_ANONIMOS] = ISNULL(B.[TOTAL_ANONIMOS],0)
	FROM [TB_AUXEMERGENCIAL_BENEFICIADOS] A
	INNER JOIN [TB_STG_AUXEMERGENCIAL_BENEFICIADOS_ANONIMOS] B
	ON B.[COD_MUNICIPIO]=A.[NR_COD_MUNICIPIO]
	AND LEFT(B.[ANO_MES],4) = A.[NM_ANO] 
	AND RIGHT(B.[ANO_MES],2) = A.[NM_MES]
END
GO

CREATE PROCEDURE [dbo].[PRC_CARGA_TB_AUXEMERGENCIAL_BENEFICIOS_POR_ENQUADRAMENTO] AS

SET NOCOUNT ON

BEGIN
	DECLARE @ANO VARCHAR(4), @MES VARCHAR(2)
	SET @ANO = (SELECT DISTINCT TOP 1 LEFT([ANO_MES],4) FROM [TB_STG_AUXEMERGENCIAL_BENEFICIOS])
	SET @MES = (SELECT DISTINCT TOP 1 RIGHT([ANO_MES],2) FROM [TB_STG_AUXEMERGENCIAL_BENEFICIOS])

	IF OBJECT_ID(N'tempdb..#TEMP_MUN_TOTAL', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_MUN_TOTAL END
	IF OBJECT_ID(N'tempdb..#TEMP_REGISTROS_MUN_ENQ', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_REGISTROS_MUN_ENQ END
	IF OBJECT_ID(N'tempdb..#TEMP_BENEFICIOS_ENQUADRAMENTO', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_BENEFICIOS_ENQUADRAMENTO END

	SELECT
		[NM_ANO]
		,[NM_MES]
		,[NR_COD_MUNICIPIO]
		,SUM([NR_TOTAL_PAGO]) AS NR_TOTAL_PAGO
	INTO #TEMP_MUN_TOTAL
	FROM [TB_AUXEMERGENCIAL_BENEFICIOS] WITH(READPAST)
	WHERE [NM_ANO] = @ANO AND [NM_MES] = @MES
	GROUP BY [NM_ANO],[NM_MES],[NR_COD_MUNICIPIO]
	ORDER BY [NR_COD_MUNICIPIO]
	

	SELECT 
		BENEF.[NM_ANO]
		,BENEF.[NM_MES]
		,BENEF.[NR_COD_MUNICIPIO]
		,BENEF.[NM_ENQUADRAMENTO]
		,SUM([NR_TOTAL_PAGO]) AS NR_TOTAL_PAGO
	INTO #TEMP_REGISTROS_MUN_ENQ
	FROM [TB_AUXEMERGENCIAL_BENEFICIOS] BENEF WITH(READPAST)
	LEFT JOIN [TB_AUXEMERGENCIAL_MUNICIPIOS] MUN WITH(READPAST)
	ON MUN.[NR_COD_MUNICIPIO]=BENEF.[NR_COD_MUNICIPIO]
	WHERE BENEF.[NM_ANO] = @ANO AND BENEF.[NM_MES] = @MES
	GROUP BY BENEF.[NM_ANO],BENEF.[NM_MES],BENEF.[NR_COD_MUNICIPIO],BENEF.[NM_ENQUADRAMENTO]
	
	
	SELECT
		REG.*
		,REG.[NR_TOTAL_PAGO] / TOT.[NR_TOTAL_PAGO] AS NR_TOTAL_PAGO_PERCENT
	INTO #TEMP_BENEFICIOS_ENQUADRAMENTO
	FROM #TEMP_REGISTROS_MUN_ENQ REG
	LEFT JOIN #TEMP_MUN_TOTAL TOT ON TOT.[NR_COD_MUNICIPIO] = REG.[NR_COD_MUNICIPIO]


	DELETE FROM [TB_AUXEMERGENCIAL_BENEFICIOS_POR_ENQUADRAMENTO]
	WHERE [NM_ANO] = @ANO AND [NM_MES] = @MES
	

	INSERT INTO [TB_AUXEMERGENCIAL_BENEFICIOS_POR_ENQUADRAMENTO]
	SELECT
		[NM_ANO]
		,[NM_MES]
		,[NR_COD_MUNICIPIO]
		,[NM_ENQUADRAMENTO]
		,[NR_TOTAL_PAGO]
		,[NR_TOTAL_PAGO_PERCENT]
	FROM #TEMP_BENEFICIOS_ENQUADRAMENTO
	ORDER BY [NR_COD_MUNICIPIO]


	IF OBJECT_ID(N'tempdb..#TEMP_MUN_TOTAL', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_MUN_TOTAL END
	IF OBJECT_ID(N'tempdb..#TEMP_REGISTROS_MUN_ENQ', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_REGISTROS_MUN_ENQ END
	IF OBJECT_ID(N'tempdb..#TEMP_BENEFICIOS_ENQUADRAMENTO', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_BENEFICIOS_ENQUADRAMENTO END
END
GO

CREATE PROCEDURE [PRC_CARGA_TB_DIM_DATA] AS

SET NOCOUNT ON

BEGIN
	DECLARE @ANO VARCHAR(4), @MES VARCHAR(2), @EXISTE INT

	SET @ANO = (SELECT DISTINCT TOP 1 LEFT([ANO_MES],4) FROM [TB_STG_AUXEMERGENCIAL_BENEFICIOS])
	SET @MES = (SELECT DISTINCT TOP 1 RIGHT([ANO_MES],2) FROM [TB_STG_AUXEMERGENCIAL_BENEFICIOS])
	SET @EXISTE = (SELECT COUNT(*) FROM [TB_DIM_DATA] WHERE [NM_ANO]=@ANO AND [NM_MES]=@MES)

	IF @EXISTE = 0
	BEGIN
		INSERT INTO [TB_DIM_DATA] (NM_ANO, NM_MES, DT_DATA)
		VALUES(@ANO,@MES,DATEFROMPARTS(@ANO,@MES,'01'))
	END
END
GO

CREATE PROCEDURE [PRC_INSERT_BASE_MUNICIPIOS] AS

SET NOCOUNT ON

BEGIN
DECLARE @CAMINHO_CSV AS VARCHAR(Max) = (SELECT NM_CAMINHO_CSV FROM (
																	SELECT DISTINCT 
																		[NM_CAMINHO_CSV]
																		,MAX(DT_ATUALIZACAO) DT_ATUALIZACAO
																	FROM [GOVBR].[DBO].[TB_AUX_CAMINHO_CSV]
																	WHERE [NM_ARQUIVO_CSV] = 'municipios.csv'
																	GROUP BY [NM_CAMINHO_CSV]) AS CSV
																	)

DECLARE @CAMINHO_CSV2 AS VARCHAR(Max) = (SELECT NM_CAMINHO_CSV FROM (
																	SELECT DISTINCT 
																		[NM_CAMINHO_CSV]
																		,MAX(DT_ATUALIZACAO) DT_ATUALIZACAO
																	FROM [GOVBR].[DBO].[TB_AUX_CAMINHO_CSV]
																	WHERE [NM_ARQUIVO_CSV] = 'estados.csv'
																	GROUP BY [NM_CAMINHO_CSV]) AS CSV
																	)
DECLARE @SQL NVARCHAR(MAX)

IF @CAMINHO_CSV IS NOT NULL OR @CAMINHO_CSV <> ''
BEGIN
	IF OBJECT_ID(N'tempdb..#TEMP_MUNICIPIO_CSV', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_MUNICIPIO_CSV END
	CREATE TABLE #TEMP_MUNICIPIO_CSV(
		codigo_ibge INT NOT NULL,
		nome VARCHAR(100) NOT NULL,
		latitude FLOAT(8) NOT NULL,
		longitude FLOAT(8) NOT NULL,
		capital INT NOT NULL,
		codigo_uf INT NOT NULL,
		siafi_id VARCHAR(4) NOT NULL UNIQUE,
		ddd INT NOT NULL,
		fuso_horario VARCHAR(32) NOT NULL,
	)
	SET @SQL = N'
	BULK INSERT #TEMP_MUNICIPIO_CSV
	FROM '''+ @CAMINHO_CSV +'''
	WITH (
		FORMAT= ''CSV''
		,FIRSTROW=2
		,FIELDTERMINATOR = '',''
		,ROWTERMINATOR = ''0x0a''
	)
	'
	EXECUTE sp_executesql @SQL

	TRUNCATE TABLE [TB_BASE_MUNICIPIOS]
	INSERT INTO [TB_BASE_MUNICIPIOS]
	SELECT
		[CODIGO_IBGE] AS NR_COD_MUNICIPIO
		,CAST([LATITUDE] AS DECIMAL(15,5)) AS NR_LATITUDE
		,CAST([LONGITUDE] AS DECIMAL(15,5)) AS NR_LONGITUDE
	FROM #TEMP_MUNICIPIO_CSV

	IF OBJECT_ID(N'tempdb..#TEMP_MUNICIPIO_CSV', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_MUNICIPIO_CSV END
END

IF @CAMINHO_CSV2 IS NOT NULL OR @CAMINHO_CSV2 <> ''
BEGIN
	IF OBJECT_ID(N'tempdb..#TEMP_ESTADOS_CSV', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_ESTADOS_CSV END
	CREATE TABLE #TEMP_ESTADOS_CSV(
		codigo_uf INT NOT NULL,
		uf VARCHAR(2) NOT NULL,
		nome VARCHAR(100) NOT NULL,
		latitude FLOAT(8) NOT NULL,
		longitude FLOAT(8) NOT NULL,
		regiao VARCHAR(15) NOT NULL,
	)
	SET @SQL = N'
	BULK INSERT #TEMP_ESTADOS_CSV
	FROM '''+ @CAMINHO_CSV2 +'''
	WITH (
		FORMAT= ''CSV''
		,FIRSTROW=2
		,FIELDTERMINATOR = '',''
		,ROWTERMINATOR = ''0x0a''
	)
	'
	EXECUTE sp_executesql @SQL

	TRUNCATE TABLE [TB_BASE_ESTADOS]
	INSERT INTO [TB_BASE_ESTADOS]
	SELECT
		[UF] AS NM_UF_ESTADO
		,CAST([LATITUDE] AS DECIMAL(15,5)) AS NR_LATITUDE
		,CAST([LONGITUDE] AS DECIMAL(15,5)) AS NR_LONGITUDE
	FROM #TEMP_ESTADOS_CSV

	IF OBJECT_ID(N'tempdb..#TEMP_ESTADOS_CSV', N'U') IS NOT NULL BEGIN DROP TABLE #TEMP_ESTADOS_CSV END
END
END
GO

CREATE PROCEDURE [PRC_LIMPA_STG_AUXEMERGENCIAL] AS
BEGIN
	TRUNCATE TABLE [TB_STG_AUXEMERGENCIAL_BENEFICIOS]
	TRUNCATE TABLE [TB_STG_AUXEMERGENCIAL_BENEFICIADOS_REGISTRADOS]
	TRUNCATE TABLE [TB_STG_AUXEMERGENCIAL_BENEFICIADOS_ANONIMOS]
	TRUNCATE TABLE [TB_STG_AUXEMERGENCIAL_MUNICIPIOS]
END
GO


--############### CRIAÇÃO DE VIEW PARA CONSUMO POWER BI ###############
CREATE VIEW [VW_TOTAL_BENEFICIADOS_POR_UF] AS
SELECT 
    BENEF.[NM_ANO]
	,BENEF.[NM_MES]
	,DATEFROMPARTS(BENEF.[NM_ANO],BENEF.[NM_MES],'01') AS DT_DATA
	,MUN.[NM_UF]
	,SUM(BENEF.[NR_TOTAL_REGISTRADOS]) AS NR_TOTAL_REGISTRADOS
	,SUM(BENEF.[NR_TOTAL_ANONIMOS]) AS NR_TOTAL_ANONIMOS
	,SUM(BENEF.[NR_TOTAL_REGISTRADOS])+SUM(BENEF.[NR_TOTAL_ANONIMOS]) AS NR_TOTAL_BENEFICIADOS
FROM [TB_AUXEMERGENCIAL_BENEFICIADOS] BENEF WITH(READPAST)
LEFT JOIN [TB_AUXEMERGENCIAL_MUNICIPIOS] MUN WITH(READPAST)
ON MUN.[NR_COD_MUNICIPIO]=BENEF.[NR_COD_MUNICIPIO]
GROUP BY BENEF.[NM_ANO],BENEF.[NM_MES],MUN.[NM_UF]
GO

CREATE VIEW [VW_TOTAL_BENEFICIADOS_POR_MUNICIPIO] AS
SELECT 
    BENEF.[NM_ANO]
	,BENEF.[NM_MES]
	,DATEFROMPARTS(BENEF.[NM_ANO],BENEF.[NM_MES],'01') AS DT_DATA
	,MUN.[NM_MUNICIPIO_UF]
	,SUM(BENEF.[NR_TOTAL_REGISTRADOS]) AS NR_TOTAL_REGISTRADOS
	,SUM(BENEF.[NR_TOTAL_ANONIMOS]) AS NR_TOTAL_ANONIMOS
	,SUM(BENEF.[NR_TOTAL_REGISTRADOS])+SUM(BENEF.[NR_TOTAL_ANONIMOS]) AS NR_TOTAL_BENEFICIADOS
FROM [TB_AUXEMERGENCIAL_BENEFICIADOS] BENEF WITH(READPAST)
LEFT JOIN [TB_AUXEMERGENCIAL_MUNICIPIOS] MUN WITH(READPAST)
ON MUN.[NR_COD_MUNICIPIO]=BENEF.[NR_COD_MUNICIPIO]
GROUP BY BENEF.[NM_ANO],BENEF.[NM_MES],MUN.[NM_MUNICIPIO_UF]
GO

CREATE VIEW [VW_TOTAL_BENEFICIOS_POR_UF] AS
SELECT
	BENEF.[NM_ANO]
	,BENEF.[NM_MES]
	,DATEFROMPARTS(BENEF.[NM_ANO],BENEF.[NM_MES],'01') AS DT_DATA
	,MUN.[NM_UF]
	,SUM(BENEF.[NR_TOTAL_PAGO]) AS NR_TOTAL_PAGO
FROM [TB_AUXEMERGENCIAL_BENEFICIOS] BENEF WITH(READPAST)
LEFT JOIN [TB_AUXEMERGENCIAL_MUNICIPIOS] MUN WITH(READPAST)
ON MUN.[NR_COD_MUNICIPIO]=BENEF.[NR_COD_MUNICIPIO]
GROUP BY BENEF.[NM_ANO],BENEF.[NM_MES],MUN.[NM_UF]
GO

CREATE VIEW [VW_TOTAL_BENEFICIOS_POR_MUNICIPIO] AS
SELECT 
	BENEF.[NM_ANO]
	,BENEF.[NM_MES]
	,DATEFROMPARTS(BENEF.[NM_ANO],BENEF.[NM_MES],'01') AS DT_DATA
	,MUN.[NM_MUNICIPIO_UF]
	,SUM([NR_TOTAL_PAGO]) AS NR_TOTAL_PAGO
FROM [TB_AUXEMERGENCIAL_BENEFICIOS] BENEF WITH(READPAST)
LEFT JOIN [TB_AUXEMERGENCIAL_MUNICIPIOS] MUN WITH(READPAST)
ON MUN.[NR_COD_MUNICIPIO]=BENEF.[NR_COD_MUNICIPIO]
GROUP BY BENEF.[NM_ANO],BENEF.[NM_MES],MUN.[NM_MUNICIPIO_UF]
GO

CREATE VIEW [VW_TOTAL_BENEFICIOS_POR_ENQUADRAMENTO] AS
SELECT 
	BENEF.[NM_ANO]
	,BENEF.[NM_MES]
	,DATEFROMPARTS(BENEF.[NM_ANO],BENEF.[NM_MES],'01') AS DT_DATA
	,BENEF.[NM_ENQUADRAMENTO]
	,SUM([NR_TOTAL_PAGO]) AS NR_TOTAL_PAGO
FROM [TB_AUXEMERGENCIAL_BENEFICIOS] BENEF WITH(READPAST)
LEFT JOIN [TB_AUXEMERGENCIAL_MUNICIPIOS] MUN WITH(READPAST)
ON MUN.[NR_COD_MUNICIPIO]=BENEF.[NR_COD_MUNICIPIO]
GROUP BY BENEF.[NM_ANO],BENEF.[NM_MES],BENEF.[NM_ENQUADRAMENTO]
GO

CREATE VIEW [VW_TOTAL_BENEFICIOS_POR_MUN_ENQUADRAMENTO] AS
SELECT
	BENEF.[NM_ANO]
    ,BENEF.[NM_MES]
	,DATEFROMPARTS(BENEF.[NM_ANO],BENEF.[NM_MES],'01') AS DT_DATA
    ,MUN.[NM_NOME_MUNICIPIO]
	,MUN.[NM_UF]
    ,BENEF.[NM_ENQUADRAMENTO]
    ,BENEF.[NR_TOTAL_PAGO]
    ,BENEF.[NR_TOTAL_PAGO_PERCENT]
	,BENEF_MUN.[NR_TOTAL_PAGO] AS NR_TOTAL_PAGO_ANO
FROM [TB_AUXEMERGENCIAL_BENEFICIOS_POR_ENQUADRAMENTO] BENEF WITH(READPAST)
LEFT JOIN [TB_AUXEMERGENCIAL_MUNICIPIOS] MUN WITH(READPAST)
ON MUN.[NR_COD_MUNICIPIO]=BENEF.[NR_COD_MUNICIPIO]
LEFT JOIN (SELECT [NM_ANO],[NM_MUNICIPIO_UF],SUM([NR_TOTAL_PAGO]) NR_TOTAL_PAGO 
			FROM [VW_TOTAL_BENEFICIOS_POR_MUNICIPIO] WITH(READPAST) 
			GROUP BY [NM_ANO],[NM_MUNICIPIO_UF]) AS BENEF_MUN
ON BENEF_MUN.[NM_ANO] = BENEF.[NM_ANO] AND BENEF_MUN.[NM_MUNICIPIO_UF] = MUN.[NM_MUNICIPIO_UF]
GO

CREATE VIEW [VW_MUNICIPIOS] AS
SELECT 
	MUN.[NR_COD_MUNICIPIO]
	,MUN.[NM_NOME_MUNICIPIO]
	,MUN.[NM_UF]
	,MUN.[NM_MUNICIPIO_UF]
	,BAS.[NR_LATITUDE]
	,BAS.[NR_LONGITUDE]
FROM [TB_AUXEMERGENCIAL_MUNICIPIOS] MUN
LEFT JOIN [TB_BASE_MUNICIPIOS] BAS ON BAS.[NR_COD_MUNICIPIO] = MUN.[NR_COD_MUNICIPIO]
GO

CREATE VIEW [dbo].[VW_BENEFICIOS_MENOR_MAIOR] AS
SELECT DISTINCT
	BASE.[NM_ANO]
	,BENEF_MIN.[NM_UF] AS MENOR_VLR_PAGO_UF
	,BENEF_MIN.[NR_TOTAL_PAGO] AS MENOR_VLR_PAGO_TOTAL
	,BENEF_MAX.[NM_UF] AS MAIOR_VLR_PAGO_UF
	,BENEF_MAX.[NR_TOTAL_PAGO] AS MAIOR_VLR_PAGO_TOTAL
	,BEN_MUN_MIN.[NM_MUNICIPIO_UF] AS MENOR_VLR_PAGO_MUN
	,BEN_MUN_MIN.[NR_TOTAL_PAGO] AS MENOR_VLR_PAGO_MUN_TOTAL
	,BEN_MUN_MAX.[NM_MUNICIPIO_UF] AS MAIOR_VLR_PAGO_MUN
	,BEN_MUN_MAX.[NR_TOTAL_PAGO] AS MAIOR_VLR_PAGO_MUN_TOTAL
FROM [TB_DIM_DATA] BASE
LEFT JOIN [VW_TOTAL_BENEFICIOS_POR_UF] BENEF_MIN
ON BASE.[NM_ANO] = BENEF_MIN.[NM_ANO]
AND BENEF_MIN.[NR_TOTAL_PAGO] = (SELECT MIN([NR_TOTAL_PAGO]) FROM [VW_TOTAL_BENEFICIOS_POR_UF] UF 
								WHERE UF.[NM_ANO] = BASE.[NM_ANO] AND UF.[NM_UF] <> 'OT')
LEFT JOIN [VW_TOTAL_BENEFICIOS_POR_UF] BENEF_MAX
ON BASE.[NM_ANO] = BENEF_MAX.[NM_ANO] 
AND BENEF_MAX.[NR_TOTAL_PAGO] = (SELECT MAX([NR_TOTAL_PAGO]) FROM [VW_TOTAL_BENEFICIOS_POR_UF] UF
								WHERE UF.[NM_ANO] = BASE.[NM_ANO] AND UF.[NM_UF] <> 'OT')
LEFT JOIN [VW_TOTAL_BENEFICIOS_POR_MUNICIPIO] BEN_MUN_MIN
ON BASE.[NM_ANO] = BEN_MUN_MIN.[NM_ANO]
AND BEN_MUN_MIN.[NR_TOTAL_PAGO] = (SELECT MIN([NR_TOTAL_PAGO]) FROM 
									(SELECT UF.[NM_MUNICIPIO_UF], MIN([NR_TOTAL_PAGO]) [NR_TOTAL_PAGO]
									FROM [VW_TOTAL_BENEFICIOS_POR_MUNICIPIO] UF 
									WHERE UF.[NM_ANO] = BASE.[NM_ANO] AND UF.[NM_MUNICIPIO_UF] <> 'OUTROS - OT'
									GROUP BY UF.[NM_MUNICIPIO_UF]) AS MIN_UF)
LEFT JOIN [VW_TOTAL_BENEFICIOS_POR_MUNICIPIO] BEN_MUN_MAX
ON BASE.[NM_ANO] = BEN_MUN_MAX.[NM_ANO] 
AND BEN_MUN_MAX.[NR_TOTAL_PAGO] = (SELECT MAX([NR_TOTAL_PAGO]) FROM 
									(SELECT UF.[NM_MUNICIPIO_UF],MAX([NR_TOTAL_PAGO]) [NR_TOTAL_PAGO]
									FROM [VW_TOTAL_BENEFICIOS_POR_MUNICIPIO] UF 
									WHERE UF.[NM_ANO] = BASE.[NM_ANO] AND UF.[NM_MUNICIPIO_UF] <> 'OUTROS - OT'
									GROUP BY UF.[NM_MUNICIPIO_UF]) AS MAX_UF)
GO
