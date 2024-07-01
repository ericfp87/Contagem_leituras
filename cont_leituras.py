from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, count, when, col, trim
import pandas as pd
import os

# Inicializando a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Carregando o arquivo parquet
df = spark.read.parquet(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\historico_UNLE.parquet")

# Selecionando as colunas necessárias
df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "VolumeMedido")

# Removendo espaços em branco de cada coluna
for column in df.columns:
    df = df.withColumn(column, trim(df[column]))

# Convertendo a coluna "DataHoraServico" para data
df = df.withColumn("DataHoraServico", to_date(df["DataHoraServico"]))

# Contagem de valores na coluna "MatriculaClienteImovel" agrupados por "NomeFuncionario" e "DataHoraServico"
df_programadas = df.groupBy("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "DataHoraServico").agg(count("MatriculaClienteImovel").alias("Programadas"))

# Ordenando o DataFrame pela coluna "DataHoraServico"
df_programadas = df_programadas.orderBy("DataHoraServico")

# Convertendo o DataFrame do PySpark para um DataFrame do Pandas
df_programadas_pandas = df_programadas.toPandas()

# Salvando o DataFrame do Pandas como um arquivo .csv com overwrite
df_programadas_pandas.to_csv(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\Dados_historico\UNLE\Programadas_UNLE.csv", sep=';', index=False, mode='w')

# Contagem de valores na coluna "MatriculaClienteImovel" onde "VolumeMedido" não é nulo, agrupados por "NomeFuncionario" e "DataHoraServico"
df_realizadas = df.where(col("VolumeMedido").isNotNull()).groupBy("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "DataHoraServico").agg(count("MatriculaClienteImovel").alias("Realizadas"))

# Ordenando o DataFrame pela coluna "DataHoraServico"
df_realizadas = df_realizadas.orderBy("DataHoraServico")

# Convertendo o DataFrame do PySpark para um DataFrame do Pandas
df_realizadas_pandas = df_realizadas.toPandas()

# Salvando o DataFrame do Pandas como um arquivo .csv com overwrite
df_realizadas_pandas.to_csv(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\Dados_historico\UNLE\Realizadas_UNLE.csv", sep=';', index=False, mode='w')

print("UNLE Concluído")





# Carregando o arquivo parquet
df = spark.read.parquet(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\historico_UNMT.parquet")

# Selecionando as colunas necessárias
df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "VolumeMedido")

# Removendo espaços em branco de cada coluna
for column in df.columns:
    df = df.withColumn(column, trim(df[column]))

# Convertendo a coluna "DataHoraServico" para data
df = df.withColumn("DataHoraServico", to_date(df["DataHoraServico"]))

# Contagem de valores na coluna "MatriculaClienteImovel" agrupados por "NomeFuncionario" e "DataHoraServico"
df_programadas = df.groupBy("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "DataHoraServico").agg(count("MatriculaClienteImovel").alias("Programadas"))

# Ordenando o DataFrame pela coluna "DataHoraServico"
df_programadas = df_programadas.orderBy("DataHoraServico")

# Convertendo o DataFrame do PySpark para um DataFrame do Pandas
df_programadas_pandas = df_programadas.toPandas()

# Salvando o DataFrame do Pandas como um arquivo .csv com overwrite
df_programadas_pandas.to_csv(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\Dados_historico\UNMT\Programadas_UNMT.csv", sep=';', index=False, mode='w')

# Contagem de valores na coluna "MatriculaClienteImovel" onde "VolumeMedido" não é nulo, agrupados por "NomeFuncionario" e "DataHoraServico"
df_realizadas = df.where(col("VolumeMedido").isNotNull()).groupBy("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "DataHoraServico").agg(count("MatriculaClienteImovel").alias("Realizadas"))

# Ordenando o DataFrame pela coluna "DataHoraServico"
df_realizadas = df_realizadas.orderBy("DataHoraServico")

# Convertendo o DataFrame do PySpark para um DataFrame do Pandas
df_realizadas_pandas = df_realizadas.toPandas()

# Salvando o DataFrame do Pandas como um arquivo .csv com overwrite
df_realizadas_pandas.to_csv(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\Dados_historico\UNMT\Realizadas_UNMT.csv", sep=';', index=False, mode='w')

print("UNMT Concluído")
