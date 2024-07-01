## Descrição do Projeto

Este projeto utiliza o PySpark para processar dados de arquivos `.parquet` e realizar contagens de leituras programadas e realizadas, agrupadas por diferentes categorias. O resultado é salvo em arquivos CSV para posterior análise.

## Funcionamento do Código

### Importação das Bibliotecas

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, count, col, trim
import pandas as pd
import os
```

O código começa importando as bibliotecas necessárias. `SparkSession` e funções do PySpark são usadas para manipulação de dados em grande escala, enquanto `pandas` é usado para conversão e salvamento dos dados em formato CSV.

### Inicialização da Sessão Spark

```python
spark = SparkSession.builder.getOrCreate()
```

A sessão Spark é inicializada, permitindo o uso das funcionalidades do PySpark.

### Processamento do Arquivo `historico_UNLE.parquet`

#### Carregamento e Seleção de Colunas

```python
df = spark.read.parquet(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\historico_UNLE.parquet")
df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "VolumeMedido")
```

O arquivo `.parquet` é carregado em um DataFrame do PySpark e as colunas necessárias são selecionadas.

#### Remoção de Espaços em Branco

```python
for column in df.columns:
    df = df.withColumn(column, trim(df[column]))
```

Espaços em branco são removidos de cada coluna para garantir a consistência dos dados.

#### Conversão da Coluna `DataHoraServico` para Data

```python
df = df.withColumn("DataHoraServico", to_date(df["DataHoraServico"]))
```

A coluna `DataHoraServico` é convertida para o tipo data.

#### Contagem de Leituras Programadas

```python
df_programadas = df.groupBy("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "DataHoraServico").agg(count("MatriculaClienteImovel").alias("Programadas"))
df_programadas = df_programadas.orderBy("DataHoraServico")
df_programadas_pandas = df_programadas.toPandas()
df_programadas_pandas.to_csv(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\Dados_historico\UNLE\Programadas_UNLE.csv", sep=';', index=False, mode='w')
```

Os dados são agrupados e contados por `MatriculaClienteImovel`, resultando no número de leituras programadas para cada combinação de colunas. O DataFrame resultante é ordenado por `DataHoraServico`, convertido para Pandas e salvo como um arquivo CSV.

#### Contagem de Leituras Realizadas

```python
df_realizadas = df.where(col("VolumeMedido").isNotNull()).groupBy("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "DataHoraServico").agg(count("MatriculaClienteImovel").alias("Realizadas"))
df_realizadas = df_realizadas.orderBy("DataHoraServico")
df_realizadas_pandas = df_realizadas.toPandas()
df_realizadas_pandas.to_csv(r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\Dados_historico\UNLE\Realizadas_UNLE.csv", sep=';', index=False, mode='w')
```

As leituras realizadas (onde `VolumeMedido` não é nulo) são contadas e processadas de maneira similar às leituras programadas, com o resultado salvo em outro arquivo CSV.

### Processamento do Arquivo `historico_UNMT.parquet`

O mesmo processo é repetido para o arquivo `historico_UNMT.parquet`, com os dados sendo carregados, processados, e salvos em arquivos CSV.

### Mensagens de Conclusão

```python
print("UNLE Concluído")
print("UNMT Concluído")
```

Mensagens são exibidas no console indicando a conclusão do processamento de cada arquivo.

## Como Executar

1. Certifique-se de ter os arquivos `.parquet` com os dados necessários.
2. Atualize os caminhos dos arquivos `.parquet` e os destinos dos arquivos CSV no código, se necessário.
3. Execute o script em um ambiente Python com PySpark e Pandas instalados.
4. Verifique os arquivos CSV gerados nos diretórios especificados.

## Requisitos

- Python 3.x
- PySpark
- Pandas

```sh
pip install pyspark pandas
```

