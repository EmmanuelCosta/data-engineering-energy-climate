import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_inmet_bronze_to_silver():
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    print("Iniciando a Spark Session...")
    spark = SparkSession.builder \
        .appName("Lakehouse-Energy-Climate") \
        .master("local[*]") \
        .getOrCreate()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
        
    bronze_path = os.path.join(project_root, "data", "bronze", "inmet", "2025")
    silver_path = os.path.join(project_root, "data", "silver", "clima_inmet")

    print("Lendo os dados da camada Bronze...")
    
    print("Removendo a sujeira do cabeçalho com RDD...")
    
    # 1. Lemos o arquivo como texto bruto direto pelo Spark Context
    rdd_raw = spark.sparkContext.textFile(bronze_path)
    
    # 2. zipWithIndex() numera as linhas. Filtramos para manter apenas da linha 8 em diante.
    # O map(lambda x: x[0]) extrai apenas o texto da linha, descartando o número do índice.
    rdd_limpo = rdd_raw.zipWithIndex() \
                       .filter(lambda x: x[1] >= 8) \
                       .map(lambda x: x[0])
                       
    # 3. Agora dizemos pro Spark ler esse RDD limpo como CSV, informando que agora temos um header
    # Lendo com o encoding correto para entender acentos do português
    df_silver = spark.read.csv(rdd_limpo, sep=";", header=True, inferSchema=True)
    
    print("Como o PySpark enxerga o arquivo AGORA:")
    # truncate=False ajuda a não cortar os nomes das colunas
    df_silver.show(5, truncate=False)

    print("Selecionando e renomeando as colunas essenciais por índice...")
    
    # Captura a lista de todos os nomes de colunas que o Spark gerou
    colunas = df_silver.columns
    
    df_silver_clean = df_silver.select(
        col(colunas[0]).alias("data_medicao"),
        col(colunas[1]).alias("hora_utc"),
        col(colunas[2]).alias("precipitacao_mm"),
        col(colunas[7]).alias("temp_ar_c")
    )
    
    print("Tabela Silver Limpa:")
    df_silver_clean.show(5)
    
    print("Salvando dados na Camada Silver no formato Parquet...")
    # Salvamos usando modo overwrite para substituir execuções anteriores
    df_silver_clean.write.format("parquet").mode("overwrite").save(silver_path)
    print("Dados climáticos salvos com sucesso na Camada Silver!")

    return


if __name__ == "__main__":
    process_inmet_bronze_to_silver()