import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, max, round
from delta import configure_spark_with_delta_pip

def process_silver_to_gold():
    # Mantendo o fix do Windows para o Hadoop não quebrar
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")


    print("Configurando Spark com suporte a Delta Lake...")

    # Desenhamos o builder com as configurações essenciais do Delta
    builder = SparkSession.builder \
        .appName("Lakehouse-Energy-Climate-Gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Injetamos os pacotes do Delta e iniciamos a sessão
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    print("Spark Session Gold iniciada com sucesso!")

    # Definindo os caminhos dinâmicos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
    
    silver_path = os.path.join(project_root, "data", "silver", "carga_energia")
    gold_path = os.path.join(project_root, "data", "gold", "carga_energia_mensal")
    
    print("Lendo os dados limpos da Camada Silver...")
    #  Lendo os dados otimizados no formato Parquet
    df_silver = spark.read.parquet(silver_path)
    
    # Exibindo as 5 primeiras linhas para inspecionarmos as colunas disponíveis
    print("Amostra dos dados da Silver:")
    df_silver.show(5)


    # Criando as colunas derivadas de data
    df_gold = df_silver.withColumn("ano", year(col("din_instante"))) \
                       .withColumn("mes", month(col("din_instante")))
                       
    # Agrupando, calculando métricas e ordenando
    df_gold = df_gold.groupBy("nom_subsistema", "ano", "mes") \
        .agg(
            round(avg(col("val_cargaenergiamwmed")), 2).alias("media_carga_mwmed"),
            round(max(col("val_cargaenergiamwmed")), 2).alias("max_carga_mwmed")
        ) \
        .orderBy("ano", "mes", "nom_subsistema")

        
    print("Tabela consolidada para a área de negócios (Camada Gold):")
    df_gold.show(10)

    df_gold.write.format("delta").mode("overwrite").save(gold_path)

    print("Dados salvos na camada Gold!")
    # Retornamos o spark para usarmos nas próximas etapas
    return spark

if __name__ == "__main__":
    process_silver_to_gold()