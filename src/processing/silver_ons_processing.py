import os
from pyspark.sql import SparkSession

def process_bronze_to_silver():
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    print("Iniciando a Spark Session...")
    spark = SparkSession.builder \
        .appName("Lakehouse-Energy-Climate") \
        .master("local[*]") \
        .getOrCreate()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
    
    bronze_path = os.path.join(project_root, "data", "bronze", "CARGA_ENERGIA_DIARIA_2025.csv")
    silver_path = os.path.join(project_root, "data", "silver", "carga_energia")
    
    print(f"Lendo dados brutos de: {bronze_path}")
    
    try:
        df_bronze = spark.read.csv(
            bronze_path,
            header=True,
            sep=";",
            inferSchema=True
        )
        
        print(f"Total de registros lidos: {df_bronze.count()}")
        
        # Transformação Silver: Padronização do nome das colunas
        df_silver = df_bronze
        for c in df_silver.columns:
            novo_nome = c.strip().lower()
            df_silver = df_silver.withColumnRenamed(c, novo_nome)
            
        print("Schema da camada Silver:")
        df_silver.printSchema()
        
        print("Salvando os dados particionados/compactados na camada Silver...")
        df_silver.write.mode("overwrite").parquet(silver_path)
        
        print(f"Sucesso! Dados salvos na camada Silver em: {silver_path}")
        
    except Exception as e:
        print(f"Erro no processamento do PySpark: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_bronze_to_silver()