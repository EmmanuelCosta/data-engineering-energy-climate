import urllib.request
import os

def ingest_ons_daily_load():
    # URL oficial de dados abertos do ONS (Carga de Energia Diária de 2025)
    url = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/CARGA_ENERGIA_2025.csv"
    
    # Caminho dinâmico para garantir que salve na pasta data/bronze independentemente de onde o script for chamado
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
    destination_path = os.path.join(project_root, "data", "bronze", "CARGA_ENERGIA_DIARIA_2025.csv")
    
    print(f"Iniciando extração (Camada Bronze)...\nFonte: {url}")
    
    try:
        urllib.request.urlretrieve(url, destination_path)
        print(f"Sucesso! Arquivo bruto salvo em: {destination_path}")
    except Exception as e:
        print(f"Erro durante a ingestão: {e}")

if __name__ == "__main__":
    ingest_ons_daily_load()