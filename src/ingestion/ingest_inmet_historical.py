import os
import requests
import zipfile
import io

def ingest_inmet_historical():
    url = "https://portal.inmet.gov.br/uploads/dadoshistoricos/2025.zip"
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../"))
    
    # Criando o caminho para a nova pasta inmet dentro da bronze
    bronze_inmet_path = os.path.join(project_root, "data", "bronze", "inmet")
    
    # O comando abaixo cria a pasta automaticamente se ela não existir
    os.makedirs(bronze_inmet_path, exist_ok=True)

    print("Iniciando o download do arquivo ZIP do INMET (Isso pode levar alguns segundos)...")
    response = requests.get(url)
    
    if response.status_code == 200:
        print("Download concluído! Lendo o arquivo ZIP na memória...")
        # Transforma o conteúdo baixado em um arquivo virtual
        z = zipfile.ZipFile(io.BytesIO(response.content))
        
        # O namelist() retorna uma lista com os nomes de todos os arquivos dentro do ZIP
        arquivos_no_zip = z.namelist()

        for nome_arquivo in arquivos_no_zip:
            if "A701" in nome_arquivo:
                print(f"Estação A701 encontrada: {nome_arquivo}")
                
                # Extrai apenas esse arquivo específico para a pasta Bronze
                z.extract(nome_arquivo, bronze_inmet_path)
                print("Arquivo extraído com sucesso para a Camada Bronze!")
                
                # Como já achamos o que queríamos, paramos o loop para economizar processamento
                break

if __name__ == "__main__":
    ingest_inmet_historical()