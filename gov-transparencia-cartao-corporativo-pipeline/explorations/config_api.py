import requests
import os
from dotenv import load_dotenv
import json
import sys

load_dotenv()

token = os.getenv("token_api")
endpoint = os.getenv("gastos_cartao_corporativo")

headers = {
    "accept": "*/*",
    "chave-api-dados": token
}

pagina = 1
todos_dados = []

while True:
    params = {"pagina": pagina,
              "codigoOrgao": "38000",
                "dataTransacaoInicio": "01/01/2020",
            }

    response = requests.get(endpoint, headers=headers, params=params)

    print(f"Lendo página {pagina}... (status {response.status_code})")

    if response.status_code != 200:
        print("Erro na requisição:", response.status_code)

    dados = response.json()

    if not dados:
        print("Nenhum dado retornado, finalizando a leitura.")
        break

    todos_dados.extend(dados)
    pagina += 1

    dados_json = json.dumps(dados)
    tamanho_mb = sys.getsizeof(dados_json) / (1024*1024)

print(f"Tamanho aproximado da página {todos_dados}: {tamanho_mb:.2f} MB")
print(f"Total de registros obtidos: {len(todos_dados)}")
print(json.dumps(todos_dados, indent=4, ensure_ascii=False))