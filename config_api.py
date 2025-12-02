import requests
import os
from dotenv import load_dotenv
from fastapi import FastAPI
import json

app = FastAPI()
load_dotenv()

token = os.getenv("token_api")
endpoint = os.getenv("gastos_cartao_corporativo")

headers = {
    "accept": "*/*",
    "chave-api-dados": token
}

# @app.get("/")
# def home():
#     return {"message": "API Local - Acesse /docs"}

# @app.get("/gastos")
# def gastos_api():
    
pagina = 1
todos_dados = []

while True:
    params = {"pagina": pagina}

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

print(f"Total de registros obtidos: {len(todos_dados)}")
print(json.dumps(todos_dados, indent=4, ensure_ascii=False))

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("__main__:app", host="127.0.0.1", port=8000, reload=True)