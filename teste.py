import requests
import pandas as pd

url = "https://api.portaldatransparencia.gov.br/api-de-dados/cartoes"
params = {
    "codigoOrgao": "38000",
    "dataTransacaoInicio": "01/09/2025",
    "dataTransacaoFim": "30/09/2025",
    "pagina": 1
}

headers = {
    "chave-api-dados": "e941af3608f165f7b41587c237ec0924",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Origin": "https://api.portaldatransparencia.gov.br",
    "Referer": "https://api.portaldatransparencia.gov.br/swagger-ui.html"
}

r = requests.get(url, headers=headers, params=params)

print(r.status_code)
print(r.text[:300])


print(pd.json_normalize(r.json()).head())