import requests
import json
import pandas as pd
from data_lake import envia_df_data_lake
from data_ware_house import trata_data_ware_house

# Define a chave de API
api_key = "186ca8f6bb29130c9c81da6ccc0b6510a417c8e609fff872480e9a308e0a2a8a2da442b4"

# Define o valor inicial de valorpagina
valorpagina = 1847
# Define uma lista vazia para armazenar os pedidos
df_lista_auxiliar = []

# Loop para paginar a consulta
while True:
    # Define a URL para a página atual
    url = f"https://bling.com.br/Api/v2/pedidos/page={valorpagina}/json/"

    # Define os parâmetros da consulta
    params = {
        "apikey": api_key
    }
    # Envia a solicitação GET usando requests
    response = requests.get(url, params=params)

    if response.status_code == 200 and response.content != b'{"retorno":{"erros":[{"erro":{"cod":14,"msg":"A informacao desejada nao foi encontrada"}}]}}':
        data = response.json()
        envia_df_data_lake(data)
        trata_data_ware_house(data, valorpagina)
        print("feito", valorpagina)
    else:
        print("acabou")


    valorpagina += 1