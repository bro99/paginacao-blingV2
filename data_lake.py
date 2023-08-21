import pandas as pd
import requests
from datetime import datetime
import json
from envia_big_query import envia_data_bigquery

id = "integracoes-infinit.DataLake.Pedidos"
method = "append"

def envia_df_data_lake(data_dict):
    print("Recebi o data")  # Converter a string JSON para um dicionário
    if "retorno" in data_dict and "pedidos" in data_dict["retorno"]:
        pedidos = data_dict['retorno']['pedidos']
        pedidos_list = [pedido['pedido'] for pedido in pedidos]
        df_lake = pd.json_normalize(pedidos_list)

        #Tratamento
        df_lake['data'] = pd.to_datetime(df_lake['data'])
        df_lake.rename(columns={col: col.replace('.', '_') for col in df_lake.columns}, inplace=True)
        # Verifica se as colunas existem e converte para tipo str se necessário
        if 'itens' in df_lake.columns:
            df_lake['itens'] = df_lake['itens'].astype(str)

        if 'transporte_volumes' in df_lake.columns:
            df_lake['transporte_volumes'] = df_lake['transporte_volumes'].astype(str)

        if 'parcelas' in df_lake.columns:
            df_lake['parcelas'] = df_lake['parcelas'].astype(str)
        #Tratamento

    envia_data_bigquery(df_lake, id, method)
 
        