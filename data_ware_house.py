import pandas as pd
from google.oauth2 import service_account
from pandas_gbq import gbq
import json
from envia_big_query import envia_data_bigquery

id = "integracoes-infinit.DataWareHouse.Pedidos_Webhook"
method = "append"

def trata_data_ware_house(data, valorpagina):
    df_aux_list = []
    num_pedidos = len(data['retorno']['pedidos'])
    for i in range(num_pedidos):
        id_pedido = data['retorno']['pedidos'][i]['pedido']['numero']
        df_aux = pd.DataFrame({
            'id_pedido': [id_pedido],
            'json_content': [json.dumps(data['retorno']['pedidos'][i]['pedido'])],
            'pagina' : [valorpagina]
        })
        df_aux_list.append(df_aux)

    df_final = pd.concat(df_aux_list)
    envia_data_bigquery(df_final, id, method)
