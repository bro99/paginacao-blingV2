import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


def carregar_dados_bigquery():
    query = '''
    SELECT 
    MAX(id_pedido) AS max_id
        FROM `integracoes-infinit.DataWareHouse.Pedidos`
    WHERE pagina = (SELECT MAX(pagina) FROM `integracoes-infinit.DataWareHouse.Pedidos`);

    '''

    credentials = service_account.Credentials.from_service_account_file(filename='json/integracoes-infinit-050833f00045.json', scopes=["https://www.googleapis.com/auth/cloud-platform"])

    df = pd.read_gbq(credentials=credentials, query=query)
    max_id = df.loc[0]['max_id']
    id_new_request = int(max_id)

    return id_new_request