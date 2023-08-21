import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file(filename="json/integracoes-infinit-050833f00045.json", scopes=["https://www.googleapis.com/auth/cloud-platform"])
def envia_data_bigquery(df, id, method):
        df.to_gbq(credentials=credentials,
                destination_table=f"{id}",
                if_exists=f"{method}")

