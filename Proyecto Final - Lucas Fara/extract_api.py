import http.client
import json
import pandas as pd

def extract_api_data():
    conn_api = http.client.HTTPSConnection("v2.nba.api-sports.io")
    headers = {
        'x-rapidapi-host': "v2.nba.api-sports.io",
        'x-rapidapi-key': "3f41540dfbba32863f7981122084859b"
    }
    conn_api.request("GET", "/teams/statistics?season=2020&id=3", headers=headers)
    res_api = conn_api.getresponse()
    data_api = res_api.read()

    # Decodificar los datos obtenidos de la API
    decoded_data_api = json.loads(data_api.decode("utf-8"))
    df_api = pd.json_normalize(decoded_data_api["response"])

    return df_api

# Prueba la función
if __name__ == "__main__":
    df_api = extract_api_data()
    df_api.to_csv('api_data.csv', index=False)
    print(df_api.head())
