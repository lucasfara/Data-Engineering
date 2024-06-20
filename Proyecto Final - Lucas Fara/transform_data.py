import pandas as pd

def transform_data(df_api):
    # Ejemplo de transformación: añadir columna de fecha de extracción
    df_api['extract_date'] = pd.to_datetime('now')
    
    return df_api

# Prueba la función
if __name__ == "__main__":
    df_api = pd.read_csv('api_data.csv')
    df_transformed = transform_data(df_api)
    df_transformed.to_csv('transformed_data.csv', index=False)
    print(df_transformed.head())
