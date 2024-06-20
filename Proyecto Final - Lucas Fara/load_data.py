from sqlalchemy import create_engine
import pandas as pd

def load_data(df_transformed):
    # Datos de conexión
    dbname = "data-engineer-database"
    user = "lucasgfara_coderhouse"
    password = "Bs1H2V5S4C"
    host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    port = "5439"
    
    # Crear la conexión
    conn_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(conn_string)
    
    # Subir los datos
    df_transformed.to_sql('nba_team_stats', engine, if_exists='append', index=False)

# Prueba la función
if __name__ == "__main__":
    df_transformed = pd.read_csv('transformed_data.csv')
    load_data(df_transformed)
