import http.client
import json
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import pandas as pd

# Configuración de variables de entorno
redshift_password = os.getenv('PWD_REDSHIFT')
api_key = os.getenv('API_KEY')

# Conexión a la API de la NBA
conn_api = http.client.HTTPSConnection("v2.nba.api-sports.io")
headers = {
    'x-rapidapi-host': "v2.nba.api-sports.io",
    'x-rapidapi-key': api_key
}
conn_api.request("GET", "/teams/statistics?season=2020&id=3", headers=headers)
res_api = conn_api.getresponse()
data_api = res_api.read()

# Decodificar los datos obtenidos de la API
decoded_data_api = json.loads(data_api.decode("utf-8"))

# Transformación de datos con Pandas
data = {
    "games": [decoded_data_api["response"][0]["games"]],
    "fastBreakPoints": [decoded_data_api["response"][0]["fastBreakPoints"]],
    "pointsInPaint": [decoded_data_api["response"][0]["pointsInPaint"]],
    "biggestLead": [decoded_data_api["response"][0]["biggestLead"]],
    "secondChancePoints": [decoded_data_api["response"][0]["secondChancePoints"]],
    "pointsOffTurnovers": [decoded_data_api["response"][0]["pointsOffTurnovers"]],
    "longestRun": [decoded_data_api["response"][0]["longestRun"]],
    "points": [decoded_data_api["response"][0]["points"]],
    "fgm": [decoded_data_api["response"][0]["fgm"]],
    "fga": [decoded_data_api["response"][0]["fga"]],
    "fgp": [decoded_data_api["response"][0]["fgp"]],
    "ftm": [decoded_data_api["response"][0]["ftm"]],
    "fta": [decoded_data_api["response"][0]["fta"]],
    "ftp": [decoded_data_api["response"][0]["ftp"]],
    "tpm": [decoded_data_api["response"][0]["tpm"]],
    "tpa": [decoded_data_api["response"][0]["tpa"]],
    "tpp": [decoded_data_api["response"][0]["tpp"]],
    "offReb": [decoded_data_api["response"][0]["offReb"]],
    "defReb": [decoded_data_api["response"][0]["defReb"]],
    "totReb": [decoded_data_api["response"][0]["totReb"]],
    "assists": [decoded_data_api["response"][0]["assists"]],
    "pFouls": [decoded_data_api["response"][0]["pFouls"]],
    "steals": [decoded_data_api["response"][0]["steals"]],
    "turnovers": [decoded_data_api["response"][0]["turnovers"]],
    "blocks": [decoded_data_api["response"][0]["blocks"]],
    "plusMinus": [decoded_data_api["response"][0]["plusMinus"]]
}

df = pd.DataFrame(data)

# Datos de conexión a Redshift
host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
dbname = "data-engineer-database"
user = "lucasgfara_coderhouse"
password = redshift_password
port = "5439"

# Conexión a Redshift y creación de tabla
conn_redshift = psycopg2.connect(
    host=host,
    dbname=dbname,
    user=user,
    password=password,
    port=port
)
cur = conn_redshift.cursor()

# Creación de la tabla en Redshift
cur.execute("""
    CREATE TABLE IF NOT EXISTS nba_team_stats (
        games INT,
        fastBreakPoints INT,
        pointsInPaint INT,
        biggestLead INT,
        secondChancePoints INT,
        pointsOffTurnovers INT,
        longestRun INT,
        points INT,
        fgm INT,
        fga INT,
        fgp FLOAT,
        ftm INT,
        fta INT,
        ftp FLOAT,
        tpm INT,
        tpa INT,
        tpp FLOAT,
        offReb INT,
        defReb INT,
        totReb INT,
        assists INT,
        pFouls INT,
        steals INT,
        turnovers INT,
        blocks INT,
        plusMinus INT
    )
""")

# Insertar datos obtenidos de la API en la tabla
cur.execute("""
    INSERT INTO nba_team_stats VALUES (
        %(games)s, %(fastBreakPoints)s, %(pointsInPaint)s, %(biggestLead)s, %(secondChancePoints)s, 
        %(pointsOffTurnovers)s, %(longestRun)s, %(points)s, %(fgm)s, %(fga)s, %(fgp)s, %(ftm)s, 
        %(fta)s, %(ftp)s, %(tpm)s, %(tpa)s, %(tpp)s, %(offReb)s, %(defReb)s, %(totReb)s, 
        %(assists)s, %(pFouls)s, %(steals)s, %(turnovers)s, %(blocks)s, %(plusMinus)s
    )
""", df.to_dict('records')[0])

# Confirmar los cambios y cerrar la conexión
conn_redshift.commit()
cur.close()
conn_redshift.close()

print("Tabla creada y datos insertados en Redshift con éxito.")

# Envío de alerta por correo electrónico si un valor sobrepasa el límite configurado
threshold_points = 100
if df['points'][0] > threshold_points:
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    sender_email = "cuenta_remitente@gmail.com"
    receiver_email = "cuenta_destinatario@gmail.com"
    password = "password"

    subject = "Alerta: Valor de puntos excedido"
    body = f"El valor de puntos ha excedido el límite configurado. Valor actual: {df['points'][0]}"

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(sender_email, password)
    text = msg.as_string()
    server.sendmail(sender_email, receiver_email, text)
    server.quit()

    print("Alerta enviada por correo electrónico.")
