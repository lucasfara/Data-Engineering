import smtplib
from email.mime.text import MIMEText

def send_alert(message):
    sender = 'cuenta_remitente@gmail.com'
    receiver = 'cuenta_destinatario@gmail.com'
    subject = 'Data Alert'
    
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver
    
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(sender, 'password')
        server.sendmail(sender, receiver, msg.as_string())

# Prueba la función
if __name__ == "__main__":
    send_alert("This is a test alert.")
