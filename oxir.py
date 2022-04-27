import psycopg2
import paho.mqtt.client as mqtt
while True:
    try:
        conn =  psycopg2.connect(host='localhost',database='fastapi',user='postgres',password = 1234, port =5432) 
        cursor = conn.cursor()
        print("Database connection was successful")
        break
    except Exception as eror:
        print("Error connecting")
        print("Error: ",eror)
client = mqtt.Client()
client.connect('185.196.214.190', 1883)
client.username_pw_set(username="emqx", password="12345")


def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("bolak")
    # W/1/TOSHKENT/867857033397873/data


def on_message(client, userdata, message):
        a = message.payload.decode()
        bolak = a.split(',')
        sana = bolak[0]
        data1 = bolak[3]
        data = data1[4:7]
        sana = sana.split(' ')
        sana1 = sana[0]
        sana2 = sana[1]
        sana1 = sana1[13:]
        sana2 = sana2[:5]
        vaqt = sana1 + " " + sana2
        cursor.execute("""INSERT INTO data(data , time) VALUES (%s, %s )  """, (data , vaqt))
        conn.commit()


while True:
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_connect1 = on_connect
    client.on_message1 = on_message
    client.loop_forever()
