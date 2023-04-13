import time
import paho.mqtt.client as mqtt
from datetime import date
from datetime import datetime


file = open("data.csv","w")
#file.write("Date-Time-srv;Ref;Label;Date of analyzer;Date of analyzer;Measurement;Temp;CO2;O2;THT;H2S;FLow;H2ST;BCKT")
file.write("Date-srv;Time-srv;Ref;Label;Num compose;compose;Date of analyzer;time of analyzer;Measurement;Temp;NH3")
file.close()


def on_message(client, userdata, message):
    if message.topic == "/hellott":
        message_recu = str(message.payload.decode("utf-8"))
        if ( message_recu == "ping"):
            ping=1
            #print("message received ", message_recu)
        else:
            print("message received ", message_recu)
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y;%H:%M:%S;")
            file = open("data.csv", "a")
            msg = message_recu.split('\x20')
            first = msg[0].split("\x0a")
            #print(msg)
            #print(first)
            file.write("\n" + dt_string  + first[1] + ";")
            for i in range(1,15):
                file.write(msg[i] + ";")
            file.close()
            client.publish("/val_temp_hem", str(round(float(msg[7]), 2)))
            client.publish("/val_O2_hem",  str(round(float(msg[8]), 2)))
            #client.publish("/val_THT_hem",  str(round(float(msg[9]), 2)))
            #client.publish("/val_CO2_hem",  str(round(float(msg[10]), 2)))
            #client.publish("/val_O2_hem",  str(round(float(msg[11]), 2)))
            #client.publish("/val_temp_hem",  str(round(float(msg[12]), 2)))
    elif message.topic == "/send_val_hem":
        print("ready to send document")
        file = open("data.csv", "r")
        data = file.read()
        client.publish("/req_val_hem", data)
        file.close()







#broker="broker.hivemq.com" # broker mosquito free
broker="test.mosquitto.org" # broker mosquito free
client = mqtt.Client()
client.connect(broker,1883,60)
client.loop_start() #start the loop
client.on_message = on_message #attach function to callback

client.subscribe("/hellott")
client.subscribe("/send_val_hem")

time.sleep(200000)
file.close()



