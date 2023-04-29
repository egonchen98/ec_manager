# encoding: utf-8
"""strategy
MQTT获取的信息先写入本地文档，并将地区标注出来；
再用一个新的函数读取文件，写入数据库（为保证串口多个数据都写入了，需要先检查文件修改时间大于3分钟。
"""
import random
from paho.mqtt import client as mqtt_client
from pathlib import Path
import datetime

broker = '124.220.27.50'
port = 1883
topic = 'cy/thesis/#'
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'daryl'
password = 'chen123456'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        area = msg.topic.split('/')[-1]
        if area in ['lanzhou', 'wuwei', 'test']:
            file = Path(f'./message_log_{area}.csv')
            if not file.exists():
                file.write_text('')
            content = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}_{area},{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")},{msg.topic},{msg.payload.hex()}\n'
            file.write_text(file.read_text() + content)
        print(datetime.datetime.now(), msg.topic, msg.payload.hex())

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
    exit()
    ms = MessageSolver()
    ms.run()
