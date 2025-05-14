from typing import Callable, List

import paho.mqtt.client as mqtt

MessageCallback = Callable[[mqtt.Client, mqtt.MQTTMessage], None]


class MQTTCallbackManager:
    def __init__(self):
        print("Callback Manager Initialized")
        self.callbacks = {}

    def register_callback(self, topic: str, msg_cbn: MessageCallback) -> None:
        print(f"Callback for topic {topic} registered")
        self.callbacks[topic] = msg_cbn

    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        print(f"Message from {msg.topic} is received.")
        cb = self.callbacks[msg.topic]
        if cb is not None:
            print("A registered callback is found.")
            cb(client, msg)


class MQTTManager:
    def __init__(self, mqtt_url: str, mqtt_username: str, mqtt_password: str, mqtt_port: int):
        print("MQTT Manager Initialized")
        self.mqtt_url = mqtt_url
        self.mqtt_port = mqtt_port
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)

    def set_on_message_received(self, cb: MessageCallback):
        self.mqtt_client.on_message = cb

    def connect_client(self):
        print("Manager Attempting Connection.")
        self.mqtt_client.connect(self.mqtt_url, self.mqtt_port)
        self.mqtt_client.loop_forever()

    def publish(self, topic, msg, log: bool):
        if log:
            print(f"A publish is performed for topic {topic}")
        self.mqtt_client.publish(topic=topic, payload=msg, qos=2)


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    client.subscribe("collect_schedule")
    client.subscribe("set_minute_to_warn")
    client.subscribe("set_minute_gap")
