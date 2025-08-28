from typing import Callable, Dict

import paho.mqtt.client as mqtt

# Type alias
MessageCallback = Callable[[mqtt.Client, mqtt.MQTTMessage], None]

# Global state
_callbacks: Dict[str, MessageCallback] = {}
mqtt_client: mqtt.Client | None = None


# Configuration
def init_mqtt(mqtt_url: str, mqtt_username: str, mqtt_password: str, mqtt_port: int) -> mqtt.Client:
    global mqtt_client
    print("[MQTT] Initializing MQTT client")
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
    mqtt_client.connect(mqtt_url, mqtt_port)
    mqtt_client.loop_start()
    return mqtt_client


# Callback registration
def register_callback(topic: str, callback: MessageCallback):
    print(f"[MQTT] Callback registered for topic: {topic}")
    _callbacks[topic] = callback


# Core handlers
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"[MQTT] Connected with result code: {reason_code}")
    # Subscribe to topics of interest
    topics = [
        "collect_schedule",
        "set_minute_to_warn",
        "set_minute_gap",
        "set_settings"
    ]
    for topic in topics:
        client.subscribe(topic)
        print(f"[MQTT] Subscribed to topic: {topic}")


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    print(f"[MQTT] Message received on topic: {msg.topic}")
    callback = _callbacks.get(msg.topic)
    if callback:
        print("[MQTT] Invoking registered callback.")
        callback(client, msg)
    else:
        print("[MQTT] No callback registered for this topic.")


# Publisher
def publish(topic: str, msg, log: bool = True):
    if log:
        print(f"[MQTT] Publishing to topic '{topic}': {msg}")
    mqtt_client.publish(topic=topic, payload=msg, qos=2)

def publish_v2(client: mqtt.Client, topic: str, msg, log: bool = True):
    if log:
        print(f"[MQTT] Publishing to topic '{topic}': {msg}")
    client.publish(topic=topic, payload=msg, qos=2)