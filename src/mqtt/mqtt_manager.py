from typing import Callable, Dict, List
import re
import paho.mqtt.client as mqtt

# Type alias
MessageCallback = Callable[[mqtt.Client, mqtt.MQTTMessage], None]

# Global state - simplified to list of tuples for pattern matching
_topic_handlers: List[tuple[str, MessageCallback]] = []
mqtt_client: mqtt.Client | None = None

# MQTT Topic Constants - Base topics for publishing (exact topics, no wildcards)
SYSTEM_SETTINGS_UPDATE_BASE = "settings_update"
SYSTEM_SETTINGS_UPDATE_ACK = "settings_update_ack"
SCHEDULE_UPDATE = "schedule_update"
SCHEDULE_UPDATE_ACK = "schedule_update_ack"
SCHEDULE_TEMP_UPDATE = "schedule_temp_update"
SCHEDULE_TEMP_ACK = "schedule_temp_update_ack"
SCHEDULE_CANCEL = "cancel_schedule"  # Base topic for cancellation
COLLECT_SCHEDULE = "collect_schedule"
SET_MINUTE_TO_WARN = "set_minute_to_warn"
SET_MINUTE_GAP = "set_minute_gap"
SET_SETTINGS = "set_settings"
SET_ROOM_STATUS = "set_room_status"
HEARTBEAT = "heartbeat"
SCHEDULE_SHUTDOWN_WARNING = "room_shutdown_warning"
USAGE_REPORT = "usage_report"  # Base topic for power usage reports

# Subscription topic patterns (with wildcards for subscribing)
SYSTEM_SETTINGS_UPDATE_PATTERN = f"{SYSTEM_SETTINGS_UPDATE_BASE}/#"
SCHEDULE_UPDATE_PATTERN = f"{SCHEDULE_UPDATE}/#"
SCHEDULE_CANCEL_PATTERN = f"{SCHEDULE_CANCEL}/#"  # Pattern for cancel_schedule/*
USAGE_REPORT_PATTERN = f"{USAGE_REPORT}/#"  # Pattern for usage_report/*

# Topic patterns for subscriptions (used in on_connect)
MQTT_SUBSCRIPTION_TOPICS = [
    COLLECT_SCHEDULE,
    SET_MINUTE_TO_WARN,
    SET_MINUTE_GAP,
    SET_SETTINGS,
    SCHEDULE_UPDATE_PATTERN,  # Subscribe to all schedule updates with wildcard
    SCHEDULE_TEMP_UPDATE,
    SCHEDULE_CANCEL_PATTERN,  # Subscribe to all schedule cancellations with wildcard
    SYSTEM_SETTINGS_UPDATE_PATTERN,  # Subscribe to all settings updates with wildcard
    USAGE_REPORT_PATTERN  # Subscribe to all usage reports with wildcard
]

# Configuration
def init_mqtt(mqtt_url: str, mqtt_username: str, mqtt_password: str, mqtt_port: int) -> mqtt.Client:
    global mqtt_client
    print("[MQTT] Initializing MQTT client")
    mqtt_client = mqtt.Client(transport="websockets")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
    
    print(f"[MQTT] Connecting to {mqtt_url}:{mqtt_port}")
    mqtt_client.connect(mqtt_url, mqtt_port, keepalive=60)
    mqtt_client.loop_start()
    return mqtt_client

# Add disconnect handler
def on_disconnect(client, userdata, rc):
    print(f"[MQTT] Disconnected with result code: {rc}")

# Simplified callback registration
def on_topic(topic: str):
    """Decorator to register a function for a specific topic"""
    def decorator(callback: MessageCallback):
        register_callback(topic, callback)
        return callback
    return decorator

def register_callback(topic: str, callback: MessageCallback):
    """Register a callback for a topic pattern"""
    print(f"[MQTT] Registered callback for topic: {topic}")
    _topic_handlers.append((topic, callback))

def _topic_matches(topic_pattern: str, actual_topic: str) -> bool:
    """Check if actual topic matches the pattern (supports # wildcards)"""
    if topic_pattern == actual_topic:
        return True
    
    # Handle # wildcard (matches everything after)
    if topic_pattern.endswith("/#"):
        base_pattern = topic_pattern[:-2]  # Remove /#
        return actual_topic.startswith(base_pattern + "/") or actual_topic == base_pattern
    
    # Handle patterns without # but should match wildcards from subscription
    if "/" in actual_topic:
        base_topic = actual_topic.split("/")[0]
        if topic_pattern == base_topic:
            return True
    
    # Handle + wildcard (matches single level) - if needed later
    if "+" in topic_pattern:
        pattern = topic_pattern.replace("+", "[^/]+")
        return bool(re.match(f"^{pattern}$", actual_topic))
    
    return False

# Core handlers
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[MQTT] Connected with result code: {rc}")
    
    # Auto-subscribe to all registered topics + default topics using constants
    topics_to_subscribe = set(MQTT_SUBSCRIPTION_TOPICS.copy())  # Use the subscription topics constant
    
    for topic_pattern, _ in _topic_handlers:
        # For patterns with #, subscribe to the base pattern
        if topic_pattern.endswith("/#"):
            topics_to_subscribe.add(topic_pattern)
        else:
            topics_to_subscribe.add(topic_pattern)
    
    for topic in topics_to_subscribe:
        result = client.subscribe(topic)
        print(f"[MQTT] Auto-subscribed to: {topic} - Result: {result}")

def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    print(f"[MQTT] Message received on topic: {msg.topic} - Payload: {msg.payload}")
    
    # Find matching handlers
    matched = False
    for topic_pattern, callback in _topic_handlers:
        if _topic_matches(topic_pattern, msg.topic):
            print(f"[MQTT] Executing callback for pattern: {topic_pattern}")
            callback(client, msg)
            matched = True
        else:
            print(f"[MQTT] Pattern '{topic_pattern}' does not match topic '{msg.topic}'")
    
    if not matched:
        print("[MQTT] No handler found for this topic")

# Publisher functions remain the same
def publish(topic: str, msg, log: bool = True):
    if log:
        print(f"[MQTT] Publishing to topic '{topic}': {msg}")
    if mqtt_client:
        mqtt_client.publish(topic=topic, payload=msg, qos=2)

def publish_v2(client: mqtt.Client, topic: str, msg, log: bool = True, retain: bool = False):
    if log:
        print(f"[MQTT] Publishing to topic '{topic}': {msg}")
    client.publish(topic=topic, payload=msg, qos=2, retain=retain)